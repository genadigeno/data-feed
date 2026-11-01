package session

import (
	"errors"
	"slices"
	"testing"
)

var allEvents = []string{EvStart, EvPause, EvResume, EvStop, EvAdmit, EvPlanned, EvDrained, EvDone, EvFail, EvRequeue}

// Every state × every event, written out: any change to the machine must change this.
func TestStateMachineExhaustively(t *testing.T) {
	const (
		bad  = "-"
		same = "="
	)
	want := map[string]map[string]string{
		Created:   {EvStart: Queued},
		Queued:    {EvStart: same, EvAdmit: Starting, EvStop: Stopped},
		Starting:  {EvStart: same, EvPlanned: Running, EvFail: Failed, EvRequeue: Queued},
		Running:   {EvStart: same, EvPause: Pausing, EvResume: same, EvStop: Stopping, EvDone: Completed, EvFail: Failed},
		Pausing:   {EvPause: same, EvDrained: Paused, EvStop: Stopping, EvFail: Failed},
		Paused:    {EvPause: same, EvResume: Running, EvStop: Stopping, EvFail: Failed},
		Stopping:  {EvStop: same, EvDrained: Stopped, EvFail: Failed},
		Stopped:   {EvStop: same},
		Completed: {},
		Failed:    {},
	}
	for _, state := range AllStates {
		for _, event := range allEvents {
			expected, ok := want[state][event]
			if !ok {
				expected = bad
			}
			got, err := Next(state, event)
			switch expected {
			case bad:
				var invalid *InvalidTransition
				if !errors.As(err, &invalid) {
					t.Errorf("%s + %s: got %+v, %v; want an invalid transition", state, event, got, err)
				}
			case same:
				if err != nil || !got.NoOp || got.To != state {
					t.Errorf("%s + %s: got %+v, %v; want a no-op", state, event, got, err)
				}
			default:
				if err != nil || got.NoOp || got.To != expected {
					t.Errorf("%s + %s: got %+v, %v; want %s", state, event, got, err, expected)
				}
			}
		}
	}
}

func TestTerminalStatesCannotRestart(t *testing.T) {
	for _, state := range []string{Completed, Failed, Stopped} {
		if !IsTerminal(state) {
			t.Errorf("%s is not terminal", state)
		}
		for _, event := range []string{EvStart, EvResume, EvPause, EvFail, EvDone} {
			if _, err := Next(state, event); err == nil {
				t.Errorf("%s accepted %s", state, event)
			}
		}
	}
	for _, state := range []string{Created, Queued, Starting, Running, Pausing, Paused, Stopping} {
		if IsTerminal(state) {
			t.Errorf("%s is terminal", state)
		}
	}
}

// Buttons must match reality: an offered action is accepted, and a lifecycle event that
// would change the state is offered.
func TestAvailableActionsMatchTheStateMachine(t *testing.T) {
	events := map[string]string{ActStart: EvStart, ActPause: EvPause, ActResume: EvResume, ActStop: EvStop}
	for _, state := range AllStates {
		actions := AvailableActions(state)
		for action, event := range events {
			got, err := Next(state, event)
			offered := slices.Contains(actions, action)
			if offered && (err != nil || got.NoOp) {
				t.Errorf("%s offers %s, but it does nothing or is refused", state, action)
			}
			if !offered && err == nil && !got.NoOp {
				t.Errorf("%s accepts %s, but does not offer it", state, action)
			}
		}
		if slices.Contains(actions, ActRetry) != IsTerminal(state) {
			t.Errorf("%s: retry offered wrongly", state)
		}
		if slices.Contains(actions, ActConfigure) != (state == Created) {
			t.Errorf("%s: configure offered wrongly", state)
		}
		if slices.Contains(actions, ActDelete) != (IsTerminal(state) || state == Created) {
			t.Errorf("%s: delete offered wrongly", state)
		}
	}
}

func TestIsActive(t *testing.T) {
	active := []string{Starting, Running, Pausing, Paused, Stopping}
	for _, state := range AllStates {
		if IsActive(state) != slices.Contains(active, state) {
			t.Errorf("%s: IsActive = %v", state, IsActive(state))
		}
	}
}

func TestInvalidTransitionMessage(t *testing.T) {
	_, err := Next(Completed, EvStart)
	if err == nil || err.Error() != "cannot start a session that is COMPLETED" {
		t.Fatalf("%v", err)
	}
}
