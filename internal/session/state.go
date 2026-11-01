// Package session is the session domain: the lifecycle state machine, the specification
// document, snapshots, and the use cases every interface shares.
// See docs/01-architecture/session-lifecycle.md.
package session

import "fmt"

// States.
const (
	Created   = "CREATED"
	Queued    = "QUEUED"
	Starting  = "STARTING"
	Running   = "RUNNING"
	Pausing   = "PAUSING"
	Paused    = "PAUSED"
	Stopping  = "STOPPING"
	Stopped   = "STOPPED"
	Completed = "COMPLETED"
	Failed    = "FAILED"
)

// AllStates lists every state.
var AllStates = []string{Created, Queued, Starting, Running, Pausing, Paused, Stopping, Stopped, Completed, Failed}

// Events. User events can be requested; the rest come from the Coordinator.
const (
	EvStart   = "start"
	EvPause   = "pause"
	EvResume  = "resume"
	EvStop    = "stop"
	EvAdmit   = "admit"
	EvPlanned = "planned"
	EvDrained = "drained"
	EvDone    = "done"
	EvFail    = "fail"
	EvRequeue = "requeue" // restart recovery: a session caught while starting
)

// Outcome is what an event does in a state.
type Outcome struct {
	To string
	// NoOp means the state already reflects the request: repeating a command is fine.
	NoOp bool
}

// Next is the state machine: a pure function, tested for every combination.
func Next(state, event string) (Outcome, error) {
	type key struct{ state, event string }
	if to, ok := transitions[key{state, event}]; ok {
		return Outcome{To: to}, nil
	}
	if noOps[key{state, event}] {
		return Outcome{To: state, NoOp: true}, nil
	}
	return Outcome{}, &InvalidTransition{State: state, Event: event}
}

// InvalidTransition reports a command the state does not allow.
type InvalidTransition struct{ State, Event string }

func (e *InvalidTransition) Error() string {
	return fmt.Sprintf("cannot %s a session that is %s", e.Event, e.State)
}

var transitions = map[struct{ state, event string }]string{
	{Created, EvStart}: Queued,

	{Queued, EvAdmit}: Starting,
	{Queued, EvStop}:  Stopped,

	{Starting, EvPlanned}: Running,
	{Starting, EvFail}:    Failed,
	{Starting, EvRequeue}: Queued,

	{Running, EvPause}: Pausing,
	{Running, EvStop}:  Stopping,
	{Running, EvDone}:  Completed,
	{Running, EvFail}:  Failed,

	{Pausing, EvDrained}: Paused,
	{Pausing, EvStop}:    Stopping,
	{Pausing, EvFail}:    Failed,

	{Paused, EvResume}: Running,
	{Paused, EvStop}:   Stopping,
	{Paused, EvFail}:   Failed,

	{Stopping, EvDrained}: Stopped,
	{Stopping, EvFail}:    Failed,
}

var noOps = map[struct{ state, event string }]bool{
	{Queued, EvStart}:   true,
	{Starting, EvStart}: true,
	{Running, EvStart}:  true,

	{Pausing, EvPause}: true,
	{Paused, EvPause}:  true,

	{Running, EvResume}: true,

	{Stopping, EvStop}: true,
	{Stopped, EvStop}:  true,
}

// IsTerminal reports whether nothing further can happen to a session.
func IsTerminal(state string) bool {
	return state == Completed || state == Stopped || state == Failed
}

// IsActive reports whether a session counts against max_concurrent_sessions.
func IsActive(state string) bool {
	switch state {
	case Starting, Running, Pausing, Paused, Stopping:
		return true
	}
	return false
}

// Actions a user can request, as reported to interfaces.
const (
	ActConfigure = "configure"
	ActStart     = "start"
	ActPause     = "pause"
	ActResume    = "resume"
	ActStop      = "stop"
	ActDelete    = "delete"
	ActClone     = "clone"
	ActRetry     = "retry"
	ActDownload  = "download"
)

// AvailableActions lists what makes sense in a state, so that interfaces never duplicate
// the rules. It reflects Next: a test checks they agree.
func AvailableActions(state string) []string {
	switch state {
	case Created:
		return []string{ActConfigure, ActStart, ActDelete, ActClone}
	case Queued:
		return []string{ActStop, ActClone}
	case Starting:
		return []string{ActClone}
	case Running:
		return []string{ActPause, ActStop, ActClone, ActDownload}
	case Pausing:
		return []string{ActStop, ActClone, ActDownload}
	case Paused:
		return []string{ActResume, ActStop, ActClone, ActDownload}
	case Stopping:
		return []string{ActClone, ActDownload}
	case Stopped, Completed, Failed:
		return []string{ActDelete, ActClone, ActRetry, ActDownload}
	}
	return nil
}
