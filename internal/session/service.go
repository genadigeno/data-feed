package session

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/coord/lease"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/ids"
	"github.com/genadigeno/data-feed/internal/snapshot"
	"github.com/genadigeno/data-feed/internal/store"
)

// Error codes.
const (
	CodeNotFound          = "ERR_NOT_FOUND"
	CodeInvalidTransition = "ERR_INVALID_TRANSITION"
	CodeVersionMismatch   = "ERR_VERSION_MISMATCH"
)

// Notifier learns about changes after they are committed.
type Notifier interface {
	// SessionChanged reports a committed lifecycle change.
	SessionChanged(s *store.Session, from string)
	// Wake asks the Coordinator to look for work now rather than at its next tick.
	Wake()
}

// ArtifactRemover handles a deleted session's files.
type ArtifactRemover interface {
	// RetainSession moves a session's files out of managed storage, keeping them on disk.
	RetainSession(session string) error
	// PurgeSession deletes a session's files.
	PurgeSession(session string) error
}

// Service holds the use cases. Every interface calls these; none has its own rules.
type Service struct {
	DB        store.DB
	Config    *config.Config
	Clock     clock.Clock
	Notifier  Notifier
	Artifacts ArtifactRemover
	Vocab     snapshot.VocabularyDecoder
	// CatalogContent resolves a catalog reference; nil means catalogs are unavailable.
	CatalogContent func(ctx context.Context, ref CatalogRef) (json.RawMessage, int, error)
}

// NotFoundError builds the error for a missing session.
func NotFoundError(id string) error {
	return apperr.New(apperr.NotFound, CodeNotFound, "no session "+id)
}

// resolver adapts the service to Compile, which runs outside any transaction.
type resolver struct {
	ctx context.Context
	s   *Service
}

func (r resolver) SchemaDocument(ref SchemaRef) ([]byte, error) {
	var doc []byte
	err := r.s.DB.Read(r.ctx, func(ctx context.Context, tx store.Tx) error {
		row, err := tx.GetSchema(ctx, ref.ID, ref.Version)
		if err != nil {
			return err
		}
		doc = []byte(row.Definition)
		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("no schema %s", ref.ID)
	}
	return doc, err
}

func (r resolver) CatalogContent(ref CatalogRef) (json.RawMessage, int, error) {
	if r.s.CatalogContent == nil {
		return nil, 0, errors.New("catalogs are not available")
	}
	return r.s.CatalogContent(r.ctx, ref)
}

func chooseSeed() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("choose seed: %w", err)
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

// ValidateSpec checks a specification without storing anything.
func (s *Service) ValidateSpec(ctx context.Context, data []byte) (*Compiled, error) {
	spec, err := ParseSpec(data)
	if err != nil {
		return nil, err
	}
	seed, err := chooseSeed()
	if err != nil {
		return nil, err
	}
	return Compile(spec, s.Config, resolver{ctx, s}, s.Vocab, seed, s.Clock.Now())
}

// row fills a session row from a compiled specification.
func (s *Service) row(id string, c *Compiled, hash, actor string, now time.Time) (*store.Session, error) {
	specJSON, err := json.Marshal(c.Spec)
	if err != nil {
		return nil, err
	}
	limits, err := json.Marshal(c.Limits)
	if err != nil {
		return nil, err
	}
	return &store.Session{
		ID: id, Name: c.Spec.Name, Description: c.Spec.Description, Strategy: c.Strategy, Mode: c.Spec.Mode,
		Format: c.Snapshot.Format, SpecJSON: string(specJSON), SnapshotHash: hash, CatalogID: c.Catalog.ID,
		CatalogVersion: c.Catalog.Version, Seed: c.Snapshot.Seed, ReferenceTime: c.Snapshot.ReferenceTime,
		EngineVersion: c.Snapshot.EngineVersion, State: Created, LimitsJSON: string(limits), CreatedAt: now,
		CreatedBy: actor, Pinned: c.Spec.Retention.Pin, Rate: c.Spec.Limits.Rate, MaxWorkers: c.Spec.Limits.MaxWorkers,
	}, nil
}

// Create validates a specification and stores a new session. An invalid specification
// leaves nothing behind.
func (s *Service) Create(ctx context.Context, data []byte, actor string) (*store.Session, error) {
	c, err := s.ValidateSpec(ctx, data)
	if err != nil {
		return nil, err
	}
	hash, body, err := c.Snapshot.Encode()
	if err != nil {
		return nil, err
	}
	now := s.Clock.Now()
	row, err := s.row(ids.New("ses"), c, hash, actor, now)
	if err != nil {
		return nil, err
	}
	err = s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		if err := tx.PutSnapshot(ctx, hash, c.Snapshot.EngineVersion, body, now); err != nil {
			return err
		}
		if err := tx.InsertSession(ctx, row); err != nil {
			return err
		}
		return tx.InsertEvent(ctx, &store.Event{SessionID: row.ID, At: now, To: Created, Event: "create", Actor: actor})
	})
	if err != nil {
		return nil, err
	}
	return row, nil
}

// Configure replaces the specification of a session that has not started.
func (s *Service) Configure(ctx context.Context, id string, data []byte, expectVersion int64, actor string) (*store.Session, error) {
	c, err := s.ValidateSpec(ctx, data)
	if err != nil {
		return nil, err
	}
	hash, body, err := c.Snapshot.Encode()
	if err != nil {
		return nil, err
	}
	now := s.Clock.Now()
	var out *store.Session
	err = s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		cur, err := getSession(ctx, tx, id)
		if err != nil {
			return err
		}
		if cur.State != Created {
			return apperr.New(apperr.Conflict, CodeInvalidTransition, fmt.Sprintf("cannot configure a session that is %s", cur.State))
		}
		if expectVersion != 0 && cur.Version != expectVersion {
			return apperr.New(apperr.PreconditionFailed, CodeVersionMismatch, "the session has changed")
		}
		row, err := s.row(id, c, hash, cur.CreatedBy, cur.CreatedAt)
		if err != nil {
			return err
		}
		row.Version = cur.Version
		if err := tx.PutSnapshot(ctx, hash, c.Snapshot.EngineVersion, body, now); err != nil {
			return err
		}
		if err := tx.UpdateSession(ctx, row, cur.Version); err != nil {
			return err
		}
		out = row
		return tx.InsertEvent(ctx, &store.Event{SessionID: id, At: now, From: Created, To: Created, Event: "configure", Actor: actor})
	})
	return out, err
}

func getSession(ctx context.Context, tx store.Tx, id string) (*store.Session, error) {
	row, err := tx.GetSession(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, NotFoundError(id)
	}
	return row, err
}

// Apply performs a transition inside a transaction: the state machine decides, and the
// row and history change together. It reports whether anything changed.
func Apply(ctx context.Context, tx store.Tx, s *store.Session, event, actor, reason string, now time.Time) (bool, error) {
	outcome, err := Next(s.State, event)
	if err != nil {
		var invalid *InvalidTransition
		if errors.As(err, &invalid) {
			e := apperr.New(apperr.Conflict, CodeInvalidTransition, err.Error())
			e.Details = map[string]any{"current_state": s.State}
			return false, e
		}
		return false, err
	}
	if outcome.NoOp {
		return false, nil
	}
	from := s.State
	s.State = outcome.To
	if reason != "" {
		s.StateReason = reason
	}
	if IsTerminal(outcome.To) {
		s.EndedAt = &now
	}
	if err := tx.UpdateSession(ctx, s, s.Version); err != nil {
		return false, err
	}
	return true, tx.InsertEvent(ctx, &store.Event{SessionID: s.ID, At: now, From: from, To: outcome.To, Event: event, Actor: actor, Reason: reason})
}

// transition runs one user command in a write transaction.
func (s *Service) transition(ctx context.Context, id, event, actor string, change func(ctx context.Context, tx store.Tx, row *store.Session, now time.Time) error) (*store.Session, error) {
	now := s.Clock.Now()
	var out *store.Session
	var from string
	var changed bool
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		row, err := getSession(ctx, tx, id)
		if err != nil {
			return err
		}
		from = row.State
		if change != nil {
			if err := change(ctx, tx, row, now); err != nil {
				return err
			}
		}
		changed, err = Apply(ctx, tx, row, event, actor, "", now)
		if err != nil {
			return err
		}
		out = row
		return nil
	})
	if err != nil {
		return nil, err
	}
	if changed {
		s.Notifier.SessionChanged(out, from)
		s.Notifier.Wake()
	}
	return out, nil
}

// Start queues a session.
func (s *Service) Start(ctx context.Context, id, actor string) (*store.Session, error) {
	return s.transition(ctx, id, EvStart, actor, func(_ context.Context, _ store.Tx, row *store.Session, now time.Time) error {
		if row.State == Created {
			row.QueuedAt = &now
		}
		return nil
	})
}

// Pause stops dispatching work. Running units finish their part and give the unit back.
func (s *Service) Pause(ctx context.Context, id, actor string) (*store.Session, error) {
	return s.transition(ctx, id, EvPause, actor, nil)
}

// Resume continues a paused session. Time spent paused does not count against a duration.
func (s *Service) Resume(ctx context.Context, id, actor string) (*store.Session, error) {
	return s.transition(ctx, id, EvResume, actor, func(_ context.Context, _ store.Tx, row *store.Session, now time.Time) error {
		if row.State == Paused && row.PausedAt != nil {
			row.PausedTotal += now.Sub(*row.PausedAt)
			row.PausedAt = nil
		}
		return nil
	})
}

// Stop ends a session. A graceful stop keeps what running units are writing; an immediate
// stop discards it at once.
func (s *Service) Stop(ctx context.Context, id string, immediate bool, actor string) (*store.Session, error) {
	return s.transition(ctx, id, EvStop, actor, func(ctx context.Context, tx store.Tx, row *store.Session, _ time.Time) error {
		if IsTerminal(row.State) || row.State == Stopping || row.State == Queued || row.State == Created {
			return nil
		}
		row.StopImmediate = immediate
		return WithdrawUnits(ctx, tx, row.ID, immediate)
	})
}

// WithdrawUnits cancels pending units and, if asked, takes running ones back too. The
// fencing token moves first, so an upload already on its way cannot commit.
func WithdrawUnits(ctx context.Context, tx store.Tx, session string, leased bool) error {
	states := []string{store.UnitPending}
	if leased {
		states = append(states, store.UnitLeased)
	}
	for _, state := range states {
		units, err := tx.UnitsInState(ctx, session, state)
		if err != nil {
			return err
		}
		for i := range units {
			u := lease.Cancel(units[i])
			if err := tx.UpdateUnit(ctx, &u); err != nil {
				return err
			}
		}
	}
	return nil
}

// Delete removes a session that is not running. Its files stay on disk, outside managed
// storage, unless purge is set.
func (s *Service) Delete(ctx context.Context, id string, purge bool) error {
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		row, err := getSession(ctx, tx, id)
		if err != nil {
			return err
		}
		if !IsTerminal(row.State) && row.State != Created {
			return apperr.New(apperr.Conflict, CodeInvalidTransition, fmt.Sprintf("cannot delete a session that is %s: stop it first", row.State))
		}
		return tx.DeleteSession(ctx, id)
	})
	if err != nil {
		return err
	}
	if s.Artifacts == nil {
		return nil
	}
	if purge {
		return s.Artifacts.PurgeSession(id)
	}
	return s.Artifacts.RetainSession(id)
}

// Clone creates an editable session from another's specification, with a new seed unless
// the specification fixed one.
func (s *Service) Clone(ctx context.Context, id, actor string) (*store.Session, error) {
	cur, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	spec, err := ParseSpec([]byte(cur.SpecJSON))
	if err != nil {
		return nil, err
	}
	spec.Name += " (copy)"
	data, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	return s.Create(ctx, data, actor)
}

// Retry starts a new execution of exactly what a finished session ran: the same snapshot,
// so the same seed and bytes.
func (s *Service) Retry(ctx context.Context, id, actor string) (*store.Session, error) {
	now := s.Clock.Now()
	var out *store.Session
	err := s.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		cur, err := getSession(ctx, tx, id)
		if err != nil {
			return err
		}
		if !IsTerminal(cur.State) {
			return apperr.New(apperr.Conflict, CodeInvalidTransition, fmt.Sprintf("cannot retry a session that is %s", cur.State))
		}
		row := &store.Session{
			ID: ids.New("ses"), Name: cur.Name, Description: cur.Description, Strategy: cur.Strategy, Mode: cur.Mode, Format: cur.Format,
			SpecJSON: cur.SpecJSON, SnapshotHash: cur.SnapshotHash, CatalogID: cur.CatalogID, CatalogVersion: cur.CatalogVersion,
			Seed: cur.Seed, ReferenceTime: cur.ReferenceTime, EngineVersion: cur.EngineVersion, State: Queued, LimitsJSON: cur.LimitsJSON,
			CreatedAt: now, QueuedAt: &now, CreatedBy: actor, RetryOf: cur.ID, Rate: cur.Rate, MaxWorkers: cur.MaxWorkers,
		}
		if err := tx.InsertSession(ctx, row); err != nil {
			return err
		}
		out = row
		return tx.InsertEvent(ctx, &store.Event{SessionID: row.ID, At: now, To: Queued, Event: "retry", Actor: actor, Reason: "retry of " + cur.ID})
	})
	if err != nil {
		return nil, err
	}
	s.Notifier.Wake()
	return out, nil
}

// Get returns a session.
func (s *Service) Get(ctx context.Context, id string) (*store.Session, error) {
	var out *store.Session
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		out, err = getSession(ctx, tx, id)
		return err
	})
	return out, err
}

// List returns sessions.
func (s *Service) List(ctx context.Context, f store.SessionFilter) ([]store.Session, error) {
	var out []store.Session
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		out, err = tx.ListSessions(ctx, f)
		return err
	})
	return out, err
}

// Events returns a session's lifecycle history.
func (s *Service) Events(ctx context.Context, id string, limit int) ([]store.Event, error) {
	var out []store.Event
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		if _, err := getSession(ctx, tx, id); err != nil {
			return err
		}
		var err error
		out, err = tx.ListEvents(ctx, id, limit)
		return err
	})
	return out, err
}

// Parts returns a session's committed output.
func (s *Service) Parts(ctx context.Context, id string) ([]store.Part, error) {
	var out []store.Part
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		if _, err := getSession(ctx, tx, id); err != nil {
			return err
		}
		var err error
		out, err = tx.SessionParts(ctx, id)
		return err
	})
	return out, err
}

// LoadSnapshot reads and verifies a session's snapshot.
func (s *Service) LoadSnapshot(ctx context.Context, hash string) (*snapshot.Snapshot, error) {
	var body []byte
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		body, err = tx.GetSnapshot(ctx, hash)
		return err
	})
	if err != nil {
		return nil, err
	}
	return snapshot.Decode(body, hash)
}

// Progress summarizes where a session stands.
type Progress struct {
	Units     map[string]int64
	Expected  float64 // expected total records; 0 when unbounded
	Exhausted bool    // every stream has been fully planned
}

// LoadProgress reads a session's unit counts and expected size.
func (s *Service) LoadProgress(ctx context.Context, id string) (Progress, error) {
	var p Progress
	err := s.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		if p.Units, err = tx.CountUnits(ctx, id); err != nil {
			return err
		}
		streams, err := tx.StreamStates(ctx, id)
		if err != nil {
			return err
		}
		p.Exhausted = len(streams) > 0
		for i := range streams {
			if streams[i].End < unboundedRows {
				p.Expected += streams[i].ExpectedRows
			}
			p.Exhausted = p.Exhausted && streams[i].Exhausted
		}
		return nil
	})
	return p, err
}

// StreamStatesFor builds a session's streams from its engine: the plan a Coordinator
// follows lazily. Records in an unbounded stream end at the top of the index space.
func StreamStatesFor(session string, streams []gen.StreamSpec) []store.StreamState {
	out := make([]store.StreamState, len(streams))
	for i, spec := range streams {
		end := spec.Space
		if end == 0 {
			end = unboundedRows
		}
		out[i] = store.StreamState{
			SessionID: session, Stream: string(spec.ID), Position: i, End: end, PerParent: spec.PerParent,
			ExpectedRows: spec.ExpectedRows, MaxRecordBytes: spec.MaxRecordBytes,
		}
	}
	return out
}
