package coord

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/genadigeno/data-feed/internal/apperr"
	"github.com/genadigeno/data-feed/internal/artifact"
	"github.com/genadigeno/data-feed/internal/clock"
	"github.com/genadigeno/data-feed/internal/config"
	"github.com/genadigeno/data-feed/internal/coord/lease"
	"github.com/genadigeno/data-feed/internal/gen"
	"github.com/genadigeno/data-feed/internal/hub"
	"github.com/genadigeno/data-feed/internal/ids"
	"github.com/genadigeno/data-feed/internal/obs"
	"github.com/genadigeno/data-feed/internal/output"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/snapshot"
	"github.com/genadigeno/data-feed/internal/store"
	"github.com/genadigeno/data-feed/internal/wire"
)

// LocalWorkerID is the embedded worker, which needs no credential: it is this process.
const LocalWorkerID = "local"

const (
	progressInterval  = 2 * time.Second
	sampleInterval    = 10 * time.Second
	maxSamples        = 8640 // 24 hours of 10-second buckets
	gcInterval        = time.Hour
	snapshotGCAge     = time.Hour
	aiCacheMaxAge     = 90 * 24 * time.Hour
	uploadSlack       = 1 << 20 // bytes beyond part_max_bytes, for the last record and footer
	copyBufferBytes   = 256 << 10
	disconnectAfter   = 3 // missed heartbeat intervals
	maxCachedCompiled = 16
)

// Options configures a Core.
type Options struct {
	DB        store.DB
	Artifacts *artifact.Store
	Config    *config.Config
	Clock     clock.Clock
	Logger    *slog.Logger
	Hub       *hub.Hub
	Logs      *obs.SessionLogs
	// FreeBytes reads free space where artifacts live.
	FreeBytes func() (uint64, error)
	Vocab     snapshot.VocabularyDecoder
	// OpenRegistration lets Workers register with no join token, for development on
	// loopback only; startup decides.
	OpenRegistration bool
}

// Core is the Coordinator's control plane.
type Core struct {
	o   Options
	cfg *config.Config
	clk clock.Clock
	log *slog.Logger

	mu       sync.Mutex
	renewals map[string]time.Time // lease ID → when this process last renewed it
	orphans  *lease.OrphanTracker
	seen     map[string]time.Time // worker → last contact
	caps     map[string]*wire.Capabilities
	inFlight map[string]map[string]wire.UnitProgress // session → unit → progress
	waiters  []chan struct{}
	rotation int
	uploads  map[string]bool // units with an upload in progress
	live     map[string]*liveStats
	lowDisk  time.Time
	draining bool

	uploadSlots chan struct{}
	wake        chan struct{}

	compiledMu sync.Mutex
	compiled   map[string]*snapshot.Compiled
	compiledAt []string
}

// liveStats tracks a session's rates, which need no durability.
type liveStats struct {
	lastCommitted uint64
	lastBytes     int64
	lastInFlight  uint64
	lastAt        time.Time
	rate          float64
	bucket        time.Time
	bucketRecords uint64
	bucketBytes   int64
}

// New returns a Core.
func New(o Options) *Core {
	return &Core{
		o: o, cfg: o.Config, clk: o.Clock, log: o.Logger.With("component", "coord"),
		renewals:    map[string]time.Time{},
		orphans:     lease.NewOrphanTracker(o.Config.Coordinator.OrphanLeaseHeartbeats),
		seen:        map[string]time.Time{},
		caps:        map[string]*wire.Capabilities{},
		inFlight:    map[string]map[string]wire.UnitProgress{},
		uploads:     map[string]bool{},
		live:        map[string]*liveStats{},
		uploadSlots: make(chan struct{}, o.Config.Coordinator.PartUploadConcurrency),
		wake:        make(chan struct{}, 1),
		compiled:    map[string]*snapshot.Compiled{},
	}
}

var _ wire.Coordinator = (*Core)(nil)

func fenced(err error) error {
	return apperr.Wrap(err, apperr.Conflict, wire.CodeLeaseFenced, "this lease is no longer current")
}

func unknownUnit(id string) error {
	return apperr.New(apperr.Conflict, wire.CodeLeaseUnknown, "no unit "+id)
}

// Wake asks for an immediate look at sessions and waiting Workers.
func (c *Core) Wake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// SessionChanged publishes a committed lifecycle change.
func (c *Core) SessionChanged(s *store.Session, from string) {
	c.o.Hub.Publish(hub.Event{
		Type: hub.TypeState, SessionID: s.ID,
		Data: map[string]any{"session_id": s.ID, "from": from, "to": s.State, "available_actions": session.AvailableActions(s.State), "reason": s.StateReason},
	})
	c.logSession(s.ID, slog.LevelInfo, "state changed", "from", from, "to", s.State, "reason", s.StateReason)
}

func (c *Core) logSession(id string, level slog.Level, msg string, attrs ...any) {
	c.log.Log(context.Background(), level, msg, append([]any{"session_id", id}, attrs...)...)
	if c.o.Logs != nil {
		c.o.Logs.Append(id, level, msg, attrs...)
	}
}

// notifyWaiters wakes up to n waiting Workers, first come first served, so a single new
// unit does not start every Worker on a pointless write transaction.
func (c *Core) notifyWaiters(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for n > 0 && len(c.waiters) > 0 {
		close(c.waiters[0])
		c.waiters = c.waiters[1:]
		n--
	}
}

func (c *Core) notifyAllWaiters() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.waiters {
		close(w)
	}
	c.waiters = nil
}

func (c *Core) addWaiter() chan struct{} {
	ch := make(chan struct{})
	c.mu.Lock()
	c.waiters = append(c.waiters, ch)
	c.mu.Unlock()
	return ch
}

func (c *Core) removeWaiter(ch chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, w := range c.waiters {
		if w == ch {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			return
		}
	}
}

// Register implements wire.Coordinator.
func (c *Core) Register(ctx context.Context, r wire.RegisterRequest) (wire.RegisterResponse, error) {
	if r.ProtocolVersion != wire.ProtocolVersion {
		return wire.RegisterResponse{}, apperr.New(apperr.Conflict, wire.CodeProtocolVersion,
			fmt.Sprintf("this Coordinator speaks protocol version %d, not %d", wire.ProtocolVersion, r.ProtocolVersion))
	}
	capsJSON, err := json.Marshal(r.Capabilities)
	if err != nil {
		return wire.RegisterResponse{}, err
	}
	now := c.clk.Now()
	var workerID, issued string
	err = c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		issued = ""
		if id, secret, ok := ParseCredential(r.Credential); ok {
			w, err := tx.GetWorker(ctx, id)
			if errors.Is(err, store.ErrNotFound) || (err == nil && !CredentialMatches(w.CredentialHash, secret)) {
				return apperr.New(apperr.Unauthenticated, "ERR_UNAUTHENTICATED", "unknown Worker credential")
			}
			if err != nil {
				return err
			}
			if w.State == store.WorkerRevoked {
				return apperr.New(apperr.PermissionDenied, wire.CodeWorkerRevoked, "this Worker has been revoked")
			}
			w.Name, w.Host, w.CapabilitiesJSON, w.Slots, w.State = r.Name, r.Host, string(capsJSON), r.Capabilities.Slots, store.WorkerActive
			w.LastHeartbeatAt = &now
			workerID = id
			return tx.PutWorker(ctx, w)
		}

		if !c.o.OpenRegistration {
			token, err := tx.FindJoinToken(ctx, HashSecret(r.JoinToken))
			if errors.Is(err, store.ErrNotFound) || (err == nil && !JoinTokenUsable(token, now)) {
				return apperr.New(apperr.Unauthenticated, "ERR_UNAUTHENTICATED", "a valid join token is required to register")
			}
			if err != nil {
				return err
			}
			if err := tx.UpdateJoinTokenUses(ctx, token.ID, token.Uses+1); err != nil {
				return err
			}
		}
		count, err := tx.CountWorkers(ctx)
		if err != nil {
			return err
		}
		if count >= int64(c.cfg.Coordinator.MaxRegisteredWorkers) {
			return apperr.New(apperr.Conflict, "ERR_TOO_MANY_WORKERS", fmt.Sprintf("the limit of %d registered Workers is reached", c.cfg.Coordinator.MaxRegisteredWorkers))
		}
		workerID = ids.New("wrk")
		secret := ids.Secret()
		issued = workerID + "." + secret
		return tx.PutWorker(ctx, &store.Worker{
			ID: workerID, Name: r.Name, Host: r.Host, CredentialHash: HashSecret(secret), CapabilitiesJSON: string(capsJSON),
			Slots: r.Capabilities.Slots, State: store.WorkerActive, RegisteredAt: now, LastHeartbeatAt: &now,
		})
	})
	if err != nil {
		return wire.RegisterResponse{}, err
	}
	c.noteWorker(workerID, &r.Capabilities, now)
	c.log.Info("worker registered", "worker_id", workerID, "name", r.Name, "slots", r.Capabilities.Slots, "arch", r.Capabilities.Arch)
	return wire.RegisterResponse{
		WorkerID: workerID, Credential: issued,
		HeartbeatInterval: c.cfg.Coordinator.HeartbeatInterval.Milliseconds(),
		LeaseDuration:     c.cfg.Coordinator.LeaseDuration.Milliseconds(),
	}, nil
}

// RegisterLocal registers the embedded worker, which is part of this process.
func (c *Core) RegisterLocal(ctx context.Context, caps wire.Capabilities) (wire.RegisterResponse, error) {
	capsJSON, err := json.Marshal(caps)
	if err != nil {
		return wire.RegisterResponse{}, err
	}
	now := c.clk.Now()
	err = c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		return tx.PutWorker(ctx, &store.Worker{
			ID: LocalWorkerID, Name: "embedded", CredentialHash: []byte{}, CapabilitiesJSON: string(capsJSON),
			Slots: caps.Slots, State: store.WorkerActive, RegisteredAt: now, LastHeartbeatAt: &now,
		})
	})
	if err != nil {
		return wire.RegisterResponse{}, err
	}
	c.noteWorker(LocalWorkerID, &caps, now)
	return wire.RegisterResponse{
		WorkerID: LocalWorkerID, HeartbeatInterval: c.cfg.Coordinator.HeartbeatInterval.Milliseconds(),
		LeaseDuration: c.cfg.Coordinator.LeaseDuration.Milliseconds(),
	}, nil
}

// Authenticate checks a Worker credential, for the network transport.
func (c *Core) Authenticate(ctx context.Context, credential string) (string, error) {
	id, secret, ok := ParseCredential(credential)
	if !ok {
		return "", apperr.New(apperr.Unauthenticated, "ERR_UNAUTHENTICATED", "a Worker credential is required")
	}
	var w *store.Worker
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		w, err = tx.GetWorker(ctx, id)
		return err
	})
	if errors.Is(err, store.ErrNotFound) || (err == nil && !CredentialMatches(w.CredentialHash, secret)) {
		return "", apperr.New(apperr.Unauthenticated, "ERR_UNAUTHENTICATED", "unknown Worker credential")
	}
	if err != nil {
		return "", err
	}
	if w.State == store.WorkerRevoked {
		return "", apperr.New(apperr.PermissionDenied, wire.CodeWorkerRevoked, "this Worker has been revoked")
	}
	return id, nil
}

func (c *Core) noteWorker(id string, caps *wire.Capabilities, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen[id] = now
	if caps != nil {
		c.caps[id] = caps
	}
}

func (c *Core) workerCaps(ctx context.Context, id string) (*wire.Capabilities, error) {
	c.mu.Lock()
	caps := c.caps[id]
	c.mu.Unlock()
	if caps != nil {
		return caps, nil
	}
	var w *store.Worker
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		w, err = tx.GetWorker(ctx, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	var parsed wire.Capabilities
	if err := json.Unmarshal([]byte(w.CapabilitiesJSON), &parsed); err != nil {
		return nil, err
	}
	c.noteWorker(id, &parsed, c.clk.Now())
	return &parsed, nil
}

// Heartbeat implements wire.Coordinator.
func (c *Core) Heartbeat(ctx context.Context, worker string, h wire.Heartbeat) (wire.HeartbeatResponse, error) {
	now := c.clk.Now()
	d := c.cfg.Coordinator.LeaseDuration
	resp := wire.HeartbeatResponse{LeaseTTLMillis: d.Milliseconds()}

	reported := make(map[string]wire.UnitProgress, len(h.Units))
	reportedIDs := make(map[string]bool, len(h.Units))
	for _, u := range h.Units {
		reported[u.UnitID] = u
		reportedIDs[u.LeaseID] = true
	}

	var renewedLeases []string
	progress := map[string]map[string]wire.UnitProgress{}
	var revoked int
	var adminDrain bool
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		resp.Lost, resp.Directives, renewedLeases, revoked = nil, nil, nil, 0
		clear(progress)

		w, err := tx.GetWorker(ctx, worker)
		if err != nil {
			return err
		}
		if w.State == store.WorkerRevoked {
			return apperr.New(apperr.PermissionDenied, wire.CodeWorkerRevoked, "this Worker has been revoked")
		}

		// Anything the Worker reports that is not its current lease is lost.
		for _, u := range h.Units {
			row, err := tx.GetUnit(ctx, u.UnitID)
			if errors.Is(err, store.ErrNotFound) {
				resp.Lost = append(resp.Lost, u.LeaseID)
				continue
			}
			if err != nil {
				return err
			}
			renewed, err := lease.Renew(*row, lease.Ref{UnitID: u.UnitID, LeaseID: u.LeaseID, WorkerID: worker, FencingToken: u.FencingToken}, now, d)
			if err != nil {
				resp.Lost = append(resp.Lost, u.LeaseID)
				continue
			}
			if err := tx.UpdateUnit(ctx, &renewed); err != nil {
				return err
			}
			renewedLeases = append(renewedLeases, u.LeaseID)
		}

		held, err := tx.LeasedByWorker(ctx, worker)
		if err != nil {
			return err
		}
		for _, orphan := range c.observeOrphans(worker, held, reportedIDs, now) {
			u := lease.Revoke(orphan)
			if err := tx.UpdateUnit(ctx, &u); err != nil {
				return err
			}
			revoked++
		}

		// Directives come from each unit's session.
		sessions := map[string]*store.Session{}
		leasedCount := map[string]int64{}
		for i := range held {
			u := &held[i]
			if _, ok := reported[u.ID]; !ok {
				continue
			}
			s, ok := sessions[u.SessionID]
			if !ok {
				s, err = tx.GetSession(ctx, u.SessionID)
				if err != nil {
					return err
				}
				sessions[u.SessionID] = s
				counts, err := tx.CountUnits(ctx, u.SessionID)
				if err != nil {
					return err
				}
				leasedCount[u.SessionID] = counts[store.UnitLeased]
			}
			if progress[u.SessionID] == nil {
				progress[u.SessionID] = map[string]wire.UnitProgress{}
			}
			progress[u.SessionID][u.ID] = reported[u.ID]
			if dir, ok := DirectiveFor(s, u.ID); ok {
				resp.Directives = append(resp.Directives, dir)
				continue
			}
			if s.Rate > 0 {
				resp.Directives = append(resp.Directives, wire.Directive{UnitID: u.ID, Action: wire.DirectiveSetRate, Rate: UnitRate(s.Rate, int(leasedCount[u.SessionID]))})
			}
		}

		// A drain, whether the Worker's own or an administrator's, lasts until it re-registers.
		state := store.WorkerActive
		if h.Draining || w.State == store.WorkerDraining {
			state = store.WorkerDraining
		}
		adminDrain = state == store.WorkerDraining && !h.Draining
		persistDue := w.LastHeartbeatAt == nil || now.Sub(*w.LastHeartbeatAt) >= c.cfg.Coordinator.HeartbeatPersistInterval
		if persistDue || w.State != state {
			w.State = state
			w.LastHeartbeatAt = &now
			return tx.PutWorker(ctx, w)
		}
		return nil
	})
	if err != nil {
		return wire.HeartbeatResponse{}, err
	}

	c.mu.Lock()
	c.seen[worker] = now
	for _, id := range renewedLeases {
		c.renewals[id] = now
	}
	for sessionID, units := range progress {
		if c.inFlight[sessionID] == nil {
			c.inFlight[sessionID] = map[string]wire.UnitProgress{}
		}
		for unit, p := range units {
			c.inFlight[sessionID][unit] = p
		}
	}
	draining := c.draining
	c.mu.Unlock()

	if draining || adminDrain {
		resp.Directives = append(resp.Directives, wire.Directive{Action: wire.DirectiveDrain})
	}
	if revoked > 0 {
		c.notifyWaiters(revoked)
	}
	return resp, nil
}

func (c *Core) observeOrphans(worker string, held []store.Unit, reported map[string]bool, now time.Time) []store.Unit {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.orphans.Observe(worker, held, reported, now)
}

// Lease implements wire.Coordinator.
func (c *Core) Lease(ctx context.Context, worker string, maxUnits int, wait time.Duration) ([]wire.Assignment, error) {
	if maxUnits <= 0 {
		return nil, nil
	}
	maxUnits = min(maxUnits, c.cfg.Coordinator.MaxLeasesPerRequest)
	wait = min(wait, c.cfg.Coordinator.LongPollMax)
	deadline := c.clk.After(wait)
	for {
		grants, err := c.grant(ctx, worker, maxUnits)
		if err != nil || len(grants) > 0 {
			return grants, err
		}
		// A parked long poll would hold up the server's shutdown.
		c.mu.Lock()
		draining := c.draining
		c.mu.Unlock()
		if draining {
			return nil, nil
		}
		waiter := c.addWaiter()
		select {
		case <-waiter:
		case <-deadline:
			c.removeWaiter(waiter)
			return nil, nil
		case <-ctx.Done():
			c.removeWaiter(waiter)
			return nil, ctx.Err()
		}
	}
}

func (c *Core) grant(ctx context.Context, worker string, maxUnits int) ([]wire.Assignment, error) {
	c.mu.Lock()
	draining := c.draining
	rotation := c.rotation
	c.rotation++
	c.mu.Unlock()
	if draining {
		return nil, nil
	}
	caps, err := c.workerCaps(ctx, worker)
	if err != nil {
		return nil, err
	}
	now := c.clk.Now()
	var out []wire.Assignment
	var renewals []string
	err = c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		out, renewals = nil, nil
		w, err := tx.GetWorker(ctx, worker)
		if err != nil {
			return err
		}
		if w.State != store.WorkerActive {
			return nil
		}

		rows, err := tx.SessionsInStates(ctx, session.Running)
		if err != nil {
			return err
		}
		rows = slices.DeleteFunc(rows, func(s store.Session) bool { return s.Finishing })
		cands := make([]Candidate, len(rows))
		for i := range rows {
			s := &rows[i]
			counts, err := tx.CountUnits(ctx, s.ID)
			if err != nil {
				return err
			}
			limits := ParseLimits(s)
			maxWorkers := c.cfg.Generation.MaxWorkersPerSession
			if s.MaxWorkers > 0 {
				maxWorkers = min(maxWorkers, s.MaxWorkers)
			}
			cands[i] = Candidate{
				SessionID: s.ID, Leased: int(counts[store.UnitLeased]), MaxWorkers: maxWorkers, Engine: s.EngineVersion, Format: s.Format,
				Strategy: s.Strategy, Arch: s.WorkerArch, Selector: limits.Selector, UnitMemory: limits.UnitMemoryBytes, Rate: s.Rate,
			}
		}

		turn := rotation
		for len(out) < maxUnits {
			at := Pick(cands, caps, turn)
			if at < 0 {
				return nil
			}
			turn = at + 1
			cand := &cands[at]
			s := &rows[at]

			unit, err := c.nextUnit(ctx, tx, s)
			if err != nil {
				return err
			}
			if unit == nil {
				cand.Empty = true
				continue
			}
			leaseID := ids.New("lse")
			granted, err := lease.Grant(*unit, worker, leaseID, now, c.cfg.Coordinator.LeaseDuration)
			if err != nil {
				return err
			}
			if err := tx.UpdateUnit(ctx, &granted); err != nil {
				return err
			}
			cand.Leased++
			if s.WorkerArch == "" {
				s.WorkerArch, cand.Arch = caps.Arch, caps.Arch
				if err := tx.UpdateSession(ctx, s, s.Version); err != nil {
					return err
				}
			}
			snap, err := c.snapshotFor(ctx, tx, s.SnapshotHash)
			if err != nil {
				return err
			}
			out = append(out, assignment(&granted, s, snap, cand, c.cfg))
			renewals = append(renewals, leaseID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	for _, id := range renewals {
		c.renewals[id] = now
	}
	c.seen[worker] = now
	c.mu.Unlock()
	return out, nil
}

func assignment(u *store.Unit, s *store.Session, snap *snapshot.Snapshot, cand *Candidate, cfg *config.Config) wire.Assignment {
	return wire.Assignment{
		LeaseRef:       wire.LeaseRef{UnitID: u.ID, LeaseID: u.LeaseID, FencingToken: u.FencingToken},
		SessionID:      s.ID,
		LeaseTTLMillis: cfg.Coordinator.LeaseDuration.Milliseconds(),
		SnapshotHash:   s.SnapshotHash,
		Stream:         u.Stream,
		UnitSeq:        u.Seq,
		RangeStart:     u.RangeStart,
		RangeEnd:       u.RangeEnd,
		ResumeFrom:     u.CommittedThrough,
		NextPartSeq:    u.NextPartSeq,
		PartLimits:     wire.PartLimits{MaxBytes: snap.PartMaxBytes, MaxRecords: snap.PartMaxRecords, MaxAgeMS: snap.PartMaxAgeMS},
		Rate:           UnitRate(s.Rate, cand.Leased),
	}
}

// nextUnit returns a pending unit, planning a new one when none is waiting.
func (c *Core) nextUnit(ctx context.Context, tx store.Tx, s *store.Session) (*store.Unit, error) {
	unit, err := tx.NextPendingUnit(ctx, s.ID)
	if err == nil {
		return unit, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	streams, err := tx.StreamStates(ctx, s.ID)
	if err != nil {
		return nil, err
	}
	snap, err := c.snapshotFor(ctx, tx, s.SnapshotHash)
	if err != nil {
		return nil, err
	}
	plan := gen.PlanConfig{UnitRecords: snap.UnitRecords, UnitMaxBytes: snap.UnitMaxBytes}
	for i := range streams {
		st := &streams[i]
		if st.Exhausted {
			continue
		}
		spec := gen.StreamSpec{
			ID: gen.StreamID(st.Stream), Space: st.End, PerParent: st.PerParent, ExpectedRows: st.ExpectedRows, MaxRecordBytes: st.MaxRecordBytes,
		}
		next, cursor, err := gen.NextUnit(spec, plan, gen.Cursor{Seq: st.NextSeq, Next: st.NextIndex}, 0)
		if errors.Is(err, gen.ErrStreamEnd) {
			st.Exhausted = true
			if err := tx.UpdateStreamState(ctx, st); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, err
		}
		st.NextIndex, st.NextSeq = cursor.Next, cursor.Seq
		st.Exhausted = cursor.Next >= st.End
		if err := tx.UpdateStreamState(ctx, st); err != nil {
			return nil, err
		}
		u := &store.Unit{
			ID: ids.New("unt"), SessionID: s.ID, Stream: st.Stream, Seq: next.Seq, RangeStart: next.Start, RangeEnd: next.End,
			CommittedThrough: next.Start, State: store.UnitPending,
		}
		if err := tx.InsertUnit(ctx, u); err != nil {
			return nil, err
		}
		return u, nil
	}
	return nil, nil
}

// snapshotFor reads a snapshot within a transaction, through a small cache.
func (c *Core) snapshotFor(ctx context.Context, tx store.Tx, hash string) (*snapshot.Snapshot, error) {
	c.compiledMu.Lock()
	cached := c.compiled[hash]
	c.compiledMu.Unlock()
	if cached != nil {
		return cached.Snapshot, nil
	}
	body, err := tx.GetSnapshot(ctx, hash)
	if err != nil {
		return nil, err
	}
	snap, err := snapshot.Decode(body, hash)
	if err != nil {
		return nil, err
	}
	c.compiledMu.Lock()
	defer c.compiledMu.Unlock()
	c.compiled[hash] = &snapshot.Compiled{Snapshot: snap}
	c.compiledAt = append(c.compiledAt, hash)
	if len(c.compiledAt) > maxCachedCompiled {
		delete(c.compiled, c.compiledAt[0])
		c.compiledAt = c.compiledAt[1:]
	}
	return snap, nil
}

// Snapshot implements wire.Coordinator.
func (c *Core) Snapshot(ctx context.Context, _, hash string) ([]byte, error) {
	var body []byte
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		body, err = tx.GetSnapshot(ctx, hash)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return nil, apperr.New(apperr.NotFound, session.CodeNotFound, "no snapshot "+hash)
	}
	return body, err
}

// PartCommitted implements wire.Coordinator.
func (c *Core) PartCommitted(ctx context.Context, _, unit string, seq int) (bool, string, error) {
	var part *store.Part
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		part, err = tx.GetPart(ctx, unit, seq)
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, part.SHA256, nil
}

// UploadPart implements wire.Coordinator. See storage-design.md for the protocol: the file
// reaches its final name before the database commit, which also re-checks the lease.
func (c *Core) UploadPart(ctx context.Context, worker string, m wire.PartMeta, body wire.PartBody) (wire.UploadResult, error) {
	if !c.beginUpload(m.UnitID) {
		return wire.UploadResult{}, apperr.New(apperr.Conflict, wire.CodeUploadInProgress, "another upload for this unit is in progress")
	}
	defer c.endUpload(m.UnitID)

	ref := lease.Ref{UnitID: m.UnitID, LeaseID: m.LeaseID, WorkerID: worker, FencingToken: m.FencingToken}
	meta := lease.PartMeta{Seq: m.Seq, FirstIndex: m.FirstIndex, EndIndex: m.EndIndex, Records: m.Records, Bytes: m.Bytes, SHA256: m.SHA256}

	// A fast check before receiving a large body.
	var unit store.Unit
	var limit int64
	var existing *store.Part
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		row, err := tx.GetUnit(ctx, m.UnitID)
		if errors.Is(err, store.ErrNotFound) {
			return unknownUnit(m.UnitID)
		}
		if err != nil {
			return err
		}
		unit = *row
		s, err := tx.GetSession(ctx, unit.SessionID)
		if err != nil {
			return err
		}
		snap, err := c.snapshotFor(ctx, tx, s.SnapshotHash)
		if err != nil {
			return err
		}
		limit = snap.PartMaxBytes + uploadSlack
		part, err := tx.GetPart(ctx, m.UnitID, m.Seq)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		existing = part
		return nil
	})
	if err != nil {
		return wire.UploadResult{}, err
	}
	if existing != nil {
		if lease.ClassifyPart(unit, existing, meta) == lease.PartDuplicate {
			return wire.UploadResult{CommittedThrough: unit.CommittedThrough, Duplicate: true}, nil
		}
		return wire.UploadResult{}, apperr.New(apperr.Conflict, wire.CodePartConflict, "this part was already committed with different content")
	}
	if err := lease.Check(unit, ref, c.clk.Now()); err != nil {
		return wire.UploadResult{}, fenced(err)
	}
	if m.Bytes > limit {
		return wire.UploadResult{}, apperr.New(apperr.ResourceExhausted, wire.CodePartTooLarge, fmt.Sprintf("part of %d bytes exceeds the limit of %d", m.Bytes, limit))
	}

	select {
	case c.uploadSlots <- struct{}{}:
		defer func() { <-c.uploadSlots }()
	default:
		return wire.UploadResult{}, apperr.New(apperr.Unavailable, wire.CodeOverloaded, "too many uploads in progress")
	}
	if c.o.FreeBytes != nil {
		if free, err := c.o.FreeBytes(); err == nil && free < uint64(c.cfg.Storage.MinFreeBytes) {
			c.noteLowDisk()
			return wire.UploadResult{}, apperr.New(apperr.Unavailable, wire.CodeDiskLow, "the Coordinator is low on disk space")
		}
		c.clearLowDisk()
	}

	staged, sum, written, err := c.stage(body, m.Bytes)
	if err != nil {
		return wire.UploadResult{}, err
	}
	defer os.Remove(staged) // already gone after a successful promotion
	if written != m.Bytes {
		return wire.UploadResult{}, apperr.New(apperr.InvalidArgument, wire.CodeChecksumMismatch, fmt.Sprintf("received %d bytes of %d", written, m.Bytes))
	}
	if sum != "" && sum != m.SHA256 {
		return wire.UploadResult{}, apperr.New(apperr.InvalidArgument, wire.CodeChecksumMismatch, "the part's checksum does not match")
	}

	relative, err := artifact.PartPath(unit.SessionID, output.StreamDir(unit.Stream), int(unit.Seq), m.Seq, m.FencingToken, m.Extension)
	if err != nil {
		return wire.UploadResult{}, apperr.Wrap(err, apperr.InvalidArgument, "ERR_UNSAFE_PATH", "invalid part identity")
	}
	if err := c.o.Artifacts.Promote(staged, relative); err != nil {
		return wire.UploadResult{}, err
	}

	var result wire.UploadResult
	committed := false
	now := c.clk.Now()
	err = c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		committed = false
		row, err := tx.GetUnit(ctx, m.UnitID)
		if errors.Is(err, store.ErrNotFound) {
			return unknownUnit(m.UnitID)
		}
		if err != nil {
			return err
		}
		if err := lease.Check(*row, ref, now); err != nil {
			return fenced(err)
		}
		switch lease.ClassifyPart(*row, nil, meta) {
		case lease.PartNew:
		default:
			e := apperr.New(apperr.Conflict, wire.CodePartOutOfOrder, fmt.Sprintf("expected part %d starting at %d", row.NextPartSeq, row.CommittedThrough))
			e.Details = map[string]any{"expected_seq": row.NextPartSeq, "expected_index": row.CommittedThrough}
			return e
		}
		if err := tx.InsertPart(ctx, &store.Part{
			UnitID: m.UnitID, Seq: m.Seq, SessionID: row.SessionID, Stream: row.Stream, UnitSeq: row.Seq, FirstIndex: m.FirstIndex, EndIndex: m.EndIndex,
			Records: m.Records, Bytes: m.Bytes, SHA256: m.SHA256, RelPath: relative, HeaderLen: m.HeaderLen, FooterLen: m.FooterLen,
			FencingToken: m.FencingToken, WorkerID: worker, CommittedAt: now,
		}); err != nil {
			return err
		}
		updated := lease.ApplyPart(*row, meta)
		if err := tx.UpdateUnit(ctx, &updated); err != nil {
			return err
		}
		s, err := tx.GetSession(ctx, row.SessionID)
		if err != nil {
			return err
		}
		s.Records += m.Records
		s.Bytes += m.Bytes
		s.LastActivityAt = &now
		if err := tx.UpdateSession(ctx, s, s.Version); err != nil {
			return err
		}
		result = wire.UploadResult{CommittedThrough: updated.CommittedThrough}
		committed = true
		return nil
	})
	if err != nil {
		if !committed {
			// Nothing refers to the promoted file: remove it. Reconciliation would too.
			if rmErr := c.o.Artifacts.Remove(relative); rmErr != nil {
				c.log.Warn("remove rejected part", "path", relative, "err", rmErr)
			}
		}
		return wire.UploadResult{}, err
	}
	c.o.Hub.Publish(hub.Event{
		Type: hub.TypeArtifact, SessionID: unit.SessionID,
		Data: map[string]any{"session_id": unit.SessionID, "stream": unit.Stream, "records": m.Records, "bytes": m.Bytes},
	})
	return result, nil
}

func (c *Core) beginUpload(unit string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.uploads[unit] {
		return false
	}
	c.uploads[unit] = true
	return true
}

func (c *Core) endUpload(unit string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.uploads, unit)
}

func (c *Core) noteLowDisk() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lowDisk.IsZero() {
		c.lowDisk = c.clk.Now()
	}
}

func (c *Core) clearLowDisk() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lowDisk = time.Time{}
}

// stage puts a part's bytes in the staging directory, returning its path and checksum. The
// embedded worker's file is already on this filesystem, so it moves without a copy; its
// checksum comes from this same process and is not recomputed.
func (c *Core) stage(body wire.PartBody, size int64) (path, sum string, written int64, err error) {
	if body.LocalPath != "" {
		info, err := os.Stat(body.LocalPath)
		if err != nil {
			return "", "", 0, err
		}
		return body.LocalPath, "", info.Size(), nil
	}
	f, err := c.o.Artifacts.StagingFile()
	if err != nil {
		return "", "", 0, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	h := newHash()
	buf := make([]byte, copyBufferBytes)
	written, err = io.CopyBuffer(io.MultiWriter(f, h), io.LimitReader(body.Reader, size+1), buf)
	if err != nil {
		_ = f.Close()
		return "", "", 0, fmt.Errorf("receive part: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return "", "", 0, fmt.Errorf("sync part: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", "", 0, fmt.Errorf("close part: %w", err)
	}
	return f.Name(), hex.EncodeToString(h.Sum(nil)), written, nil
}

// Complete implements wire.Coordinator.
func (c *Core) Complete(ctx context.Context, worker string, ref wire.LeaseRef, end uint64, parts int) error {
	r := lease.Ref{UnitID: ref.UnitID, LeaseID: ref.LeaseID, WorkerID: worker, FencingToken: ref.FencingToken}
	now := c.clk.Now()
	var sessionID string
	var changed *store.Session
	var from string
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		changed, from = nil, ""
		row, err := tx.GetUnit(ctx, ref.UnitID)
		if errors.Is(err, store.ErrNotFound) {
			return unknownUnit(ref.UnitID)
		}
		if err != nil {
			return err
		}
		s, err := tx.GetSession(ctx, row.SessionID)
		if err != nil {
			return err
		}
		sessionID = s.ID
		finishing := s.Finishing || s.State == session.Stopping || s.State == session.Pausing
		done, err := lease.Complete(*row, r, end, parts, finishing, now)
		switch {
		case errors.Is(err, lease.ErrFenced):
			return fenced(err)
		case errors.Is(err, lease.ErrIncomplete):
			return apperr.Wrap(err, apperr.Conflict, wire.CodeUnitIncomplete, err.Error())
		case err != nil:
			return err
		}
		if err := tx.UpdateUnit(ctx, &done); err != nil {
			return err
		}
		s.LastActivityAt = &now
		if err := tx.UpdateSession(ctx, s, s.Version); err != nil {
			return err
		}
		from = s.State
		settled, err := c.settle(ctx, tx, s, now)
		if err != nil {
			return err
		}
		if settled {
			changed = s
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.forgetUnit(sessionID, ref)
	c.afterChange(ctx, changed, from)
	c.notifyWaiters(1)
	return nil
}

func (c *Core) forgetUnit(session string, ref wire.LeaseRef) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.renewals, ref.LeaseID)
	delete(c.inFlight[session], ref.UnitID)
}

// settle applies whatever lifecycle change the session's units now call for.
func (c *Core) settle(ctx context.Context, tx store.Tx, s *store.Session, now time.Time) (bool, error) {
	counts, err := tx.CountUnits(ctx, s.ID)
	if err != nil {
		return false, err
	}
	streams, err := tx.StreamStates(ctx, s.ID)
	if err != nil {
		return false, err
	}
	exhausted := true
	for i := range streams {
		exhausted = exhausted && streams[i].Exhausted
	}
	event := DrainEvent(s, counts[store.UnitLeased], counts[store.UnitPending], exhausted)
	if event == "" {
		return false, nil
	}
	if event == session.EvDrained && s.State == session.Pausing {
		s.PausedAt = &now
	}
	return session.Apply(ctx, tx, s, event, "coordinator", "", now)
}

// afterChange runs what must follow a committed lifecycle change, outside the transaction.
func (c *Core) afterChange(ctx context.Context, s *store.Session, from string) {
	if s == nil {
		return
	}
	c.SessionChanged(s, from)
	if session.IsTerminal(s.State) {
		if err := c.writeManifest(ctx, s); err != nil {
			c.logSession(s.ID, slog.LevelError, "write manifest", "err", err)
		}
		c.mu.Lock()
		delete(c.inFlight, s.ID)
		delete(c.live, s.ID)
		c.mu.Unlock()
	}
	c.Wake()
}

// Release implements wire.Coordinator.
func (c *Core) Release(ctx context.Context, worker string, ref wire.LeaseRef) error {
	r := lease.Ref{UnitID: ref.UnitID, LeaseID: ref.LeaseID, WorkerID: worker, FencingToken: ref.FencingToken}
	now := c.clk.Now()
	var sessionID, from string
	var changed *store.Session
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		changed = nil
		row, err := tx.GetUnit(ctx, ref.UnitID)
		if errors.Is(err, store.ErrNotFound) {
			return unknownUnit(ref.UnitID)
		}
		if err != nil {
			return err
		}
		released, err := lease.Release(*row, r, now)
		if err != nil {
			return fenced(err)
		}
		if err := tx.UpdateUnit(ctx, &released); err != nil {
			return err
		}
		s, err := tx.GetSession(ctx, row.SessionID)
		if err != nil {
			return err
		}
		sessionID, from = s.ID, s.State
		settled, err := c.settle(ctx, tx, s, now)
		if settled {
			changed = s
		}
		return err
	})
	if err != nil {
		return err
	}
	c.forgetUnit(sessionID, ref)
	c.afterChange(ctx, changed, from)
	c.notifyWaiters(1)
	return nil
}

// Fail implements wire.Coordinator.
func (c *Core) Fail(ctx context.Context, worker string, ref wire.LeaseRef, message string, retryable bool) error {
	r := lease.Ref{UnitID: ref.UnitID, LeaseID: ref.LeaseID, WorkerID: worker, FencingToken: ref.FencingToken}
	now := c.clk.Now()
	var sessionID, from string
	var changed *store.Session
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		changed = nil
		row, err := tx.GetUnit(ctx, ref.UnitID)
		if errors.Is(err, store.ErrNotFound) {
			return unknownUnit(ref.UnitID)
		}
		if err != nil {
			return err
		}
		updated, final, err := lease.FailAttempt(*row, r, message, retryable, c.cfg.Coordinator.MaxUnitAttempts, now)
		if err != nil {
			return fenced(err)
		}
		if err := tx.UpdateUnit(ctx, &updated); err != nil {
			return err
		}
		s, err := tx.GetSession(ctx, row.SessionID)
		if err != nil {
			return err
		}
		sessionID, from = s.ID, s.State
		s.ErrorCount++
		s.LastError = message
		if final {
			if err := session.WithdrawUnits(ctx, tx, s.ID, true); err != nil {
				return err
			}
			s.Finishing = true
			applied, err := session.Apply(ctx, tx, s, session.EvFail, "coordinator", "unit failed: "+message, now)
			if err != nil {
				return err
			}
			if applied {
				changed = s
			}
			return nil
		}
		return tx.UpdateSession(ctx, s, s.Version)
	})
	if err != nil {
		return err
	}
	c.forgetUnit(sessionID, ref)
	c.logSession(sessionID, slog.LevelWarn, "unit attempt failed", "unit_id", ref.UnitID, "worker_id", worker, "retryable", retryable, "err", message)
	c.afterChange(ctx, changed, from)
	c.notifyWaiters(1)
	return nil
}

// Deregister implements wire.Coordinator. Whatever the Worker still holds goes back.
func (c *Core) Deregister(ctx context.Context, worker string) error {
	var released int
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		released = 0
		held, err := tx.LeasedByWorker(ctx, worker)
		if err != nil {
			return err
		}
		for i := range held {
			u := lease.Revoke(held[i])
			if err := tx.UpdateUnit(ctx, &u); err != nil {
				return err
			}
			released++
		}
		w, err := tx.GetWorker(ctx, worker)
		if err != nil {
			return err
		}
		if w.State == store.WorkerRevoked {
			return nil
		}
		w.State = store.WorkerDisconnected
		return tx.PutWorker(ctx, w)
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.seen, worker)
	delete(c.caps, worker)
	c.orphans.Forget(worker)
	c.mu.Unlock()
	if released > 0 {
		c.notifyWaiters(released)
	}
	c.log.Info("worker deregistered", "worker_id", worker)
	return nil
}

func (c *Core) writeManifest(ctx context.Context, s *store.Session) error {
	var parts []store.Part
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		parts, err = tx.SessionParts(ctx, s.ID)
		return err
	})
	if err != nil {
		return err
	}
	return c.o.Artifacts.WriteManifest(s.ID, ManifestFor(s.Format, parts))
}

// ManifestFor renders committed parts as a manifest.
func ManifestFor(format string, parts []store.Part) artifact.Manifest {
	m := artifact.Manifest{Format: format, Compression: output.CompressionNone, Parts: make([]artifact.Part, len(parts))}
	for i := range parts {
		p := &parts[i]
		if strings.HasSuffix(p.RelPath, ".gz") {
			m.Compression = output.CompressionGzip
		}
		m.Records += p.Records
		m.Bytes += p.Bytes
		m.Parts[i] = ToArtifactPart(p)
	}
	artifact.SortManifest(&m)
	return m
}

// ToArtifactPart converts a row for the artifact store.
func ToArtifactPart(p *store.Part) artifact.Part {
	return artifact.Part{
		SessionID: p.SessionID, Stream: p.Stream, UnitID: p.UnitID, UnitSeq: int(p.UnitSeq), Seq: p.Seq, RelPath: p.RelPath,
		FirstIndex: p.FirstIndex, EndIndex: p.EndIndex, Records: p.Records, Bytes: p.Bytes, SHA256: p.SHA256,
		HeaderLen: p.HeaderLen, FooterLen: p.FooterLen, FencingToken: p.FencingToken,
	}
}

// Run does the Coordinator's periodic work until ctx is cancelled. A panic in one round
// is logged, and the next round goes ahead.
func (c *Core) Run(ctx context.Context) {
	ticker := c.clk.NewTicker(c.cfg.Coordinator.ReaperInterval)
	defer ticker.Stop()
	progress := c.clk.NewTicker(progressInterval)
	defer progress.Stop()
	gc := c.clk.NewTicker(gcInterval)
	defer gc.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			c.guard("tick", func() { c.tick(ctx) })
		case <-c.wake:
			c.guard("tick", func() { c.tick(ctx) })
		case <-progress.C():
			c.guard("progress", func() { c.flushProgress(ctx) })
		case <-gc.C():
			c.guard("gc", func() { c.collectGarbage(ctx) })
		}
	}
}

func (c *Core) guard(name string, fn func()) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("panic in background work", "work", name, "panic", p, "stack", string(debug.Stack()))
		}
	}()
	fn()
}

func (c *Core) tick(ctx context.Context) {
	steps := []struct {
		name string
		run  func(context.Context) error
	}{
		{"expire leases", c.expireLeases},
		{"track workers", c.trackWorkers},
		{"stop conditions", c.checkStopConditions},
		{"settle", c.settleAll},
		{"disk", c.checkDisk},
		{"admit", c.admit},
	}
	for _, step := range steps {
		if err := step.run(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("periodic step failed", "step", step.name, "err", err)
		}
	}
}

func (c *Core) expireLeases(ctx context.Context) error {
	now := c.clk.Now()
	d := c.cfg.Coordinator.LeaseDuration
	var units []store.Unit
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		units, err = tx.LeasedUnits(ctx)
		return err
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	var due []store.Unit
	for i := range units {
		last, ok := c.renewals[units[i].LeaseID]
		if lease.Expired(units[i], last, ok, now, d) {
			due = append(due, units[i])
		}
	}
	c.mu.Unlock()
	if len(due) == 0 {
		return nil
	}
	var expired []store.Unit
	err = c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		expired = nil
		for i := range due {
			// Re-read: a renewal may have won the race since the check above.
			row, err := tx.GetUnit(ctx, due[i].ID)
			if err != nil {
				return err
			}
			if row.State != store.UnitLeased || row.LeaseID != due[i].LeaseID || now.Before(row.LeaseExpiresAt) {
				continue
			}
			u := lease.Expire(*row)
			if err := tx.UpdateUnit(ctx, &u); err != nil {
				return err
			}
			expired = append(expired, due[i])
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := range expired {
		c.forgetUnit(expired[i].SessionID, wire.LeaseRef{UnitID: expired[i].ID, LeaseID: expired[i].LeaseID})
		c.logSession(expired[i].SessionID, slog.LevelWarn, "lease expired", "unit_id", expired[i].ID, "worker_id", expired[i].WorkerID)
	}
	c.notifyWaiters(len(expired))
	return nil
}

func (c *Core) trackWorkers(ctx context.Context) error {
	now := c.clk.Now()
	limit := disconnectAfter * c.cfg.Coordinator.HeartbeatInterval
	var quiet []string
	c.mu.Lock()
	for id, at := range c.seen {
		if id != LocalWorkerID && now.Sub(at) >= limit {
			quiet = append(quiet, id)
			delete(c.seen, id)
		}
	}
	c.mu.Unlock()
	if len(quiet) == 0 {
		return nil
	}
	return c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		for _, id := range quiet {
			w, err := tx.GetWorker(ctx, id)
			if err != nil {
				return err
			}
			if w.State == store.WorkerActive || w.State == store.WorkerDraining {
				w.State = store.WorkerDisconnected
				if err := tx.PutWorker(ctx, w); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (c *Core) checkStopConditions(ctx context.Context) error {
	now := c.clk.Now()
	var running []store.Session
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		running, err = tx.SessionsInStates(ctx, session.Running)
		return err
	})
	if err != nil {
		return err
	}
	for i := range running {
		s := &running[i]
		limits := ParseLimits(s)
		if !ShouldFinish(s, &limits, now) {
			continue
		}
		err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
			row, err := tx.GetSession(ctx, s.ID)
			if err != nil {
				return err
			}
			if !ShouldFinish(row, &limits, now) {
				return nil
			}
			row.Finishing = true
			if err := tx.UpdateSession(ctx, row, row.Version); err != nil {
				return err
			}
			return session.WithdrawUnits(ctx, tx, row.ID, false)
		})
		if err != nil {
			return err
		}
		c.logSession(s.ID, slog.LevelInfo, "stop condition reached")
	}
	return nil
}

func (c *Core) settleAll(ctx context.Context) error {
	now := c.clk.Now()
	var rows []store.Session
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		rows, err = tx.SessionsInStates(ctx, session.Running, session.Pausing, session.Stopping)
		return err
	})
	if err != nil {
		return err
	}
	for i := range rows {
		id := rows[i].ID
		var from string
		var changed *store.Session
		err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
			changed = nil
			s, err := tx.GetSession(ctx, id)
			if err != nil {
				return err
			}
			from = s.State
			settled, err := c.settle(ctx, tx, s, now)
			if settled {
				changed = s
			}
			return err
		})
		if err != nil {
			return err
		}
		c.afterChange(ctx, changed, from)
	}
	return nil
}

// checkDisk fails sessions that have been unable to store output for too long. Until then
// uploads just wait, which holds generation back.
func (c *Core) checkDisk(ctx context.Context) error {
	c.mu.Lock()
	since := c.lowDisk
	c.mu.Unlock()
	if since.IsZero() || c.clk.Now().Sub(since) < c.cfg.Storage.DiskFullTimeout {
		return nil
	}
	now := c.clk.Now()
	var rows []store.Session
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		rows, err = tx.SessionsInStates(ctx, session.Running)
		return err
	})
	if err != nil {
		return err
	}
	for i := range rows {
		id := rows[i].ID
		var changed *store.Session
		err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
			changed = nil
			s, err := tx.GetSession(ctx, id)
			if err != nil {
				return err
			}
			if s.State != session.Running {
				return nil
			}
			if err := session.WithdrawUnits(ctx, tx, id, true); err != nil {
				return err
			}
			s.LastError = "ERR_DISK_FULL: no room for output"
			applied, err := session.Apply(ctx, tx, s, session.EvFail, "coordinator", "ERR_DISK_FULL", now)
			if applied {
				changed = s
			}
			return err
		})
		if err != nil {
			return err
		}
		c.afterChange(ctx, changed, session.Running)
	}
	c.clearLowDisk()
	return nil
}

// admit starts queued sessions while there is room, oldest first.
func (c *Core) admit(ctx context.Context) error {
	var queued []store.Session
	var active int
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		queued, err = tx.SessionsInStates(ctx, session.Queued)
		if err != nil {
			return err
		}
		running, err := tx.SessionsInStates(ctx, session.Starting, session.Running, session.Pausing, session.Paused, session.Stopping)
		active = len(running)
		return err
	})
	if err != nil {
		return err
	}
	admitted := false
	for i := range queued {
		if active >= c.cfg.Generation.MaxConcurrentSessions {
			break
		}
		if err := c.startSession(ctx, &queued[i]); err != nil {
			return err
		}
		active++
		admitted = true
	}
	if admitted {
		c.notifyAllWaiters()
	}
	return nil
}

// startSession plans a session. Building its engine is too slow for a transaction, so
// STARTING records the intent, and a crash in between replans on restart.
func (c *Core) startSession(ctx context.Context, q *store.Session) error {
	now := c.clk.Now()
	var row *store.Session
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		s, err := tx.GetSession(ctx, q.ID)
		if err != nil {
			return err
		}
		if s.State != session.Queued {
			row = nil
			return nil
		}
		if _, err := session.Apply(ctx, tx, s, session.EvAdmit, "coordinator", "", now); err != nil {
			return err
		}
		row = s
		return nil
	})
	if err != nil || row == nil {
		return err
	}
	c.SessionChanged(row, session.Queued)

	streams, planErr := c.planStreams(ctx, row)

	var from = session.Starting
	var changed *store.Session
	err = c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		s, err := tx.GetSession(ctx, row.ID)
		if err != nil {
			return err
		}
		if s.State != session.Starting {
			return nil
		}
		if planErr != nil {
			s.LastError = planErr.Error()
			_, err := session.Apply(ctx, tx, s, session.EvFail, "coordinator", "planning failed: "+planErr.Error(), now)
			changed = s
			return err
		}
		for i := range streams {
			if err := tx.InsertStreamState(ctx, &streams[i]); err != nil {
				return err
			}
		}
		if s.StartedAt == nil {
			s.StartedAt = &now
		}
		if _, err := session.Apply(ctx, tx, s, session.EvPlanned, "coordinator", "", now); err != nil {
			return err
		}
		changed = s
		return nil
	})
	if err != nil {
		return err
	}
	c.afterChange(ctx, changed, from)
	return nil
}

func (c *Core) planStreams(ctx context.Context, s *store.Session) ([]store.StreamState, error) {
	var body []byte
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		body, err = tx.GetSnapshot(ctx, s.SnapshotHash)
		return err
	})
	if err != nil {
		return nil, err
	}
	snap, err := snapshot.Decode(body, s.SnapshotHash)
	if err != nil {
		return nil, err
	}
	compiled, err := snap.Compile(c.o.Vocab)
	if err != nil {
		return nil, err
	}
	return session.StreamStatesFor(s.ID, compiled.Engine.Streams()), nil
}

// flushProgress computes live rates, publishes progress and stores throughput history.
func (c *Core) flushProgress(ctx context.Context) {
	now := c.clk.Now()
	var rows []store.Session
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		rows, err = tx.SessionsInStates(ctx, session.Running, session.Pausing, session.Stopping)
		return err
	})
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error("read sessions", "err", err)
		}
		return
	}
	for i := range rows {
		s := &rows[i]
		c.mu.Lock()
		inFlight := c.inFlightLocked(s.ID)
		l := c.live[s.ID]
		if l == nil {
			l = &liveStats{lastCommitted: s.Records, lastBytes: s.Bytes, lastInFlight: inFlight, lastAt: now, bucket: now.Truncate(sampleInterval)}
			c.live[s.ID] = l
		}
		elapsed := now.Sub(l.lastAt).Seconds()
		total, lastTotal := s.Records+inFlight, l.lastCommitted+l.lastInFlight
		if elapsed > 0 && total >= lastTotal {
			l.rate = float64(total-lastTotal) / elapsed
		}
		rate := l.rate
		l.bucketRecords += s.Records - min(l.lastCommitted, s.Records)
		l.bucketBytes += s.Bytes - min(l.lastBytes, s.Bytes)
		l.lastCommitted, l.lastBytes, l.lastInFlight, l.lastAt = s.Records, s.Bytes, inFlight, now

		var flush *store.Sample
		if bucket := now.Truncate(sampleInterval); bucket.After(l.bucket) {
			flush = &store.Sample{SessionID: s.ID, Bucket: l.bucket, Records: l.bucketRecords, Bytes: l.bucketBytes}
			l.bucket, l.bucketRecords, l.bucketBytes = bucket, 0, 0
		}
		c.mu.Unlock()

		peak := rate > s.PeakRate
		if flush != nil || peak {
			id := s.ID
			err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
				if flush != nil {
					if err := tx.PutSample(ctx, *flush); err != nil {
						return err
					}
					if err := tx.TrimSamples(ctx, id, maxSamples); err != nil {
						return err
					}
				}
				if peak {
					row, err := tx.GetSession(ctx, id)
					if err != nil {
						return err
					}
					row.PeakRate = max(row.PeakRate, rate)
					return tx.UpdateSession(ctx, row, row.Version)
				}
				return nil
			})
			if err != nil && ctx.Err() == nil {
				c.log.Error("store progress", "session_id", id, "err", err)
			}
		}
		c.o.Hub.Publish(hub.Event{
			Type: hub.TypeProgress, SessionID: s.ID,
			Data: map[string]any{"session_id": s.ID, "records": s.Records, "bytes": s.Bytes, "in_flight": inFlight, "rate": rate},
		})
	}
}

func (c *Core) inFlightLocked(session string) uint64 {
	var total uint64
	for _, p := range c.inFlight[session] {
		total += p.Records
	}
	return total
}

// Live is what only memory knows about a session.
type Live struct {
	InFlight uint64
	Rate     float64
	// Pressure is time spent blocked, summed over running units, which names the bottleneck.
	GenBlocked, EncWait, SinkBlocked, RateWait time.Duration
}

// LiveStats reads a session's current numbers.
func (c *Core) LiveStats(session string) Live {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := Live{InFlight: c.inFlightLocked(session)}
	if l := c.live[session]; l != nil {
		out.Rate = l.rate
	}
	for _, p := range c.inFlight[session] {
		out.GenBlocked += time.Duration(p.GenBlockedNanos)
		out.EncWait += time.Duration(p.EncWaitNanos)
		out.SinkBlocked += time.Duration(p.SinkBlockedNanos)
		out.RateWait += time.Duration(p.RateWaitNanos)
	}
	return out
}

func (c *Core) collectGarbage(ctx context.Context) {
	now := c.clk.Now()
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		if _, err := tx.DeleteUnreferencedSnapshots(ctx, now.Add(-snapshotGCAge)); err != nil {
			return err
		}
		if err := tx.DeleteIdempotencyBefore(ctx, now.Add(-c.cfg.API.IdempotencyTTL)); err != nil {
			return err
		}
		return tx.DeleteAICacheBefore(ctx, now.Add(-aiCacheMaxAge))
	})
	if err != nil && ctx.Err() == nil {
		c.log.Error("collect garbage", "err", err)
	}
	if err := c.applyRetention(ctx, now); err != nil && ctx.Err() == nil {
		c.log.Error("apply retention", "err", err)
	}
}

// applyRetention purges the output of old finished sessions, keeping pinned ones.
func (c *Core) applyRetention(ctx context.Context, now time.Time) error {
	r := c.cfg.Storage.Retention
	if r.MaxAge <= 0 && r.MaxTotalBytes <= 0 {
		return nil
	}
	var rows []store.Session
	err := c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		rows, err = tx.SessionsInStates(ctx, session.Completed, session.Stopped, session.Failed)
		return err
	})
	if err != nil {
		return err
	}
	var total int64
	for i := range rows {
		total += rows[i].Bytes
	}
	for i := range rows { // oldest first, as IDs are time-ordered
		s := &rows[i]
		if s.Pinned || s.Bytes == 0 {
			continue
		}
		old := r.MaxAge > 0 && s.EndedAt != nil && now.Sub(*s.EndedAt) >= r.MaxAge
		over := r.MaxTotalBytes > 0 && total > int64(r.MaxTotalBytes)
		if !old && !over {
			continue
		}
		if err := c.PurgeArtifacts(ctx, s.ID); err != nil {
			return err
		}
		total -= s.Bytes
		c.logSession(s.ID, slog.LevelInfo, "output removed by retention")
	}
	return nil
}

// PurgeArtifacts deletes a session's output while keeping its history. Marking comes
// first, so an interruption is finished by the next startup.
func (c *Core) PurgeArtifacts(ctx context.Context, id string) error {
	var parts []store.Part
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		s, err := tx.GetSession(ctx, id)
		if err != nil {
			return err
		}
		if !session.IsTerminal(s.State) {
			return apperr.New(apperr.Conflict, session.CodeInvalidTransition, fmt.Sprintf("cannot purge the output of a session that is %s", s.State))
		}
		s.Purging = true
		if err := tx.UpdateSession(ctx, s, s.Version); err != nil {
			return err
		}
		parts, err = tx.SessionParts(ctx, id)
		return err
	})
	if err != nil {
		return err
	}
	if err := c.o.Artifacts.PurgeSession(id); err != nil {
		return err
	}
	return c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		for i := range parts {
			if err := tx.DeletePart(ctx, parts[i].UnitID, parts[i].Seq); err != nil {
				return err
			}
		}
		s, err := tx.GetSession(ctx, id)
		if err != nil {
			return err
		}
		s.Purging = false
		s.ArtifactsDegraded = true
		return tx.UpdateSession(ctx, s, s.Version)
	})
}

// DrainWorker stops giving a Worker new units. Those it has run to the end.
func (c *Core) DrainWorker(ctx context.Context, id string) error {
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		w, err := tx.GetWorker(ctx, id)
		if err != nil {
			return err
		}
		if w.State == store.WorkerRevoked {
			return apperr.New(apperr.Conflict, wire.CodeWorkerRevoked, "this Worker has been revoked")
		}
		w.State = store.WorkerDraining
		return tx.PutWorker(ctx, w)
	})
	if errors.Is(err, store.ErrNotFound) {
		return apperr.New(apperr.NotFound, session.CodeNotFound, "no worker "+id)
	}
	return err
}

// RevokeWorker cuts a Worker off for good. Its units go to others, and its token no longer
// fences only because of expiry: the lease change takes effect at once.
func (c *Core) RevokeWorker(ctx context.Context, id string) error {
	if id == LocalWorkerID {
		return apperr.New(apperr.InvalidArgument, "ERR_BAD_REQUEST", "the embedded worker cannot be revoked: set worker.local_slots to 0")
	}
	var released int
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		released = 0
		w, err := tx.GetWorker(ctx, id)
		if err != nil {
			return err
		}
		w.State = store.WorkerRevoked
		if err := tx.PutWorker(ctx, w); err != nil {
			return err
		}
		held, err := tx.LeasedByWorker(ctx, id)
		if err != nil {
			return err
		}
		for i := range held {
			u := lease.Revoke(held[i])
			if err := tx.UpdateUnit(ctx, &u); err != nil {
				return err
			}
			released++
		}
		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return apperr.New(apperr.NotFound, session.CodeNotFound, "no worker "+id)
	}
	if err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.seen, id)
	delete(c.caps, id)
	c.orphans.Forget(id)
	c.mu.Unlock()
	c.notifyWaiters(released)
	c.log.Warn("worker revoked", "worker_id", id, "units_released", released)
	return nil
}

// CreateJoinToken makes a token that lets new Workers register. Only its hash is kept.
func (c *Core) CreateJoinToken(ctx context.Context, maxUses int, ttl time.Duration) (id, secret string, expires *time.Time, err error) {
	id, secret = ids.New("jtk"), ids.Secret()
	now := c.clk.Now()
	row := &store.JoinToken{ID: id, Hash: HashSecret(secret), CreatedAt: now, MaxUses: maxUses}
	if ttl > 0 {
		at := now.Add(ttl)
		row.ExpiresAt, expires = &at, &at
	}
	err = c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error { return tx.InsertJoinToken(ctx, row) })
	return id, secret, expires, err
}

// BeginDrain stops handing out work and tells Workers to drain, ahead of shutdown.
func (c *Core) BeginDrain() {
	c.mu.Lock()
	c.draining = true
	c.mu.Unlock()
	c.notifyAllWaiters()
}
