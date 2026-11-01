package coord

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"log/slog"

	"github.com/genadigeno/data-feed/internal/artifact"
	"github.com/genadigeno/data-feed/internal/coord/lease"
	"github.com/genadigeno/data-feed/internal/session"
	"github.com/genadigeno/data-feed/internal/store"
)

func newHash() hash.Hash { return sha256.New() }

// Recover brings persisted state back in line after a restart, before anything is served.
// See docs/01-architecture/recovery-design.md.
func (c *Core) Recover(ctx context.Context) error {
	if err := c.reconcileArtifacts(ctx); err != nil {
		return fmt.Errorf("reconcile artifacts: %w", err)
	}
	if err := c.recoverLeases(ctx); err != nil {
		return fmt.Errorf("recover leases: %w", err)
	}
	if err := c.recoverSessions(ctx); err != nil {
		return fmt.Errorf("recover sessions: %w", err)
	}
	return nil
}

// reconcileArtifacts aligns files and rows. Nothing can be mid-upload at this point.
func (c *Core) reconcileArtifacts(ctx context.Context) error {
	files, err := c.o.Artifacts.Scan()
	if err != nil {
		return err
	}
	var rows []store.Part
	err = c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		rows, err = tx.AllParts(ctx)
		return err
	})
	if err != nil {
		return err
	}
	parts := make([]artifact.Part, len(rows))
	for i := range rows {
		parts[i] = ToArtifactPart(&rows[i])
	}
	actions := artifact.PlanReconcile(files, parts)

	degraded := map[string]bool{}
	bySeq := map[PartKey]store.Part{}
	for i := range rows {
		bySeq[PartKey{rows[i].UnitID, rows[i].Seq}] = rows[i]
	}
	err = c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		clear(degraded)
		for _, a := range actions {
			switch a.Kind {
			case artifact.ActionDropPart:
				part, ok := bySeq[PartKey{a.UnitID, a.Seq}]
				if !ok {
					continue
				}
				if err := tx.DeletePart(ctx, a.UnitID, a.Seq); err != nil {
					return err
				}
				s, err := tx.GetSession(ctx, part.SessionID)
				if err != nil {
					return err
				}
				s.Records -= min(part.Records, s.Records)
				s.Bytes -= min(part.Bytes, s.Bytes)
				if session.IsTerminal(s.State) {
					s.ArtifactsDegraded = true
					degraded[s.ID] = true
				}
				if err := tx.UpdateSession(ctx, s, s.Version); err != nil {
					return err
				}
			case artifact.ActionRollbackUnit:
				unit, err := tx.GetUnit(ctx, a.UnitID)
				if err != nil {
					return err
				}
				records, bytes := CountBefore(rows, a.UnitID, a.NextSeq)
				rolled := lease.Rollback(*unit, a.To, a.NextSeq, records, bytes)
				if err := tx.UpdateUnit(ctx, &rolled); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := c.o.Artifacts.ApplyFiles(actions); err != nil {
		return err
	}
	for id := range degraded {
		c.logSession(id, slog.LevelWarn, "output files were found missing or damaged; regenerate with a retry")
	}
	if len(actions) > 0 {
		c.log.Warn("reconciled artifacts after restart", "actions", len(actions))
	}
	return nil
}

// recoverLeases gives surviving Workers time to reconnect. Their lease IDs and tokens are
// unchanged, so one back within the grace period simply carries on. The embedded worker's
// memory is gone, so its leases end now.
func (c *Core) recoverLeases(ctx context.Context) error {
	now := c.clk.Now()
	grace := now.Add(c.cfg.Coordinator.RestartGrace)
	return c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		workers, err := tx.ListWorkers(ctx)
		if err != nil {
			return err
		}
		for i := range workers {
			if workers[i].State == store.WorkerRevoked {
				continue
			}
			workers[i].State = store.WorkerDisconnected
			if err := tx.PutWorker(ctx, &workers[i]); err != nil {
				return err
			}
		}
		units, err := tx.LeasedUnits(ctx)
		if err != nil {
			return err
		}
		for i := range units {
			u := units[i]
			if u.WorkerID == LocalWorkerID {
				u = lease.Revoke(u)
			} else if u.LeaseExpiresAt.Before(grace) {
				u.LeaseExpiresAt = grace
			}
			if err := tx.UpdateUnit(ctx, &u); err != nil {
				return err
			}
		}
		return nil
	})
}

// recoverSessions puts sessions caught mid-change back on track, and redoes the parts of
// finishing that happen after the commit.
func (c *Core) recoverSessions(ctx context.Context) error {
	now := c.clk.Now()
	err := c.o.DB.Write(ctx, func(ctx context.Context, tx store.Tx) error {
		starting, err := tx.SessionsInStates(ctx, session.Starting)
		if err != nil {
			return err
		}
		for i := range starting {
			if _, err := session.Apply(ctx, tx, &starting[i], session.EvRequeue, "coordinator", "restarted while starting", now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	var terminal []store.Session
	err = c.o.DB.Read(ctx, func(ctx context.Context, tx store.Tx) error {
		var err error
		terminal, err = tx.SessionsInStates(ctx, session.Completed, session.Stopped, session.Failed)
		return err
	})
	if err != nil {
		return err
	}
	for i := range terminal {
		s := &terminal[i]
		if s.Purging {
			if err := c.PurgeArtifacts(ctx, s.ID); err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
			continue
		}
		if s.Bytes > 0 {
			if err := c.writeManifest(ctx, s); err != nil {
				c.log.Error("write manifest", "session_id", s.ID, "err", err)
			}
		}
	}
	return nil
}
