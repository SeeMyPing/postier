// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package dispatch moves accepted messages from Postgres to KumoMTA.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/kumo"
	"github.com/SeeMyPing/postier/internal/queue"
	"github.com/SeeMyPing/postier/internal/render"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// Injector hands a message to the MTA. Implemented by kumo.Client.
type Injector interface {
	Inject(ctx context.Context, in kumo.Injection) error
}

// DefaultGuardDelay is how long a retry waits for the log record of a
// previous attempt before injecting again. It must exceed the usual latency
// of the log hook (KumoMTA to ingest to Postgres), which is well under a
// second on a healthy system.
const DefaultGuardDelay = 15 * time.Second

// InjectWorker injects one message per job.
//
// Delivery is at least once. KumoMTA has no idempotent injection, so a crash
// between "KumoMTA accepted the message" and "the job is marked done" leaves
// a job that will run again for a message that is already on its way. The
// guard in Work closes that window in the common case; what remains is
// documented in docs/validation-phase0.md (decision D4).
type InjectWorker struct {
	river.WorkerDefaults[queue.InjectArgs]

	Pool       *store.SystemPool
	Kumo       Injector
	Signer     *domain.Signer
	Log        *slog.Logger
	GuardDelay time.Duration
	Now        func() time.Time
}

func (w *InjectWorker) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

// Work implements river.Worker.
func (w *InjectWorker) Work(ctx context.Context, job *river.Job[queue.InjectArgs]) error {
	id := job.Args.MessageID
	log := w.Log.With("message_id", id, "attempt", job.Attempt)

	createdAt, err := domain.IDTime(id)
	if err != nil {
		return river.JobCancel(fmt.Errorf("message id %s: %w", id, err))
	}
	q := w.Pool.Q()
	msg, err := q.GetMessageForDispatch(ctx, sqlcgen.GetMessageForDispatchParams{ID: id, CreatedAt: createdAt})
	if errors.Is(err, pgx.ErrNoRows) {
		return river.JobCancel(fmt.Errorf("message %s does not exist", id))
	}
	if err != nil {
		return err
	}
	log = log.With("tenant_id", msg.TenantID, "trace_id", deref(msg.TraceID))

	// Re-check, right before injecting: time has passed since acceptance.
	//
	// The status first. Anything but accepted means there is nothing left to
	// do: the message was cancelled, or a previous attempt did inject it and
	// its Reception record has already moved it to queued.
	if domain.Status(msg.Status) != domain.StatusAccepted {
		log.Info("injection skipped", "status", msg.Status)
		return nil
	}
	// Then the domain: it may have lost its verification since.
	if msg.DomainVerifiedAt == nil {
		return w.reject(ctx, id, createdAt, msg.TenantID, "sending domain is no longer verified", log)
	}
	if msg.ContentMime == nil || msg.ContentEncoding == nil {
		return w.reject(ctx, id, createdAt, msg.TenantID, "message body is no longer available", log)
	}

	// The guard against sending twice. On a retry, the previous attempt may
	// have reached KumoMTA even though it looked like a failure (a timeout
	// after the message was spooled, a crash before the job was completed).
	if job.Attempt > 1 {
		seen, err := q.HasDeliveryEvent(ctx, sqlcgen.HasDeliveryEventParams{
			MessageID: id, Since: createdAt.Add(-time.Hour),
		})
		if err != nil {
			return err
		}
		if seen {
			log.Info("injection skipped: a previous attempt went through")
			return nil
		}
		// No record yet. If the previous attempt is recent, its record may
		// still be on its way: wait for it rather than risk a duplicate.
		if wait := w.guardWait(job); wait > 0 {
			return river.JobSnooze(wait)
		}
	}

	mime, err := store.DecodeContent(*msg.ContentEncoding, msg.ContentMime)
	if err != nil {
		return w.reject(ctx, id, createdAt, msg.TenantID, "message body is unreadable", log)
	}
	err = w.Kumo.Inject(ctx, kumo.Injection{
		EnvelopeSender: w.Signer.ReturnPath(id, msg.ReturnPathDomain),
		Recipient:      msg.RcptAddr,
		Content:        render.WithMessageID(mime, w.Signer.MessageID(id, msg.SendingDomain)),
		PostierID:      id.String(),
		Tenant:         msg.TenantID.String(),
		Trace:          deref(msg.TraceID),
	})
	switch {
	case err == nil:
		// The status stays accepted: it becomes queued when the Reception
		// record comes back, which is also what the guard above looks for.
		log.Info("message injected")
		return nil
	case errors.Is(err, kumo.ErrRejected):
		return w.reject(ctx, id, createdAt, msg.TenantID, err.Error(), log)
	case job.Attempt >= job.MaxAttempts:
		// Out of retries: say so, rather than leave the message accepted
		// forever with nobody working on it.
		log.Error("injection abandoned", "err", err)
		return w.reject(ctx, id, createdAt, msg.TenantID, "injection failed repeatedly: "+err.Error(), log)
	default:
		return err
	}
}

// guardWait returns how long to wait before a retry may inject again.
func (w *InjectWorker) guardWait(job *river.Job[queue.InjectArgs]) time.Duration {
	if len(job.Errors) == 0 {
		return 0
	}
	last := job.Errors[len(job.Errors)-1].At
	delay := w.GuardDelay
	if delay <= 0 {
		delay = DefaultGuardDelay
	}
	return delay - w.now().Sub(last)
}

// reject closes a message that will never be injected. It always returns nil:
// the job is done, even though the message is not sent.
func (w *InjectWorker) reject(ctx context.Context, id uuid.UUID, createdAt time.Time, tenantID uuid.UUID, reason string, log *slog.Logger) error {
	err := w.Pool.InTx(ctx, func(q *store.Queries, _ pgx.Tx) error {
		_, err := store.AppendEvent(ctx, q, store.NewEvent{
			MessageID: id, CreatedAt: createdAt, TenantID: tenantID,
			Type:       domain.EventRejected,
			OccurredAt: w.now().UTC().Truncate(time.Microsecond),
			// One rejection per message, however many times this runs.
			DedupKey: "rejected:" + id.String(),
			Detail:   map[string]any{"response": reason},
		})
		return err
	})
	if err != nil {
		return err
	}
	log.Warn("message rejected at injection", "reason", reason)
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// EnsurePartitionsWorker creates the weekly partitions ahead of time.
type EnsurePartitionsWorker struct {
	river.WorkerDefaults[queue.EnsurePartitionsArgs]
	Pool *store.SystemPool
	Log  *slog.Logger
}

// Work implements river.Worker.
func (w *EnsurePartitionsWorker) Work(ctx context.Context, _ *river.Job[queue.EnsurePartitionsArgs]) error {
	created, err := w.Pool.Q().EnsurePartitions(ctx, store.PartitionWeeksAhead)
	if err != nil {
		return err
	}
	if created > 0 {
		w.Log.Info("partitions created", "count", created)
	}
	return nil
}

// PurgeIdempotencyWorker deletes expired idempotency keys.
type PurgeIdempotencyWorker struct {
	river.WorkerDefaults[queue.PurgeIdempotencyArgs]
	Pool *store.SystemPool
	Log  *slog.Logger
}

// Work implements river.Worker.
func (w *PurgeIdempotencyWorker) Work(ctx context.Context, _ *river.Job[queue.PurgeIdempotencyArgs]) error {
	deleted, err := w.Pool.Q().DeleteExpiredIdempotencyKeys(ctx)
	if err != nil {
		return err
	}
	if deleted > 0 {
		w.Log.Info("idempotency keys purged", "count", deleted)
	}
	return nil
}

// Workers registers every worker of the dispatch role.
func Workers(inject *InjectWorker) *river.Workers {
	workers := river.NewWorkers()
	river.AddWorker(workers, inject)
	river.AddWorker(workers, &EnsurePartitionsWorker{Pool: inject.Pool, Log: inject.Log})
	river.AddWorker(workers, &PurgeIdempotencyWorker{Pool: inject.Pool, Log: inject.Log})
	return workers
}
