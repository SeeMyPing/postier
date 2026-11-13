// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package queue wraps River, the Postgres-backed job queue.
//
// River is the transactional outbox: the injection job is inserted in the
// same transaction as the message, so "accepted" and "will be injected" are
// one atomic fact, without a broker.
package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// Queue names.
const (
	QueueInject      = "inject"
	QueueMaintenance = "maintenance"
)

// InjectArgs asks dispatch to hand one message to KumoMTA.
type InjectArgs struct {
	MessageID uuid.UUID `json:"message_id"`
}

// Kind implements river.JobArgs.
func (InjectArgs) Kind() string { return "postier.inject" }

// InsertOpts implements river.JobArgsWithInsertOpts.
//
// With River's default backoff (attempt^4 seconds), 20 attempts span about
// eight days: a message accepted while KumoMTA is down waits in Postgres
// instead of being lost, which is the point of the outbox.
func (InjectArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueInject, MaxAttempts: 20}
}

// EnsurePartitionsArgs asks for the weekly partitions to be created ahead.
type EnsurePartitionsArgs struct{}

// Kind implements river.JobArgs.
func (EnsurePartitionsArgs) Kind() string { return "postier.ensure_partitions" }

// InsertOpts implements river.JobArgsWithInsertOpts.
func (EnsurePartitionsArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueMaintenance, MaxAttempts: 5}
}

// PurgeIdempotencyArgs asks for expired idempotency keys to be deleted.
type PurgeIdempotencyArgs struct{}

// Kind implements river.JobArgs.
func (PurgeIdempotencyArgs) Kind() string { return "postier.purge_idempotency" }

// InsertOpts implements river.JobArgsWithInsertOpts.
func (PurgeIdempotencyArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: QueueMaintenance, MaxAttempts: 5}
}

// Inserter enqueues jobs without working any. It is what the entry points
// use, with the app pool.
type Inserter struct {
	client *river.Client[pgx.Tx]
}

// NewInserter returns an insert-only client.
func NewInserter(pool *pgxpool.Pool, log *slog.Logger) (*Inserter, error) {
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Logger: log})
	if err != nil {
		return nil, err
	}
	return &Inserter{client: client}, nil
}

// EnqueueInjections inserts one injection job per message, in tx.
func (i *Inserter) EnqueueInjections(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error {
	params := make([]river.InsertManyParams, len(ids))
	for n, id := range ids {
		params[n] = river.InsertManyParams{Args: InjectArgs{MessageID: id}}
	}
	_, err := i.client.InsertManyTx(ctx, tx, params)
	return err
}

// WorkerConfig describes a client that works jobs.
type WorkerConfig struct {
	Workers           *river.Workers
	InjectConcurrency int
	Logger            *slog.Logger
}

// NewWorkerClient returns a client that works the injection and maintenance
// queues, and schedules the periodic jobs. Periodic jobs are only enqueued by
// the elected leader among the dispatch replicas.
func NewWorkerClient(pool *pgxpool.Pool, cfg WorkerConfig) (*river.Client[pgx.Tx], error) {
	return river.NewClient(riverpgxv5.New(pool), &river.Config{
		Logger:  cfg.Logger,
		Workers: cfg.Workers,
		Queues: map[string]river.QueueConfig{
			QueueInject:      {MaxWorkers: cfg.InjectConcurrency},
			QueueMaintenance: {MaxWorkers: 1},
		},
		PeriodicJobs: []*river.PeriodicJob{
			river.NewPeriodicJob(
				river.PeriodicInterval(24*time.Hour),
				func() (river.JobArgs, *river.InsertOpts) { return EnsurePartitionsArgs{}, nil },
				&river.PeriodicJobOpts{RunOnStart: true},
			),
			river.NewPeriodicJob(
				river.PeriodicInterval(time.Hour),
				func() (river.JobArgs, *river.InsertOpts) { return PurgeIdempotencyArgs{}, nil },
				&river.PeriodicJobOpts{RunOnStart: true},
			),
		},
		// River waits this long between two fetches, and a fetch takes at
		// most MaxWorkers jobs: with the default of 100 ms, 16 workers top
		// out at 160 injections per second, whatever KumoMTA can take.
		// Measured on the test bench, see docs/validation-phase0.md.
		FetchCooldown: 25 * time.Millisecond,
		// A job running for longer than this lost its worker (the pod was
		// killed) and is handed to another one. It must exceed JobTimeout,
		// itself above the 60 s the injection client allows KumoMTA.
		RescueStuckJobsAfter: 2 * time.Minute,
		JobTimeout:           90 * time.Second,
	})
}
