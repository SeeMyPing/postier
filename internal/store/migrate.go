// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/SeeMyPing/postier/db"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// PartitionWeeksAhead is how many weekly partitions exist in advance. The
// periodic job runs daily, so there is a month of slack if it stops.
const PartitionWeeksAhead = 4

// Migrate brings the database to the schema this binary expects. It runs as
// the owner of the schema, not as one of the two runtime roles:
//
//  1. Postier's own migrations (goose);
//  2. the job queue's tables (River has its own migrator);
//  3. the weekly partitions;
//  4. the grants to postier_app and postier_system.
//
// Every step is idempotent: running it again is a no-op.
func Migrate(ctx context.Context, url string, log *slog.Logger) error {
	pool, err := open(ctx, url, "postier-migrate")
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := migrateGoose(ctx, pool, log); err != nil {
		return fmt.Errorf("schema migrations: %w", err)
	}

	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("job queue migrations: %w", err)
	}
	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return fmt.Errorf("job queue migrations: %w", err)
	}
	for _, v := range res.Versions {
		log.Info("job queue migration applied", "version", v.Version)
	}

	created, err := sqlcgen.New(pool).EnsurePartitions(ctx, PartitionWeeksAhead)
	if err != nil {
		return fmt.Errorf("partitions: %w", err)
	}
	log.Info("partitions ensured", "created", created)

	if _, err := pool.Exec(ctx, db.Grants); err != nil {
		return fmt.Errorf("grants: %w", err)
	}
	log.Info("grants applied")
	return nil
}

func migrateGoose(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	migrations, err := fs.Sub(db.Migrations, "migrations")
	if err != nil {
		return err
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	defer func() { _ = sqlDB.Close() }()

	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations)
	if err != nil {
		return err
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return err
	}
	for _, r := range results {
		log.Info("schema migration applied", "version", r.Source.Version, "duration", r.Duration.String())
	}
	return nil
}
