// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SeeMyPing/postier/db"
)

// SchemaVersion is the version of the newest migration embedded in this
// binary: the schema it needs to run.
func SchemaVersion() (int64, error) {
	entries, err := fs.ReadDir(db.Migrations, "migrations")
	if err != nil {
		return 0, err
	}
	var latest int64
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			continue
		}
		v, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("migration %s: %w", e.Name(), err)
		}
		latest = max(latest, v)
	}
	if latest == 0 {
		return 0, fmt.Errorf("no migration embedded")
	}
	return latest, nil
}

// WaitReady blocks until the database is reachable and migrated at least up
// to what this binary needs, or until ctx ends. It is the init container of
// every role: a role that starts before `postier migrate` has finished would
// crash and restart instead.
//
// "Migrated" means the last step of Migrate is done: the grants. They are
// checked through the role of the caller, on a table of the job queue, which
// is created after Postier's own tables.
func WaitReady(ctx context.Context, url string, log *slog.Logger) error {
	want, err := SchemaVersion()
	if err != nil {
		return err
	}
	var last error
	for {
		last = checkReady(ctx, url, want)
		if last == nil {
			return nil
		}
		log.Info("database not ready yet", "reason", last.Error())
		select {
		case <-ctx.Done():
			return fmt.Errorf("database not ready: %w", last)
		case <-time.After(2 * time.Second):
		}
	}
}

func checkReady(ctx context.Context, url string, want int64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return fmt.Errorf("unreachable: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	var got int64
	err = conn.QueryRow(ctx,
		"SELECT coalesce(max(version_id), 0) FROM goose_db_version WHERE is_applied").Scan(&got)
	if err != nil {
		return fmt.Errorf("not migrated: %w", err)
	}
	if got < want {
		return fmt.Errorf("schema at version %d, this binary needs %d", got, want)
	}
	var granted bool
	err = conn.QueryRow(ctx,
		"SELECT has_table_privilege(current_user, 'river_job', 'INSERT')").Scan(&granted)
	if err != nil {
		return fmt.Errorf("job queue not migrated: %w", err)
	}
	if !granted {
		return fmt.Errorf("grants not applied yet")
	}
	return nil
}
