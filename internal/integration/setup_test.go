// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/queue"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

const envURL = "POSTIER_TEST_DATABASE_URL"

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// env is a migrated database of its own, with the two pools of production.
type env struct {
	admin  *pgxpool.Pool     // superuser, for assertions and fixtures
	app    *store.AppPool    // runs as postier_app
	system *store.SystemPool // runs as postier_system
	jobs   *queue.Inserter

	tenantID  uuid.UUID
	projectID uuid.UUID
	principal accept.Principal
}

// newEnv creates a fresh database, migrates it and seeds one tenant with the
// verified sending domain acme.test.
//
// The app and system pools connect as the superuser and SET ROLE on every
// connection. The tests therefore run with exactly the privileges the two
// roles have in production: a missing GRANT fails a test.
func newEnv(t *testing.T) *env {
	t.Helper()
	base := os.Getenv(envURL)
	if base == "" {
		t.Skipf("%s is not set: integration test skipped", envURL)
	}
	ctx := context.Background()

	root, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connecting to %s: %v", envURL, err)
	}
	name := "postier_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := root.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = root.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = root.Close(context.Background())
	})

	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	dbURL := u.String()

	if err := store.Migrate(ctx, dbURL, quiet); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	e := &env{
		admin:  openAs(t, dbURL, ""),
		app:    store.WrapApp(openAs(t, dbURL, "postier_app")),
		system: store.WrapSystem(openAs(t, dbURL, "postier_system")),
	}
	if e.jobs, err = queue.NewInserter(e.app.Raw(), quiet); err != nil {
		t.Fatal(err)
	}

	q := sqlcgen.New(e.admin)
	if e.tenantID, err = q.UpsertTenant(ctx, sqlcgen.UpsertTenantParams{ID: domain.NewID(), Name: "acme"}); err != nil {
		t.Fatal(err)
	}
	if e.projectID, err = q.UpsertProject(ctx, sqlcgen.UpsertProjectParams{ID: domain.NewID(), TenantID: e.tenantID, Name: "default"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	e.addDomain(t, "acme.test", &now)
	e.principal = accept.Principal{
		CredentialID: domain.NewID(), TenantID: e.tenantID, ProjectID: e.projectID,
		Scopes: []string{accept.ScopeSend, accept.ScopeRead},
	}
	return e
}

func openAs(t *testing.T, dbURL, role string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 20
	if role != "" {
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, "SET ROLE "+role)
			return err
		}
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func (e *env) addDomain(t *testing.T, name string, verifiedAt *time.Time) uuid.UUID {
	t.Helper()
	id, err := sqlcgen.New(e.admin).UpsertSendingDomain(context.Background(), sqlcgen.UpsertSendingDomainParams{
		ID: domain.NewID(), TenantID: e.tenantID, ProjectID: e.projectID, Domain: name,
		ReturnPathDomain: "bounce." + name, DkimSelector: "pst1", VerifiedAt: verifiedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *env) service() *accept.Service {
	return accept.NewService(e.app, e.jobs, accept.DefaultLimits(), quiet)
}

// count runs a "SELECT count(*) …" query.
func (e *env) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (e *env) status(t *testing.T, id uuid.UUID) (status domain.Status, attempts int) {
	t.Helper()
	err := e.admin.QueryRow(context.Background(),
		"SELECT status, attempts FROM messages WHERE id = $1", id).Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("message %s: %v", id, err)
	}
	return status, attempts
}

func (e *env) eventTypes(t *testing.T, id uuid.UUID) string {
	t.Helper()
	rows, err := e.admin.Query(context.Background(),
		"SELECT type FROM events WHERE message_id = $1 ORDER BY occurred_at, id", id)
	if err != nil {
		t.Fatal(err)
	}
	types, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(types, ",")
}

// ensureWeek creates the partitions holding t, for tests that replay records
// captured in the past. Production only ever creates partitions ahead.
func (e *env) ensureWeek(t *testing.T, at time.Time) {
	t.Helper()
	ctx := context.Background()
	for _, parent := range []string{"messages", "events"} {
		var from, to time.Time
		if err := e.admin.QueryRow(ctx,
			"SELECT date_trunc('week', $1::timestamptz, 'UTC'), date_trunc('week', $1::timestamptz, 'UTC') + interval '7 days'",
			at).Scan(&from, &to); err != nil {
			t.Fatal(err)
		}
		part := fmt.Sprintf("%s_test_%s", parent, from.UTC().Format("20060102"))
		_, err := e.admin.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
			part, parent, from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339)))
		if err != nil && !strings.Contains(err.Error(), "would overlap") {
			t.Fatal(err)
		}
	}
}

func validRequest(to ...string) accept.Request {
	return accept.Request{
		From:     "Acme <no-reply@acme.test>",
		To:       to,
		Subject:  "Votre commande 48213",
		HTML:     "<p>Merci</p>",
		Text:     "Merci",
		Tags:     []string{"order"},
		Metadata: map[string]string{"order_id": "48213"},
	}
}

func apiOpts(key string) accept.Options {
	return accept.Options{Source: accept.SourceAPI, IdempotencyKey: key, TraceID: "trace-test"}
}
