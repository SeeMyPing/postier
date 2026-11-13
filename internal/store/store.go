// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package store is the only package that talks to Postgres.
//
// It exposes two pools that cannot be confused at compile time:
//
//   - AppPool connects as postier_app, the role subject to row-level
//     security. Its only way to reach tenant data is a TenantTx, which cannot
//     be built without a tenant id.
//   - SystemPool connects as postier_system, which bypasses row-level
//     security. It is for the workers, whose batches mix tenants.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// ErrNotFound is returned when a row does not exist, or is not visible to the
// caller's tenant: the two cases must be indistinguishable.
var ErrNotFound = errors.New("not found")

// Queries is the generated query set.
type Queries = sqlcgen.Queries

func open(ctx context.Context, url, appName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("database url: %w", err)
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = appName
	cfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database unreachable: %w", err)
	}
	return pool, nil
}

// AppPool is the pool of the entry points (api, smtp, tracking).
type AppPool struct{ pool *pgxpool.Pool }

// OpenApp connects with the postier_app role.
func OpenApp(ctx context.Context, url, role string) (*AppPool, error) {
	pool, err := open(ctx, url, "postier-"+role)
	if err != nil {
		return nil, err
	}
	return &AppPool{pool: pool}, nil
}

// Close releases the connections.
func (p *AppPool) Close() { p.pool.Close() }

// Ping is the readiness check.
func (p *AppPool) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

// Raw returns the underlying pool, for the insert-only job queue client.
func (p *AppPool) Raw() *pgxpool.Pool { return p.pool }

// Credential is what authentication needs to know about an API key.
type Credential struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	ProjectID  uuid.UUID
	SecretHash []byte
	Scopes     []string
	Revoked    bool
}

// CredentialByPrefix is the one query that runs before the tenant is known:
// it is what establishes it.
func (p *AppPool) CredentialByPrefix(ctx context.Context, prefix string) (Credential, error) {
	row, err := sqlcgen.New(p.pool).GetCredentialByPrefix(ctx, prefix)
	if errors.Is(err, pgx.ErrNoRows) {
		return Credential{}, ErrNotFound
	}
	if err != nil {
		return Credential{}, err
	}
	return Credential{
		ID: row.ID, TenantID: row.TenantID, ProjectID: row.ProjectID,
		SecretHash: row.SecretHash, Scopes: row.Scopes, Revoked: row.RevokedAt != nil,
	}, nil
}

// TenantTx is a transaction scoped to one tenant. The zero value is unusable
// and there is no constructor other than AppPool.InTenantTx: code that has no
// tenant id cannot reach tenant data through the app pool.
type TenantTx struct {
	tx       pgx.Tx
	tenantID uuid.UUID
	q        *sqlcgen.Queries
}

// TenantID returns the tenant the transaction is scoped to.
func (t TenantTx) TenantID() uuid.UUID { return t.tenantID }

// Q returns the queries, bound to the transaction.
func (t TenantTx) Q() *Queries { return t.q }

// Tx returns the transaction, for the job queue's transactional insert.
func (t TenantTx) Tx() pgx.Tx { return t.tx }

// InTenantTx runs fn in a transaction where postier.tenant_id is set. The
// row-level security policies filter on that setting; it is transaction-local
// (SET LOCAL semantics), so it cannot leak to the next user of the connection,
// even behind PgBouncer in transaction mode.
func (p *AppPool) InTenantTx(ctx context.Context, tenantID uuid.UUID, fn func(TenantTx) error) error {
	if tenantID == uuid.Nil {
		return errors.New("store: InTenantTx without a tenant id")
	}
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('postier.tenant_id', $1, true)", tenantID.String()); err != nil {
			return err
		}
		return fn(TenantTx{tx: tx, tenantID: tenantID, q: sqlcgen.New(tx)})
	})
}

// SystemPool is the pool of the workers (dispatch, ingest, webhooks).
type SystemPool struct{ pool *pgxpool.Pool }

// OpenSystem connects with the postier_system role.
func OpenSystem(ctx context.Context, url, role string) (*SystemPool, error) {
	pool, err := open(ctx, url, "postier-"+role)
	if err != nil {
		return nil, err
	}
	return &SystemPool{pool: pool}, nil
}

// Close releases the connections.
func (p *SystemPool) Close() { p.pool.Close() }

// Ping is the readiness check.
func (p *SystemPool) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

// Raw returns the underlying pool, for the job queue client.
func (p *SystemPool) Raw() *pgxpool.Pool { return p.pool }

// Q returns the queries, outside of any transaction.
func (p *SystemPool) Q() *Queries { return sqlcgen.New(p.pool) }

// InTx runs fn in a transaction that sees every tenant.
func (p *SystemPool) InTx(ctx context.Context, fn func(q *Queries, tx pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		return fn(sqlcgen.New(tx), tx)
	})
}

// NotFound maps pgx's "no rows" to ErrNotFound and passes anything else on.
func NotFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// WrapApp builds an AppPool around an existing pool. It exists for tests,
// which connect as a superuser and switch role per connection.
func WrapApp(pool *pgxpool.Pool) *AppPool { return &AppPool{pool: pool} }

// WrapSystem is WrapApp for the system pool.
func WrapSystem(pool *pgxpool.Pool) *SystemPool { return &SystemPool{pool: pool} }
