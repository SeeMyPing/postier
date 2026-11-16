// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package suppression manages the suppression list on behalf of API clients:
// consult it, add an address by hand, lift a suppression. The automatic
// side, fed by bounces and complaints, lives in ingest.
package suppression

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// Scopes of a suppression.
const (
	ScopeProject = "project"
	ScopeTenant  = "tenant"
)

// Page sizes of List.
const (
	DefaultLimit = 50
	MaxLimit     = 200
)

// Suppression is an entry of the list.
type Suppression struct {
	ID uuid.UUID
	// Address is empty once the recipient asked for erasure: the entry then
	// only holds a hash, and still applies.
	Address   string
	Reason    domain.SuppressionReason
	Scope     string
	ExpiresAt *time.Time
	CreatedAt time.Time
}

// Service implements the suppression endpoints.
type Service struct {
	pool *store.AppPool
}

// NewService returns the service.
func NewService(pool *store.AppPool) *Service { return &Service{pool: pool} }

func authorize(p accept.Principal) error {
	if !p.Can(accept.ScopeSuppressions) {
		return domain.Errorf(domain.CodeForbidden, "this API key cannot manage suppressions")
	}
	return nil
}

func scopeOf(projectID *uuid.UUID) string {
	if projectID == nil {
		return ScopeTenant
	}
	return ScopeProject
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// List returns the suppressions that apply to the caller's project, newest
// first: its own and those of the whole tenant. address, when not empty,
// narrows the list to one address. The returned cursor is nil on the last
// page.
func (s *Service) List(ctx context.Context, p accept.Principal, address string, cursor *uuid.UUID, limit int) ([]Suppression, *uuid.UUID, error) {
	if err := authorize(p); err != nil {
		return nil, nil, err
	}
	if limit <= 0 {
		limit = DefaultLimit
	}
	if limit > MaxLimit {
		return nil, nil, domain.FieldErrorf("limit", "at most %d", MaxLimit)
	}
	var hash []byte
	if address != "" {
		a, err := domain.ParseAddress(address)
		if err != nil {
			return nil, nil, domain.FieldErrorf("address", "%v", err)
		}
		h := a.Hash()
		hash = h[:]
	}

	var out []Suppression
	err := s.pool.InTenantTx(ctx, p.TenantID, func(tx store.TenantTx) error {
		rows, err := tx.Q().ListSuppressions(ctx, sqlcgen.ListSuppressionsParams{
			TenantID: p.TenantID, ProjectID: &p.ProjectID, AddrHash: hash,
			BeforeID: cursor,
			MaxRows:  int32(limit) + 1, //nolint:gosec // bounded by MaxLimit
		})
		if err != nil {
			return err
		}
		for _, r := range rows {
			out = append(out, Suppression{
				ID: r.ID, Address: deref(r.Addr), Reason: domain.SuppressionReason(r.Reason),
				Scope: scopeOf(r.ProjectID), ExpiresAt: r.ExpiresAt, CreatedAt: r.CreatedAt,
			})
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	// One row more than asked was fetched: it only tells whether a next
	// page exists.
	var next *uuid.UUID
	if len(out) > limit {
		out = out[:limit]
		next = &out[limit-1].ID
	}
	return out, next, nil
}

// Add suppresses an address by hand. It is idempotent: adding an address
// that is already suppressed returns the existing entry, and never weakens
// it. The second result is false in that case.
func (s *Service) Add(ctx context.Context, p accept.Principal, address, scope string, expiresAt *time.Time) (Suppression, bool, error) {
	if err := authorize(p); err != nil {
		return Suppression{}, false, err
	}
	addr, err := domain.ParseAddress(address)
	if err != nil {
		return Suppression{}, false, domain.FieldErrorf("address", "%v", err)
	}
	projectID := &p.ProjectID
	switch scope {
	case "", ScopeProject:
	case ScopeTenant:
		// A project key acting on every project of the tenant.
		if !p.Can(accept.ScopeAdmin) {
			return Suppression{}, false, domain.Errorf(domain.CodeForbidden,
				"a tenant-wide suppression needs an API key with the admin scope")
		}
		projectID = nil
	default:
		return Suppression{}, false, domain.FieldErrorf("scope", "must be %q or %q", ScopeProject, ScopeTenant)
	}
	if expiresAt != nil && !expiresAt.After(time.Now()) {
		return Suppression{}, false, domain.FieldErrorf("expires_at", "must be in the future")
	}

	var (
		out     Suppression
		created bool
	)
	err = s.pool.InTenantTx(ctx, p.TenantID, func(tx store.TenantTx) error {
		hash := addr.Hash()
		text := addr.String()
		row, err := tx.Q().UpsertSuppression(ctx, sqlcgen.UpsertSuppressionParams{
			ID: domain.NewID(), TenantID: p.TenantID, ProjectID: projectID,
			AddrHash: hash[:], Addr: &text, Reason: string(domain.ReasonManual), ExpiresAt: expiresAt,
		})
		if err != nil {
			return err
		}
		created = row.Created
		out = Suppression{
			ID: row.ID, Address: deref(row.Addr), Reason: domain.SuppressionReason(row.Reason),
			Scope: scopeOf(row.ProjectID), ExpiresAt: row.ExpiresAt, CreatedAt: row.CreatedAt,
		}
		return nil
	})
	return out, created, err
}

// Remove lifts a suppression. The address can be sent to again at once.
func (s *Service) Remove(ctx context.Context, p accept.Principal, id uuid.UUID) error {
	if err := authorize(p); err != nil {
		return err
	}
	notFound := domain.Errorf(domain.CodeNotFound, "no such suppression")
	return s.pool.InTenantTx(ctx, p.TenantID, func(tx store.TenantTx) error {
		row, err := tx.Q().GetSuppression(ctx, sqlcgen.GetSuppressionParams{
			ID: id, TenantID: p.TenantID, ProjectID: &p.ProjectID,
		})
		if errors.Is(store.NotFound(err), store.ErrNotFound) {
			return notFound
		}
		if err != nil {
			return err
		}
		// Tenant-wide entries are mostly complaints. Lifting one exposes
		// every project to a recipient who asked not to be written to.
		if row.ProjectID == nil && !p.Can(accept.ScopeAdmin) {
			return domain.Errorf(domain.CodeForbidden,
				"lifting a tenant-wide suppression needs an API key with the admin scope")
		}
		n, err := tx.Q().DeleteSuppression(ctx, sqlcgen.DeleteSuppressionParams{ID: id, TenantID: p.TenantID})
		if err != nil {
			return err
		}
		if n == 0 {
			return notFound
		}
		return nil
	})
}
