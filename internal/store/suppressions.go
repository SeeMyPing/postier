// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// SuppressedAmong returns, among the given addresses, those the project must
// not send to, with the reason. The key is the address in normalised form.
func SuppressedAmong(ctx context.Context, q *Queries, tenantID, projectID uuid.UUID, addrs []domain.Address) (map[string]domain.SuppressionReason, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	hashes := make([][]byte, len(addrs))
	byHash := make(map[string]string, len(addrs))
	for i, a := range addrs {
		h := a.Hash()
		hashes[i] = h[:]
		byHash[string(h[:])] = a.String()
	}
	rows, err := q.FindSuppressions(ctx, sqlcgen.FindSuppressionsParams{
		TenantID: tenantID, ProjectID: &projectID, Hashes: hashes,
	})
	if err != nil {
		return nil, err
	}
	out := make(map[string]domain.SuppressionReason, len(rows))
	for _, r := range rows {
		addr := byHash[string(r.AddrHash)]
		// A tenant-wide suppression has precedence in what is reported.
		if _, seen := out[addr]; !seen || r.Reason == string(domain.ReasonComplaint) {
			out[addr] = domain.SuppressionReason(r.Reason)
		}
	}
	return out, nil
}

// Recipient identifies who an event is about.
type Recipient struct {
	TenantID  uuid.UUID
	ProjectID uuid.UUID
	MessageID uuid.UUID
	Addr      string
	AddrHash  []byte
}

// SuppressOnEvent applies the suppression policy to an event that has just
// been appended, in the same transaction: a hard bounce or a complaint
// suppresses at once, soft failures once they repeat. It reports whether a
// suppression was written.
func SuppressOnEvent(ctx context.Context, q *Queries, rcpt Recipient, trigger domain.SuppressionTrigger, at time.Time) (bool, error) {
	params := sqlcgen.UpsertSuppressionParams{
		ID: domain.NewID(), TenantID: rcpt.TenantID, ProjectID: &rcpt.ProjectID,
		AddrHash: rcpt.AddrHash, Addr: &rcpt.Addr, MessageID: &rcpt.MessageID,
	}
	switch trigger {
	case domain.SuppressForProject:
		params.Reason = string(domain.ReasonHardBounce)
	case domain.SuppressForTenant:
		params.Reason = string(domain.ReasonComplaint)
		params.ProjectID = nil
	case domain.CountSoftBounce:
		failures, err := q.CountSoftFailures(ctx, sqlcgen.CountSoftFailuresParams{
			ProjectID: rcpt.ProjectID, RcptHash: rcpt.AddrHash,
			Since: at.Add(-domain.SoftBounceWindow), Classes: domain.SoftBounceClasses(),
		})
		if err != nil {
			return false, err
		}
		if failures < domain.SoftBounceThreshold {
			return false, nil
		}
		until := at.Add(domain.SoftBounceWindow)
		params.Reason = string(domain.ReasonSoftBounces)
		params.ExpiresAt = &until
	default:
		return false, nil
	}
	_, err := q.UpsertSuppression(ctx, params)
	return err == nil, err
}
