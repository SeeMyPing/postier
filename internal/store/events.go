// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"encoding/json"
	"net/netip"
	"time"

	"github.com/google/uuid"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// NewEvent is an event about to be appended to a message's history.
type NewEvent struct {
	MessageID  uuid.UUID
	CreatedAt  time.Time // of the message: its partition key
	TenantID   uuid.UUID
	Type       domain.EventType
	OccurredAt time.Time
	// DedupKey makes the append idempotent: the same key at the same
	// OccurredAt is inserted once.
	DedupKey     string
	Attempts     int
	SMTPCode     *int32
	EnhancedCode *string
	BounceClass  *string
	RemoteMX     *string
	SourceIP     *netip.Addr
	Detail       map[string]any
}

// AppendResult says what an append did.
type AppendResult struct {
	Inserted      bool // false: the event was already known
	StatusChanged bool // false: recorded, but the status had precedence
}

// AppendEvent is the one way a message's status changes after acceptance:
// append the event, then move the status if, and only if, the transition
// matrix allows it from the current status. Both happen in the caller's
// transaction.
//
// It never takes an application lock and never reads the status first: the
// rule is in the WHERE clause of the update, so concurrent ingest replicas
// cannot interleave a check and a write.
func AppendEvent(ctx context.Context, q *Queries, ev NewEvent) (AppendResult, error) {
	detail := []byte(`{}`)
	if len(ev.Detail) > 0 {
		var err error
		if detail, err = json.Marshal(ev.Detail); err != nil {
			return AppendResult{}, err
		}
	}
	inserted, err := q.InsertEvent(ctx, sqlcgen.InsertEventParams{
		// The id embeds occurred_at, like a message id embeds created_at.
		ID: domain.NewIDAt(ev.OccurredAt), MessageID: ev.MessageID, TenantID: ev.TenantID,
		Type: string(ev.Type), OccurredAt: ev.OccurredAt, DedupKey: ev.DedupKey,
		SmtpCode: ev.SMTPCode, EnhancedCode: ev.EnhancedCode, BounceClass: ev.BounceClass,
		RemoteMx: ev.RemoteMX, SourceIp: ev.SourceIP, Detail: detail,
	})
	if err != nil {
		return AppendResult{}, err
	}
	if inserted == 0 {
		return AppendResult{}, nil
	}
	res := AppendResult{Inserted: true}

	to, ok := ev.Type.TargetStatus()
	if !ok {
		return res, nil
	}
	updated, err := q.TransitionMessage(ctx, sqlcgen.TransitionMessageParams{
		ID: ev.MessageID, CreatedAt: ev.CreatedAt,
		Status: string(to), StatusAt: ev.OccurredAt,
		Attempts:    int32(ev.Attempts), //nolint:gosec // a delivery attempt count
		AllowedFrom: domain.AllowedFromStrings(to),
	})
	if err != nil {
		return res, err
	}
	res.StatusChanged = updated == 1
	if updated == 0 && ev.Attempts > 0 {
		// The status had precedence, but the count of attempts must not
		// depend on the order records arrive in.
		err = q.RaiseAttempts(ctx, sqlcgen.RaiseAttemptsParams{
			ID: ev.MessageID, CreatedAt: ev.CreatedAt,
			Attempts: int32(ev.Attempts), //nolint:gosec // a delivery attempt count
		})
	}
	return res, err
}
