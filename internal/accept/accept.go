// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package accept

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/render"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// Sources of a message.
const (
	SourceAPI  = "api"
	SourceSMTP = "smtp"
)

// IdempotencyTTL is how long an Idempotency-Key is remembered.
const IdempotencyTTL = 24 * time.Hour

// Options are the parts of a submission that are not content.
type Options struct {
	Source         string
	IdempotencyKey string
	TraceID        string
}

// Result is the answer to a send: one message per recipient.
type Result struct {
	BatchID  uuid.UUID  `json:"batch_id"`
	Messages []Accepted `json:"messages"`
}

// Accepted is one accepted message.
type Accepted struct {
	ID     uuid.UUID     `json:"id"`
	To     string        `json:"to"`
	Status domain.Status `json:"status"`
}

// JobInserter enqueues the injection of accepted messages, in the caller's
// transaction.
type JobInserter interface {
	EnqueueInjections(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) error
}

// Service accepts messages.
type Service struct {
	pool   *store.AppPool
	jobs   JobInserter
	limits Limits
	log    *slog.Logger
	now    func() time.Time
}

// NewService returns the acceptance pipeline.
func NewService(pool *store.AppPool, jobs JobInserter, limits Limits, log *slog.Logger) *Service {
	return &Service{pool: pool, jobs: jobs, limits: limits, log: log, now: time.Now}
}

// Send accepts a message for each recipient of the request.
//
// When it returns without error, the messages, their accepted events and
// their injection jobs are committed: "accepted" means "persisted". The
// second return value is true when the result is the replay of an earlier
// request made with the same idempotency key.
func (s *Service) Send(ctx context.Context, p Principal, req Request, opts Options) (Result, bool, error) {
	if !p.Can(ScopeSend) {
		return Result{}, false, domain.Errorf(domain.CodeForbidden, "this API key cannot send email")
	}
	if err := validateIdempotencyKey(opts.IdempotencyKey); err != nil {
		return Result{}, false, err
	}
	v, err := s.limits.validate(req)
	if err != nil {
		return Result{}, false, err
	}

	now := s.now().UTC()
	v.content.Date = now
	mime, err := render.Build(v.content)
	if err != nil {
		if errors.Is(err, render.ErrHeaderNotAllowed) {
			return Result{}, false, domain.FieldErrorf("headers", "%v", err)
		}
		return Result{}, false, fmt.Errorf("render: %w", err)
	}
	// Compressed outside the transaction: it is CPU work, and the
	// transaction holds a connection.
	content := store.EncodeContent(mime)
	requestHash := hashRequest(req)

	var (
		result   Result
		replayed bool
	)
	err = s.pool.InTenantTx(ctx, p.TenantID, func(tx store.TenantTx) error {
		q := tx.Q()

		// 1. Idempotency goes first. It takes the lock that makes a
		// concurrent request with the same key wait for this transaction.
		if opts.IdempotencyKey != "" {
			prior, found, err := claimIdempotencyKey(ctx, q, p.ProjectID, opts.IdempotencyKey, requestHash, now)
			if err != nil {
				return err
			}
			if found {
				result, replayed = prior, true
				return nil
			}
		}

		// 2. The sender must own the domain, and have proven it.
		dom, err := q.GetSendingDomain(ctx, v.content.From.Address.Domain)
		switch {
		case errors.Is(err, pgx.ErrNoRows), err == nil && dom.ProjectID != p.ProjectID:
			// Same answer whether the domain is unknown or belongs to
			// someone else: do not reveal other tenants' domains.
			return domain.Errorf(domain.CodeDomainNotAllowed,
				"%s is not a sending domain of this project", v.content.From.Address.Domain)
		case err != nil:
			return err
		case dom.VerifiedAt == nil:
			return domain.Errorf(domain.CodeDomainNotVerified,
				"%s is not verified yet: check its DNS records", dom.Domain)
		}

		// 3. One body for the whole send, one message per recipient.
		result = Result{BatchID: domain.NewIDAt(now)}
		contentID := domain.NewIDAt(now)
		if err := q.InsertContent(ctx, sqlcgen.InsertContentParams{
			ID: contentID, TenantID: p.TenantID, CreatedAt: now,
			Encoding: content.Encoding, Mime: content.Data, Sha256: content.SHA256,
			SizeBytes: int32(content.Size), //nolint:gosec // bounded by MaxBodyBytes
		}); err != nil {
			return err
		}
		metadata, err := json.Marshal(v.metadata)
		if err != nil {
			return err
		}
		ids := make([]uuid.UUID, 0, len(v.content.To))
		for _, rcpt := range v.content.To {
			id := domain.NewIDAt(now)
			// created_at is the id's own timestamp, not now: it is the
			// partition key, and must be recomputable from the id alone.
			createdAt, err := domain.IDTime(id)
			if err != nil {
				return err
			}
			hash := rcpt.Hash()
			size := int32(content.Size) //nolint:gosec // bounded by MaxBodyBytes
			if err := q.InsertMessage(ctx, sqlcgen.InsertMessageParams{
				ID: id, TenantID: p.TenantID, ProjectID: p.ProjectID,
				BatchID: result.BatchID, ContentID: &contentID, SendingDomainID: dom.ID,
				Source: opts.Source, FromAddr: v.content.From.Address.String(),
				RcptAddr: rcpt.String(), RcptDomain: rcpt.Domain, RcptHash: hash[:],
				Subject: &v.content.Subject, Status: string(domain.StatusAccepted),
				StatusAt: createdAt, Tags: v.tags, Metadata: metadata,
				SizeBytes: &size, TraceID: nonEmpty(opts.TraceID), CreatedAt: createdAt,
			}); err != nil {
				return err
			}
			if _, err := q.InsertEvent(ctx, sqlcgen.InsertEventParams{
				ID: domain.NewIDAt(createdAt), MessageID: id, TenantID: p.TenantID,
				Type: string(domain.EventAccepted), OccurredAt: createdAt,
				DedupKey: "accepted:" + id.String(), Detail: []byte(`{}`),
			}); err != nil {
				return err
			}
			ids = append(ids, id)
			result.Messages = append(result.Messages, Accepted{
				ID: id, To: rcpt.String(), Status: domain.StatusAccepted,
			})
		}

		// 4. The outbox: the job commits with the messages, or not at all.
		if err := s.jobs.EnqueueInjections(ctx, tx.Tx(), ids); err != nil {
			return fmt.Errorf("enqueue injection: %w", err)
		}

		if opts.IdempotencyKey != "" {
			response, err := json.Marshal(result)
			if err != nil {
				return err
			}
			status := int32(http.StatusAccepted)
			return q.CompleteIdempotencyKey(ctx, sqlcgen.CompleteIdempotencyKeyParams{
				ProjectID: p.ProjectID, Key: opts.IdempotencyKey,
				StatusCode: &status, Response: response,
			})
		}
		return nil
	})
	if err != nil {
		return Result{}, false, err
	}

	if !replayed {
		s.log.InfoContext(ctx, "messages accepted",
			"tenant_id", p.TenantID, "batch_id", result.BatchID,
			"count", len(result.Messages), "source", opts.Source, "trace_id", opts.TraceID)
	}
	return result, replayed, nil
}

// claimIdempotencyKey inserts the key, or returns the response stored by the
// request that used it first.
func claimIdempotencyKey(ctx context.Context, q *store.Queries, projectID uuid.UUID, key string, hash []byte, now time.Time) (Result, bool, error) {
	claimed, err := q.ClaimIdempotencyKey(ctx, sqlcgen.ClaimIdempotencyKeyParams{
		ProjectID: projectID, Key: key, RequestHash: hash, ExpiresAt: now.Add(IdempotencyTTL),
	})
	if err != nil {
		return Result{}, false, err
	}
	if claimed == 1 {
		return Result{}, false, nil
	}
	// The key exists and its transaction has ended: had it rolled back, the
	// insert above would have succeeded.
	prior, err := q.GetIdempotencyKey(ctx, sqlcgen.GetIdempotencyKeyParams{ProjectID: projectID, Key: key})
	if err != nil {
		return Result{}, false, err
	}
	if !bytes.Equal(prior.RequestHash, hash) {
		return Result{}, false, domain.Errorf(domain.CodeIdempotencyConflict,
			"this Idempotency-Key was already used with a different request")
	}
	var result Result
	if err := json.Unmarshal(prior.Response, &result); err != nil {
		return Result{}, false, fmt.Errorf("stored idempotent response: %w", err)
	}
	return result, true, nil
}

// hashRequest fingerprints a request for idempotency. encoding/json sorts
// map keys, so equal requests hash alike.
func hashRequest(req Request) []byte {
	raw, _ := json.Marshal(req) // a struct of strings cannot fail to encode
	sum := sha256.Sum256(raw)
	return sum[:]
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
