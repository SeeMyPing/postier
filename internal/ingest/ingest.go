// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/kumo"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// maxBatchBytes bounds a log hook request. A record is about 1.5 kB, 20 kB
// for a complaint that quotes the original message.
const maxBatchBytes = 64 << 20

// maxClockSkew is how far an event may be from what the clocks allow: before
// its own message, or in the future. Beyond that the record is refused, not
// stored under a nonsensical date.
const maxClockSkew = time.Hour

// Service ingests log records.
type Service struct {
	pool   *store.SystemPool
	signer *domain.Signer
	token  string
	log    *slog.Logger
	now    func() time.Time
	// captureDir, when set, receives a copy of every batch as it arrived.
	// It is how the reference files of internal/kumo/testdata are produced.
	captureDir string
	// dropAck is a fault injected for tests: the probability of answering
	// 503 after a batch was committed, which makes KumoMTA send it again.
	dropAck float64
}

// Config configures the ingest service.
type Config struct {
	Pool   *store.SystemPool
	Signer *domain.Signer
	// Token is the bearer token KumoMTA presents. Required: the endpoint
	// changes message statuses.
	Token      string
	Log        *slog.Logger
	CaptureDir string
	// ChaosDropAck, between 0 and 1, makes that share of the committed
	// batches look failed to KumoMTA. Test benches only: it exercises the
	// deduplication on real redeliveries. Statuses must stay exact.
	ChaosDropAck float64
}

// New returns the ingest service.
func New(cfg Config) (*Service, error) {
	if len(cfg.Token) < 16 {
		return nil, errors.New("ingest: the hook token must be at least 16 characters")
	}
	return &Service{
		pool: cfg.Pool, signer: cfg.Signer, token: cfg.Token,
		log: cfg.Log, now: time.Now, captureDir: cfg.CaptureDir, dropAck: cfg.ChaosDropAck,
	}, nil
}

// Stats summarises what a batch did.
type Stats struct {
	Received   int `json:"received"`
	Inserted   int `json:"inserted"`
	Duplicates int `json:"duplicates"`
	Ignored    int `json:"ignored"`
	Unverified int `json:"unverified"`
	// Invalid records could not be decoded. They are parked in
	// kumo_dead_records, in the same transaction as the rest of the batch.
	Invalid int `json:"invalid"`
}

// Handler returns the endpoint KumoMTA's log hook posts to.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/kumo/records", s.handleRecords)
	return mux
}

func (s *Service) handleRecords(w http.ResponseWriter, r *http.Request) {
	scheme, token, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	if !strings.EqualFold(scheme, "Bearer") ||
		subtle.ConstantTimeCompare([]byte(token), []byte(s.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBatchBytes))
	if err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	s.capture(body)

	records, invalid, err := kumo.DecodeBatch(body)
	if err != nil {
		// The body itself is not a batch (truncated, wrong endpoint).
		// Refusing it makes KumoMTA keep the records and try again.
		s.log.ErrorContext(r.Context(), "undecodable batch refused", "err", err, "bytes", len(body))
		http.Error(w, "undecodable batch", http.StatusBadRequest)
		return
	}

	stats, err := s.Process(r.Context(), records, invalid)
	if err != nil {
		// Nothing was committed. KumoMTA keeps the batch and retries.
		s.log.ErrorContext(r.Context(), "batch failed, will be retried", "err", err, "records", len(records))
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
		return
	}
	if s.dropAck > 0 && rand.Float64() < s.dropAck { //nolint:gosec // a test fault, not a secret
		s.log.WarnContext(r.Context(), "chaos: committed batch reported as failed", "records", stats.Received)
		http.Error(w, "chaos: acknowledgement dropped", http.StatusServiceUnavailable)
		return
	}
	// Only now, after the commit, is the batch acknowledged.
	writeStats(w, stats)
}

func writeStats(w http.ResponseWriter, stats Stats) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

// pending is a record that maps to an event of a known message id.
type pending struct {
	rec        *kumo.Record
	messageID  uuid.UUID
	createdAt  time.Time
	eventType  domain.EventType
	occurredAt time.Time
}

// Process stores a batch of records in one transaction: either every record
// is taken into account, or none is and the batch must be sent again.
//
// It is idempotent. A record already seen inserts nothing and changes
// nothing, so KumoMTA resending a batch (ingest crashed before answering) is
// harmless.
func (s *Service) Process(ctx context.Context, records []kumo.Record, invalid []kumo.Invalid) (Stats, error) {
	stats := Stats{Received: len(records) + len(invalid), Invalid: len(invalid)}
	now := s.now()

	todo := make([]pending, 0, len(records))
	for i := range records {
		rec := &records[i]
		p, err := s.prepare(rec, now)
		switch {
		case err == nil:
			todo = append(todo, p)
		case errors.Is(err, errUnverified):
			stats.Unverified++
			s.log.WarnContext(ctx, "record with a forged or foreign reference ignored",
				"type", rec.Type, "kumo_id", rec.ID)
		default:
			stats.Ignored++
			s.log.DebugContext(ctx, "record ignored", "type", rec.Type, "kumo_id", rec.ID, "reason", err.Error())
		}
	}
	if len(todo) == 0 && len(invalid) == 0 {
		return stats, nil
	}

	// Rows are locked in a consistent order across concurrent batches, which
	// rules out deadlocks between ingest replicas. The order inside a batch
	// has no functional effect: precedence is decided by the transition
	// matrix, not by arrival order.
	slices.SortStableFunc(todo, func(a, b pending) int {
		return cmp.Or(
			cmp.Compare(a.messageID.String(), b.messageID.String()),
			a.occurredAt.Compare(b.occurredAt),
		)
	})

	// Counted apart and merged after the commit: a rolled back batch must
	// not report inserts.
	var inserted, duplicates, ignored int
	err := s.pool.InTx(ctx, func(q *store.Queries, _ pgx.Tx) error {
		inserted, duplicates, ignored = 0, 0, 0
		for _, bad := range invalid {
			s.log.ErrorContext(ctx, "undecodable record parked", "err", bad.Err)
			if err := q.InsertDeadRecord(ctx, sqlcgen.InsertDeadRecordParams{
				Error: bad.Err.Error(), Record: bad.Raw,
			}); err != nil {
				return err
			}
		}
		for _, p := range todo {
			ref, err := q.GetMessageRef(ctx, sqlcgen.GetMessageRefParams{ID: p.messageID, CreatedAt: p.createdAt})
			if errors.Is(err, pgx.ErrNoRows) {
				// A well-formed id that is not ours: another installation
				// sharing the MTA, or a purged message.
				ignored++
				continue
			}
			if err != nil {
				return err
			}
			res, err := store.AppendEvent(ctx, q, s.newEvent(p, ref.TenantID))
			if err != nil {
				return fmt.Errorf("record %s (%s): %w", p.rec.ID, p.rec.Type, err)
			}
			if res.Inserted {
				inserted++
			} else {
				duplicates++
			}
		}
		return nil
	})
	if err != nil {
		return Stats{Received: stats.Received}, err
	}
	stats.Inserted, stats.Duplicates = inserted, duplicates
	stats.Ignored += ignored

	s.log.InfoContext(ctx, "batch ingested",
		"received", stats.Received, "inserted", stats.Inserted, "duplicates", stats.Duplicates,
		"ignored", stats.Ignored, "unverified", stats.Unverified, "invalid", stats.Invalid)
	return stats, nil
}

// prepare decides, without touching the database, whether a record becomes
// an event and for which message.
func (s *Service) prepare(rec *kumo.Record, now time.Time) (pending, error) {
	if rec.IsLogHookEcho() {
		return pending{}, errUnknownType
	}
	eventType, ok := eventTypes[rec.Type]
	if !ok {
		return pending{}, errUnknownType
	}
	id, err := resolveMessageID(rec, s.signer)
	if err != nil {
		return pending{}, err
	}
	createdAt, err := domain.IDTime(id)
	if err != nil {
		return pending{}, errNoMessage
	}
	occurredAt := rec.OccurredAt()
	if occurredAt.Before(createdAt.Add(-maxClockSkew)) || occurredAt.After(now.Add(maxClockSkew)) {
		return pending{}, fmt.Errorf("event time %s out of range", occurredAt.Format(time.RFC3339))
	}
	return pending{rec: rec, messageID: id, createdAt: createdAt, eventType: eventType, occurredAt: occurredAt}, nil
}

func (s *Service) newEvent(p pending, tenantID uuid.UUID) store.NewEvent {
	rec := p.rec
	ev := store.NewEvent{
		MessageID: p.messageID, CreatedAt: p.createdAt, TenantID: tenantID,
		Type: p.eventType, OccurredAt: p.occurredAt, DedupKey: rec.DedupKey(),
		Attempts: attempts(rec), SourceIP: sourceIP(rec), Detail: detail(rec),
	}
	if rec.Response.Code != 0 {
		code := int32(rec.Response.Code) //nolint:gosec // a three-digit SMTP code
		ev.SMTPCode = &code
	}
	if rec.Response.EnhancedCode != nil {
		enhanced := rec.Response.EnhancedCode.String()
		ev.EnhancedCode = &enhanced
	}
	// KumoMTA classifies its own administrative bounce from the 5.7.1 code it
	// fabricates ("AuthenticationFailed"), which would be misleading here.
	if rec.BounceClassification != "" && rec.BounceClassification != "Uncategorized" &&
		rec.Type != kumo.TypeAdminBounce {
		ev.BounceClass = &rec.BounceClassification
	}
	// For an OOB or a Feedback record the peer is whoever relayed the
	// report, not the server that refused the message.
	if rec.PeerAddress != nil && rec.PeerAddress.Name != "" &&
		rec.Type != kumo.TypeOOB && rec.Type != kumo.TypeFeedback && rec.Type != kumo.TypeReception {
		ev.RemoteMX = &rec.PeerAddress.Name
	}
	return ev
}

// capture writes a batch to the capture directory. Best effort: a failure to
// capture must never fail an ingestion.
func (s *Service) capture(body []byte) {
	if s.captureDir == "" {
		return
	}
	name := filepath.Join(s.captureDir, fmt.Sprintf("batch-%d.json", s.now().UnixNano()))
	if err := os.WriteFile(name, body, 0o600); err != nil {
		s.log.Warn("capture failed", "err", err)
	}
}
