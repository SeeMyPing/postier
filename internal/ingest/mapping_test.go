// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"pgregory.net/rapid"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/kumo"
)

// benchSigningKey is the public development key of compose.yaml. The
// reference records were captured on that bench: their return paths and
// Message-IDs are signed with it.
const benchSigningKey = "dev-signing-key-0123456789abcdef-not-secret"

func benchSigner(t testing.TB) *domain.Signer {
	t.Helper()
	s, err := domain.NewSigner([]byte(benchSigningKey))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func otherSigner(t testing.TB) *domain.Signer {
	t.Helper()
	s, err := domain.NewSigner([]byte(strings.Repeat("another-installation", 2)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func loadSession(t testing.TB) []kumo.Record {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "kumo", "testdata", "session.json"))
	if err != nil {
		t.Fatal(err)
	}
	records, invalid, err := kumo.DecodeBatch(raw)
	if err != nil || len(invalid) > 0 {
		t.Fatalf("session: invalid=%d err=%v", len(invalid), err)
	}
	return records
}

func loadRecord(t testing.TB, typ string) kumo.Record {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "kumo", "testdata", "records", typ+".json"))
	if err != nil {
		t.Fatal(err)
	}
	records, _, err := kumo.DecodeBatch(raw)
	if err != nil || len(records) != 1 {
		t.Fatalf("%s: %v", typ, err)
	}
	return records[0]
}

func testService(t testing.TB, signer *domain.Signer) *Service {
	t.Helper()
	return &Service{
		signer: signer,
		token:  "a-long-enough-test-token",
		log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		// The session was captured on 2026-11-13; "now" is the day after.
		now: func() time.Time { return time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC) },
	}
}

func TestRecordTypesMapToEvents(t *testing.T) {
	want := map[string]domain.EventType{
		kumo.TypeReception:        domain.EventQueued,
		kumo.TypeTransientFailure: domain.EventDeferred,
		kumo.TypeDelivery:         domain.EventDelivered,
		kumo.TypeBounce:           domain.EventBounced,
		kumo.TypeOOB:              domain.EventBounced,
		kumo.TypeFeedback:         domain.EventComplained,
		kumo.TypeExpiration:       domain.EventExpired,
		kumo.TypeAdminBounce:      domain.EventCanceled,
	}
	svc := testService(t, benchSigner(t))
	for typ, event := range want {
		rec := loadRecord(t, typ)
		p, err := svc.prepare(&rec, svc.now())
		if err != nil {
			t.Errorf("%s: %v", typ, err)
			continue
		}
		if p.eventType != event {
			t.Errorf("%s -> %s, want %s", typ, p.eventType, event)
		}
		if p.messageID.Version() != 7 {
			t.Errorf("%s: message id %s", typ, p.messageID)
		}
	}
}

// A Rejection is KumoMTA refusing inbound SMTP. It concerns no message of
// ours: injection refusals come back synchronously to dispatch instead.
func TestRejectionOfInboundMailIsIgnored(t *testing.T) {
	svc := testService(t, benchSigner(t))
	rec := loadRecord(t, kumo.TypeRejection)
	if _, err := svc.prepare(&rec, svc.now()); !errors.Is(err, errNoMessage) {
		t.Fatalf("err = %v", err)
	}
}

// The OOB record is found through its signed return path, and only then.
func TestOOBIsAuthenticated(t *testing.T) {
	rec := loadRecord(t, kumo.TypeOOB)

	id, err := resolveMessageID(&rec, benchSigner(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.Sender, domain.CompactID(id)) {
		t.Errorf("resolved %s, not the id of %s", id, rec.Sender)
	}

	// Signed by someone else: a bounce forged for a guessed message id.
	if _, err := resolveMessageID(&rec, otherSigner(t)); !errors.Is(err, errUnverified) {
		t.Errorf("foreign signature: %v", err)
	}
	// Same tag, another message.
	forged := rec
	forged.Sender = strings.Replace(rec.Sender, domain.CompactID(id), domain.CompactID(domain.NewID()), 1)
	if _, err := resolveMessageID(&forged, benchSigner(t)); !errors.Is(err, errUnverified) {
		t.Errorf("swapped id: %v", err)
	}
	// Not a VERP address at all: an ordinary bounce to a role address.
	plain := rec
	plain.Sender = "postmaster@bounce.acme.test"
	if _, err := resolveMessageID(&plain, benchSigner(t)); !errors.Is(err, errNoMessage) {
		t.Errorf("unsigned address: %v", err)
	}
	// Metadata must not be a way around the signature.
	withMeta := forged
	withMeta.Meta = rec.Meta
	if _, err := resolveMessageID(&withMeta, benchSigner(t)); !errors.Is(err, errUnverified) {
		t.Errorf("metadata bypassed the signature: %v", err)
	}
}

func TestFeedbackIsAuthenticated(t *testing.T) {
	rec := loadRecord(t, kumo.TypeFeedback)

	id, err := resolveMessageID(&rec, benchSigner(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rec.FeedbackReport.OriginalMessageText(), domain.CompactID(id)) {
		t.Errorf("resolved %s, which the report does not mention", id)
	}
	if _, err := resolveMessageID(&rec, otherSigner(t)); !errors.Is(err, errUnverified) {
		t.Errorf("foreign signature: %v", err)
	}

	// A provider that redacts the envelope sender (Microsoft JMRP): the
	// Message-ID alone is enough.
	redacted := rec
	report := *rec.FeedbackReport
	report.OriginalMailFrom = ""
	redacted.FeedbackReport = &report
	if got, err := resolveMessageID(&redacted, benchSigner(t)); err != nil || got != id {
		t.Errorf("without original_mail_from: %s, %v", got, err)
	}

	// A report that quotes no message: only the return path is left.
	bare := rec
	report2 := *rec.FeedbackReport
	report2.OriginalMessage = nil
	bare.FeedbackReport = &report2
	if got, err := resolveMessageID(&bare, benchSigner(t)); err != nil || got != id {
		t.Errorf("without original_message: %s, %v", got, err)
	}

	empty := rec
	empty.FeedbackReport = nil
	if _, err := resolveMessageID(&empty, benchSigner(t)); !errors.Is(err, errNoMessage) {
		t.Errorf("no report: %v", err)
	}
}

func TestEventTimeMustBePlausible(t *testing.T) {
	svc := testService(t, benchSigner(t))
	rec := loadRecord(t, kumo.TypeDelivery)

	future := svc.now().Add(48 * time.Hour)
	rec.EventTime = &future
	if _, err := svc.prepare(&rec, svc.now()); err == nil {
		t.Error("an event from the future was accepted")
	}
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	rec.EventTime = &past
	if _, err := svc.prepare(&rec, svc.now()); err == nil {
		t.Error("an event older than its message was accepted")
	}
}

func TestLogHookEchoIsIgnored(t *testing.T) {
	svc := testService(t, benchSigner(t))
	rec := loadRecord(t, kumo.TypeDelivery)
	proto := kumo.ProtocolLogRecord
	rec.ReceptionProtocol = &proto
	if _, err := svc.prepare(&rec, svc.now()); err == nil {
		t.Fatal("a record about the log hook itself was accepted")
	}
}

func TestAttempts(t *testing.T) {
	cases := map[string]int{
		kumo.TypeReception:   0,
		kumo.TypeDelivery:    1, // first try
		kumo.TypeBounce:      1,
		kumo.TypeOOB:         0, // a report, not an attempt
		kumo.TypeFeedback:    0,
		kumo.TypeAdminBounce: 1, // bounced after one failed attempt
	}
	for typ, want := range cases {
		rec := loadRecord(t, typ)
		if got := attempts(&rec); got != want {
			t.Errorf("%s (num_attempts=%d): attempts = %d, want %d", typ, rec.NumAttempts, got, want)
		}
	}
}

// expectedStatuses is what each message of the captured session ended as on
// the bench, by recipient. See test/capture.
var expectedStatuses = map[string]domain.Status{
	"ok@sink.test":            domain.StatusDelivered,
	"550@sink.test":           domain.StatusBounced,
	"defer2-cap@sink.test":    domain.StatusDelivered,
	"421@sink.test":           domain.StatusExpired,
	"oob-cap@sink.test":       domain.StatusBounced,
	"fbl-cap@sink.test":       domain.StatusComplained,
	"defer99-admin@sink.test": domain.StatusCanceled,
}

// replay applies records to an in-memory projection, the way the database
// does: an event moves the status only if the matrix allows it.
func replay(t interface{ Fatalf(string, ...any) }, svc *Service, records []kumo.Record) map[uuid.UUID]domain.Status {
	statuses := map[uuid.UUID]domain.Status{}
	seen := map[string]bool{}
	for i := range records {
		p, err := svc.prepare(&records[i], svc.now())
		if err != nil {
			continue
		}
		if key := records[i].DedupKey(); seen[key] {
			continue
		} else {
			seen[key] = true
		}
		cur, ok := statuses[p.messageID]
		if !ok {
			cur = domain.StatusAccepted
		}
		if to, ok := p.eventType.TargetStatus(); ok && domain.CanTransition(cur, to) {
			cur = to
		}
		statuses[p.messageID] = cur
	}
	return statuses
}

func recipientsByID(t testing.TB, svc *Service, records []kumo.Record) map[uuid.UUID]string {
	out := map[uuid.UUID]string{}
	for i := range records {
		if records[i].Type != kumo.TypeReception {
			continue
		}
		if p, err := svc.prepare(&records[i], svc.now()); err == nil {
			out[p.messageID] = records[i].Recipient.First()
		}
	}
	if len(out) != len(expectedStatuses) {
		t.Fatalf("session holds %d messages, want %d", len(out), len(expectedStatuses))
	}
	return out
}

// TestSessionEndsInTheRightStatuses replays a whole real session.
func TestSessionEndsInTheRightStatuses(t *testing.T) {
	svc := testService(t, benchSigner(t))
	records := loadSession(t)
	recipients := recipientsByID(t, svc, records)

	for id, status := range replay(t, svc, records) {
		if want := expectedStatuses[recipients[id]]; status != want {
			t.Errorf("%s: %s, want %s", recipients[id], status, want)
		}
	}
}

// TestSessionIsOrderAndReplayIndependent is the statement of the milestone
// in miniature: KumoMTA may deliver the records in any order, and any of
// them several times, and every message still ends in the same status.
func TestSessionIsOrderAndReplayIndependent(t *testing.T) {
	svc := testService(t, benchSigner(t))
	records := loadSession(t)
	recipients := recipientsByID(t, svc, records)

	rapid.Check(t, func(t *rapid.T) {
		shuffled := rapid.Permutation(records).Draw(t, "order")
		// Redeliver a random subset on top.
		extra := rapid.SliceOfN(rapid.SampledFrom(records), 0, len(records)).Draw(t, "redelivered")
		for id, status := range replay(t, svc, append(shuffled, extra...)) {
			if want := expectedStatuses[recipients[id]]; status != want {
				t.Fatalf("%s: %s, want %s", recipients[id], status, want)
			}
		}
	})
}

func TestEndpointRequiresTheToken(t *testing.T) {
	svc := testService(t, benchSigner(t))
	h := svc.Handler()
	for name, header := range map[string]string{
		"none":   "",
		"wrong":  "Bearer not-the-token",
		"basic":  "Basic YTpi",
		"prefix": "Bearer a-long-enough-test-toke",
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/kumo/records", strings.NewReader("[]"))
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status %d", name, rec.Code)
		}
	}
}

// A body that is not a batch is refused, so that KumoMTA keeps the records.
func TestEndpointRefusesGarbage(t *testing.T) {
	svc := testService(t, benchSigner(t))
	req := httptest.NewRequest(http.MethodPost, "/v1/kumo/records", strings.NewReader(`{"type":`))
	req.Header.Set("Authorization", "Bearer a-long-enough-test-token")
	rec := httptest.NewRecorder()
	svc.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestNewRequiresAToken(t *testing.T) {
	if _, err := New(Config{Token: "short"}); err == nil {
		t.Fatal("a short token was accepted")
	}
}
