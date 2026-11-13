// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	"github.com/SeeMyPing/postier/internal/dispatch"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/ingest"
	"github.com/SeeMyPing/postier/internal/kumo"
	"github.com/SeeMyPing/postier/internal/queue"
	"github.com/SeeMyPing/postier/internal/store"
)

// benchSigningKey is the public development key of compose.yaml, which
// signed the records of internal/kumo/testdata.
const benchSigningKey = "dev-signing-key-0123456789abcdef-not-secret"

func signer(t *testing.T) *domain.Signer {
	t.Helper()
	s, err := domain.NewSigner([]byte(benchSigningKey))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// --- dispatch ----------------------------------------------------------------

// fakeKumo records injections and answers with a scripted error.
type fakeKumo struct {
	mu    sync.Mutex
	calls []kumo.Injection
	err   error
	done  chan struct{}
}

func (f *fakeKumo) Inject(_ context.Context, in kumo.Injection) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, in)
	if f.done != nil {
		f.done <- struct{}{}
	}
	return f.err
}

func (f *fakeKumo) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (e *env) sendOne(t *testing.T, to string) uuid.UUID {
	t.Helper()
	res, _, err := e.service().Send(context.Background(), e.principal, validRequest(to), apiOpts(""))
	if err != nil {
		t.Fatal(err)
	}
	return res.Messages[0].ID
}

func (e *env) worker(t *testing.T, k dispatch.Injector) *dispatch.InjectWorker {
	t.Helper()
	return &dispatch.InjectWorker{Pool: e.system, Kumo: k, Signer: signer(t), Log: quiet, GuardDelay: 15 * time.Second}
}

func job(id uuid.UUID, attempt int, lastErrorAgo time.Duration) *river.Job[queue.InjectArgs] {
	row := &rivertype.JobRow{Attempt: attempt, MaxAttempts: 20}
	for i := 1; i < attempt; i++ {
		row.Errors = append(row.Errors, rivertype.AttemptError{Attempt: i, At: time.Now().Add(-lastErrorAgo), Error: "timeout"})
	}
	return &river.Job[queue.InjectArgs]{JobRow: row, Args: queue.InjectArgs{MessageID: id}}
}

func TestDispatchInjectsWithSignedIdentifiers(t *testing.T) {
	e := newEnv(t)
	id := e.sendOne(t, "alice@example.org")
	k := &fakeKumo{}

	if err := e.worker(t, k).Work(context.Background(), job(id, 1, 0)); err != nil {
		t.Fatal(err)
	}
	if k.count() != 1 {
		t.Fatalf("%d injections", k.count())
	}
	in := k.calls[0]
	if in.Recipient != "alice@example.org" || in.PostierID != id.String() || in.Tenant != e.tenantID.String() || in.Trace != "trace-test" {
		t.Errorf("injection = %+v", in)
	}
	// The envelope sender and the Message-ID both lead back to the message,
	// and only with the right key.
	if got, err := signer(t).ParseReturnPath(in.EnvelopeSender); err != nil || got != id {
		t.Errorf("return path %q: %s, %v", in.EnvelopeSender, got, err)
	}
	if !strings.HasSuffix(in.EnvelopeSender, "@bounce.acme.test") {
		t.Errorf("return path domain: %q", in.EnvelopeSender)
	}
	header, _, _ := bytes.Cut(in.Content, []byte("\r\n"))
	mid := strings.TrimPrefix(string(header), "Message-ID: ")
	if got, err := signer(t).ParseMessageID(mid); err != nil || got != id {
		t.Errorf("first header %q: %s, %v", header, got, err)
	}
	if !bytes.Contains(in.Content, []byte("Subject: Votre commande 48213\r\n")) {
		t.Error("stored body was not restored")
	}
	// Still accepted: it becomes queued when KumoMTA confirms the reception.
	if status, _ := e.status(t, id); status != domain.StatusAccepted {
		t.Errorf("status = %s", status)
	}
}

func TestDispatchRejection(t *testing.T) {
	e := newEnv(t)
	id := e.sendOne(t, "alice@example.org")
	k := &fakeKumo{err: fmt.Errorf("%w: 550 5.7.1 nope", kumo.ErrRejected)}

	// A permanent refusal ends the job: retrying would fail the same way.
	if err := e.worker(t, k).Work(context.Background(), job(id, 1, 0)); err != nil {
		t.Fatal(err)
	}
	if status, _ := e.status(t, id); status != domain.StatusRejected {
		t.Errorf("status = %s", status)
	}
	if got := e.eventTypes(t, id); got != "accepted,rejected" {
		t.Errorf("events = %q", got)
	}
	// Running again changes nothing and injects nothing.
	if err := e.worker(t, k).Work(context.Background(), job(id, 2, time.Hour)); err != nil {
		t.Fatal(err)
	}
	if k.count() != 1 || e.eventTypes(t, id) != "accepted,rejected" {
		t.Errorf("a rejected message was processed again: %d injections", k.count())
	}
}

func TestDispatchTransientFailureIsRetried(t *testing.T) {
	e := newEnv(t)
	id := e.sendOne(t, "alice@example.org")
	k := &fakeKumo{err: errors.New("connection refused")}

	if err := e.worker(t, k).Work(context.Background(), job(id, 1, 0)); err == nil {
		t.Fatal("a transient failure must fail the job, so that River retries it")
	}
	if status, _ := e.status(t, id); status != domain.StatusAccepted {
		t.Errorf("status = %s", status)
	}

	// Out of attempts: the message is closed rather than left in limbo.
	last := job(id, 20, time.Hour)
	if err := e.worker(t, k).Work(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	if status, _ := e.status(t, id); status != domain.StatusRejected {
		t.Errorf("status after the last attempt = %s", status)
	}
}

func TestDispatchRechecksBeforeInjecting(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// Cancelled between acceptance and injection.
	cancelled := e.sendOne(t, "alice@example.org")
	if _, err := e.admin.Exec(ctx, "UPDATE messages SET status = 'canceled' WHERE id = $1", cancelled); err != nil {
		t.Fatal(err)
	}
	k := &fakeKumo{}
	if err := e.worker(t, k).Work(ctx, job(cancelled, 1, 0)); err != nil || k.count() != 0 {
		t.Errorf("cancelled message: %d injections, err=%v", k.count(), err)
	}

	// The domain lost its verification in the meantime.
	orphan := e.sendOne(t, "bob@example.org")
	if _, err := e.admin.Exec(ctx, "UPDATE sending_domains SET verified_at = NULL"); err != nil {
		t.Fatal(err)
	}
	if err := e.worker(t, k).Work(ctx, job(orphan, 1, 0)); err != nil || k.count() != 0 {
		t.Errorf("unverified domain: %d injections, err=%v", k.count(), err)
	}
	if status, _ := e.status(t, orphan); status != domain.StatusRejected {
		t.Errorf("status = %s", status)
	}

	// A job for a message that does not exist is dropped, not retried forever.
	err := e.worker(t, k).Work(ctx, job(domain.NewID(), 1, 0))
	var cancel *river.JobCancelError
	if !errors.As(err, &cancel) {
		t.Errorf("unknown message: %v", err)
	}
}

// TestDispatchDoesNotInjectTwice covers the window described in
// docs/implementation-plan.md §2.6: KumoMTA took the message, the job was
// not marked done, the job runs again.
func TestDispatchDoesNotInjectTwice(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// 1. The Reception record of the first attempt has arrived.
	seen := e.sendOne(t, "alice@example.org")
	createdAt, _ := domain.IDTime(seen)
	err := e.system.InTx(ctx, func(q *store.Queries, _ pgx.Tx) error {
		_, err := store.AppendEvent(ctx, q, store.NewEvent{
			MessageID: seen, CreatedAt: createdAt, TenantID: e.tenantID,
			Type: domain.EventQueued, OccurredAt: time.Now().UTC().Truncate(time.Microsecond), DedupKey: "k1",
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	k := &fakeKumo{}
	if err := e.worker(t, k).Work(ctx, job(seen, 2, time.Hour)); err != nil || k.count() != 0 {
		t.Errorf("message already queued: %d injections, err=%v", k.count(), err)
	}

	// 2. No record yet, and the previous attempt is recent: wait for it.
	pending := e.sendOne(t, "bob@example.org")
	err = e.worker(t, k).Work(ctx, job(pending, 2, time.Second))
	var snooze *river.JobSnoozeError
	if !errors.As(err, &snooze) || k.count() != 0 {
		t.Fatalf("recent attempt: %d injections, err=%v", k.count(), err)
	}
	if snooze.Duration <= 0 || snooze.Duration > 15*time.Second {
		t.Errorf("snoozed for %s", snooze.Duration)
	}

	// 3. No record after the guard delay: the first attempt did fail.
	if err := e.worker(t, k).Work(ctx, job(pending, 2, time.Minute)); err != nil || k.count() != 1 {
		t.Errorf("after the guard delay: %d injections, err=%v", k.count(), err)
	}
}

// TestOutboxEndToEnd runs the real queue: a message accepted through the app
// role is picked up by a worker running as the system role.
func TestOutboxEndToEnd(t *testing.T) {
	e := newEnv(t)
	k := &fakeKumo{done: make(chan struct{}, 4)}
	client, err := queue.NewWorkerClient(e.system.Raw(), queue.WorkerConfig{
		Workers: dispatch.Workers(e.worker(t, k)), InjectConcurrency: 4, Logger: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Stop(context.Background()) }()

	id := e.sendOne(t, "alice@example.org")
	select {
	case <-k.done:
	case <-time.After(20 * time.Second):
		t.Fatal("the injection job was never worked")
	}
	if k.calls[0].PostierID != id.String() {
		t.Errorf("injected %s, want %s", k.calls[0].PostierID, id)
	}
}

// --- ingest ------------------------------------------------------------------

func loadSession(t *testing.T) []kumo.Record {
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

// What each message of the captured session ended as on the bench.
var sessionOutcome = map[string]struct {
	status   domain.Status
	attempts int
	events   string
}{
	"ok@sink.test":            {domain.StatusDelivered, 1, "accepted,queued,delivered"},
	"550@sink.test":           {domain.StatusBounced, 1, "accepted,queued,bounced"},
	"defer2-cap@sink.test":    {domain.StatusDelivered, 3, "accepted,queued,deferred,deferred,delivered"},
	"421@sink.test":           {domain.StatusExpired, 6, "accepted,queued,deferred,deferred,deferred,deferred,deferred,deferred,expired"},
	"oob-cap@sink.test":       {domain.StatusBounced, 1, "accepted,queued,delivered,bounced"},
	"fbl-cap@sink.test":       {domain.StatusComplained, 1, "accepted,queued,delivered,complained"},
	"defer99-admin@sink.test": {domain.StatusCanceled, 1, "accepted,queued,deferred,canceled"},
}

// restoreSession recreates the messages the captured records are about, with
// their original ids, in accepted state.
func (e *env) restoreSession(t *testing.T, records []kumo.Record) map[string]uuid.UUID {
	t.Helper()
	ctx := context.Background()
	var domainID uuid.UUID
	if err := e.admin.QueryRow(ctx, "SELECT id FROM sending_domains WHERE domain = 'acme.test'").Scan(&domainID); err != nil {
		t.Fatal(err)
	}
	ids := map[string]uuid.UUID{}
	for i := range records {
		rec := &records[i]
		if rec.Type != kumo.TypeReception || rec.MetaString(kumo.MetaPostierID) == "" {
			continue
		}
		id := uuid.MustParse(rec.MetaString(kumo.MetaPostierID))
		createdAt, err := domain.IDTime(id)
		if err != nil {
			t.Fatal(err)
		}
		e.ensureWeek(t, createdAt)
		rcpt := rec.Recipient.First()
		_, err = e.admin.Exec(ctx, `
			INSERT INTO messages (id, tenant_id, project_id, batch_id, sending_domain_id, source,
			                      from_addr, rcpt_addr, rcpt_domain, rcpt_hash, status, status_at, created_at)
			VALUES ($1, $2, $3, $1, $4, 'api', 'no-reply@acme.test', $5, 'sink.test', '\x00', 'accepted', $6, $6)`,
			id, e.tenantID, e.projectID, domainID, rcpt, createdAt)
		if err != nil {
			t.Fatal(err)
		}
		_, err = e.admin.Exec(ctx, `
			INSERT INTO events (id, message_id, tenant_id, type, occurred_at, dedup_key)
			VALUES ($1, $2, $3, 'accepted', $4, $5)`,
			domain.NewIDAt(createdAt), id, e.tenantID, createdAt, "accepted:"+id.String())
		if err != nil {
			t.Fatal(err)
		}
		ids[rcpt] = id
	}
	if len(ids) != len(sessionOutcome) {
		t.Fatalf("session holds %d messages, want %d", len(ids), len(sessionOutcome))
	}
	return ids
}

func (e *env) ingest(t *testing.T) *ingest.Service {
	t.Helper()
	svc, err := ingest.New(ingest.Config{Pool: e.system, Signer: signer(t), Token: "a-long-enough-test-token", Log: quiet})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func (e *env) checkSession(t *testing.T, ids map[string]uuid.UUID) {
	t.Helper()
	for rcpt, want := range sessionOutcome {
		status, attempts := e.status(t, ids[rcpt])
		if status != want.status || attempts != want.attempts {
			t.Errorf("%s: %s after %d attempts, want %s after %d", rcpt, status, attempts, want.status, want.attempts)
		}
		if got := e.eventTypes(t, ids[rcpt]); got != want.events {
			t.Errorf("%s: events %q, want %q", rcpt, got, want.events)
		}
	}
}

// TestIngestRealSession stores a whole captured session, then stores it
// again: statuses are exact, and nothing is counted twice.
func TestIngestRealSession(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	records := loadSession(t)
	ids := e.restoreSession(t, records)
	svc := e.ingest(t)

	first, err := svc.Process(ctx, records, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 27 records: 25 about our messages; the Rejection and the Reception of
	// the inbound bounce concern no message.
	if first.Received != len(records) || first.Inserted != 25 || first.Ignored != 2 || first.Duplicates != 0 || first.Unverified != 0 {
		t.Errorf("first pass: %+v", first)
	}
	e.checkSession(t, ids)
	events := e.count(t, "SELECT count(*) FROM events")

	// KumoMTA sends everything again (ingest crashed before answering).
	second, err := svc.Process(ctx, records, nil)
	if err != nil {
		t.Fatal(err)
	}
	if second.Inserted != 0 || second.Duplicates != first.Inserted {
		t.Errorf("second pass: %+v", second)
	}
	e.checkSession(t, ids)
	if n := e.count(t, "SELECT count(*) FROM events"); n != events {
		t.Errorf("%d events after the replay, %d before", n, events)
	}

	// The details kept for support.
	var (
		code     int
		enhanced string
		class    string
		mx       string
		response string
	)
	err = e.admin.QueryRow(ctx, `
		SELECT smtp_code, enhanced_code, bounce_class, remote_mx, detail->>'response'
		FROM events WHERE message_id = $1 AND type = 'bounced'`, ids["550@sink.test"]).
		Scan(&code, &enhanced, &class, &mx, &response)
	if err != nil {
		t.Fatal(err)
	}
	if code != 550 || enhanced != "5.1.1" || class != "InvalidRecipient" || mx != "smtpsink" || !strings.Contains(response, "does not exist") {
		t.Errorf("bounce event: %d %s %s %s %q", code, enhanced, class, mx, response)
	}
}

// TestIngestOrderDoesNotMatter stores the same session backwards, one record
// per batch: the worst order KumoMTA could choose.
func TestIngestOrderDoesNotMatter(t *testing.T) {
	e := newEnv(t)
	records := loadSession(t)
	ids := e.restoreSession(t, records)
	svc := e.ingest(t)

	reversed := slices.Clone(records)
	slices.Reverse(reversed)
	for i := range reversed {
		if _, err := svc.Process(context.Background(), reversed[i:i+1], nil); err != nil {
			t.Fatal(err)
		}
	}
	// Same statuses and attempt counts. The histories hold the same events;
	// they are listed by time, so their order is the same too.
	e.checkSession(t, ids)
}

// TestIngestConcurrentReplicas has several ingest replicas receive
// overlapping batches at once, as happens when KumoMTA retries while a slow
// request is still running.
func TestIngestConcurrentReplicas(t *testing.T) {
	e := newEnv(t)
	records := loadSession(t)
	ids := e.restoreSession(t, records)
	svc := e.ingest(t)

	var wg sync.WaitGroup
	for replica := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := slices.Clone(records)
			if replica%2 == 1 {
				slices.Reverse(batch)
			}
			if _, err := svc.Process(context.Background(), batch, nil); err != nil {
				t.Errorf("replica %d: %v", replica, err)
			}
		}()
	}
	wg.Wait()
	e.checkSession(t, ids)
	if n := e.count(t, "SELECT count(*) FROM events WHERE type <> 'accepted'"); n != 25 {
		t.Errorf("%d events, want 25: a record was stored twice or lost", n)
	}
}

func TestIngestForgedBounceChangesNothing(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	id := e.sendOne(t, "victim@example.org")

	// An attacker who knows a message id, but not the signing key.
	forged := kumo.Record{
		Type:      kumo.TypeOOB,
		ID:        "0123456789abcdef",
		Sender:    "b+" + domain.CompactID(id) + ".aaaaaaaa@bounce.acme.test",
		Recipient: kumo.Recipients{"victim@example.org"},
		Timestamp: time.Now().Unix(),
		Created:   time.Now().Unix(),
	}
	stats, err := e.ingest(t).Process(ctx, []kumo.Record{forged}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Unverified != 1 || stats.Inserted != 0 {
		t.Errorf("stats = %+v", stats)
	}
	if status, _ := e.status(t, id); status != domain.StatusAccepted {
		t.Errorf("a forged bounce moved the status to %s", status)
	}

	// The same bounce with the real signature is taken into account.
	forged.Sender = signer(t).ReturnPath(id, "bounce.acme.test")
	if stats, err = e.ingest(t).Process(ctx, []kumo.Record{forged}, nil); err != nil || stats.Inserted != 1 {
		t.Fatalf("genuine bounce: %+v, %v", stats, err)
	}
	if status, _ := e.status(t, id); status != domain.StatusBounced {
		t.Errorf("status = %s", status)
	}
}

func TestIngestParksWhatItCannotDecode(t *testing.T) {
	e := newEnv(t)
	body := []byte(`[{"type":"Delivery","num_attempts":"three","id":"abc"}]`)
	records, invalid, err := kumo.DecodeBatch(body)
	if err != nil || len(invalid) != 1 {
		t.Fatalf("invalid=%d err=%v", len(invalid), err)
	}
	stats, err := e.ingest(t).Process(context.Background(), records, invalid)
	if err != nil || stats.Invalid != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	var kept string
	if err := e.admin.QueryRow(context.Background(), "SELECT record->>'id' FROM kumo_dead_records").Scan(&kept); err != nil || kept != "abc" {
		t.Errorf("parked record: %q, %v", kept, err)
	}
}

func TestIngestIgnoresUnknownMessages(t *testing.T) {
	e := newEnv(t)
	records := loadSession(t) // none of these messages exists in this database
	e.ensureWeek(t, records[0].OccurredAt())
	stats, err := e.ingest(t).Process(context.Background(), records, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Inserted != 0 || stats.Ignored != len(records) {
		t.Errorf("stats = %+v", stats)
	}
}
