// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/store"
)

func codeOf(err error) domain.ErrorCode {
	var de *domain.Error
	if errors.As(err, &de) {
		return de.Code
	}
	return ""
}

func TestMigrateIsIdempotent(t *testing.T) {
	e := newEnv(t)
	// newEnv migrated once. Running the same steps again must be a no-op.
	cfg := e.admin.Config().ConnConfig
	if err := store.Migrate(context.Background(), cfg.ConnString(), quiet); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	// Last week to four weeks ahead, for messages and for events.
	if n := e.count(t, `SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
	                    WHERE p.relname IN ('messages', 'events')`); n != 12 {
		t.Errorf("%d partitions, want 12", n)
	}
}

// TestAcceptedMeansPersisted: one request, one transaction, everything in it.
func TestAcceptedMeansPersisted(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	res, replayed, err := e.service().Send(ctx, e.principal,
		validRequest("alice@example.org", "Bob@Example.ORG", "carol@xn--bcher-kva.example"), apiOpts(""))
	if err != nil || replayed {
		t.Fatalf("Send: replayed=%v err=%v", replayed, err)
	}
	if len(res.Messages) != 3 {
		t.Fatalf("%d messages, want one per recipient", len(res.Messages))
	}
	if res.Messages[1].To != "bob@example.org" {
		t.Errorf("recipient not normalised: %q", res.Messages[1].To)
	}

	if n := e.count(t, "SELECT count(*) FROM messages WHERE batch_id = $1 AND status = 'accepted'", res.BatchID); n != 3 {
		t.Errorf("%d messages stored", n)
	}
	// One body for the three recipients.
	if n := e.count(t, "SELECT count(DISTINCT content_id) FROM messages WHERE batch_id = $1", res.BatchID); n != 1 {
		t.Errorf("%d bodies stored for one send", n)
	}
	if n := e.count(t, "SELECT count(*) FROM message_contents"); n != 1 {
		t.Errorf("%d rows in message_contents", n)
	}
	// The outbox: one injection job per message, committed with it.
	if n := e.count(t, "SELECT count(*) FROM river_job WHERE kind = 'postier.inject' AND state = 'available'"); n != 3 {
		t.Errorf("%d injection jobs", n)
	}
	for _, m := range res.Messages {
		if got := e.eventTypes(t, m.ID); got != "accepted" {
			t.Errorf("%s: events = %q", m.To, got)
		}
		// The partition key is the timestamp of the id: a lookup by id alone
		// can prune.
		at, _ := domain.IDTime(m.ID)
		if n := e.count(t, "SELECT count(*) FROM messages WHERE id = $1 AND created_at = $2", m.ID, at); n != 1 {
			t.Errorf("%s: created_at is not the timestamp of its id", m.To)
		}
	}
	// Addresses are stored with their hash, the key of future suppressions.
	want, _ := domain.ParseAddress("bob@example.org")
	hash := want.Hash()
	if n := e.count(t, "SELECT count(*) FROM messages WHERE rcpt_hash = $1", hash[:]); n != 1 {
		t.Error("rcpt_hash is not the hash of the normalised address")
	}
}

// failingJobs makes the outbox insert fail, after the messages were written.
type failingJobs struct{}

func (failingJobs) EnqueueInjections(context.Context, pgx.Tx, []uuid.UUID) error {
	return errors.New("queue unavailable")
}

// TestNoJobNoMessage: if the injection job cannot be written, the messages
// are not either. A message that nobody will ever inject must not exist.
func TestNoJobNoMessage(t *testing.T) {
	e := newEnv(t)
	svc := accept.NewService(e.app, failingJobs{}, accept.DefaultLimits(), quiet)

	_, _, err := svc.Send(context.Background(), e.principal, validRequest("alice@example.org"), apiOpts("key-1"))
	if err == nil {
		t.Fatal("Send succeeded without an injection job")
	}
	for _, table := range []string{"messages", "events", "message_contents", "idempotency_keys"} {
		if n := e.count(t, "SELECT count(*) FROM "+table); n != 0 {
			t.Errorf("%d rows left in %s after a failed send", n, table)
		}
	}
	// The key was released with the transaction: the client's retry works.
	if _, _, err := e.service().Send(context.Background(), e.principal, validRequest("alice@example.org"), apiOpts("key-1")); err != nil {
		t.Fatalf("retry with the same key: %v", err)
	}
}

func TestIdempotency(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.service()

	first, replayed, err := svc.Send(ctx, e.principal, validRequest("alice@example.org"), apiOpts("order-48213"))
	if err != nil || replayed {
		t.Fatalf("first: replayed=%v err=%v", replayed, err)
	}
	second, replayed, err := svc.Send(ctx, e.principal, validRequest("alice@example.org"), apiOpts("order-48213"))
	if err != nil || !replayed {
		t.Fatalf("second: replayed=%v err=%v", replayed, err)
	}
	if second.BatchID != first.BatchID || second.Messages[0].ID != first.Messages[0].ID {
		t.Errorf("replay returned another batch: %+v vs %+v", second, first)
	}
	if n := e.count(t, "SELECT count(*) FROM messages"); n != 1 {
		t.Errorf("%d messages after a replay", n)
	}

	// Same key, another body.
	_, _, err = svc.Send(ctx, e.principal, validRequest("mallory@example.org"), apiOpts("order-48213"))
	if codeOf(err) != domain.CodeIdempotencyConflict {
		t.Errorf("different body: %v", err)
	}
	// No key: no deduplication.
	if _, replayed, err := svc.Send(ctx, e.principal, validRequest("alice@example.org"), apiOpts("")); err != nil || replayed {
		t.Errorf("without key: replayed=%v err=%v", replayed, err)
	}
	// Keys are per project.
	other := e.principal
	other.ProjectID = domain.NewID()
	if _, _, err := svc.Send(ctx, other, validRequest("alice@example.org"), apiOpts("order-48213")); codeOf(err) != domain.CodeDomainNotAllowed {
		t.Errorf("another project reusing the key: %v", err)
	}
}

// TestConcurrentIdempotency: a client that times out and retries while its
// first request is still running. Twenty requests race with one key; exactly
// one batch may come out, and every caller must get it.
func TestConcurrentIdempotency(t *testing.T) {
	e := newEnv(t)
	svc := e.service()

	const racers = 20
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		batches = map[uuid.UUID]int{}
		fresh   int
	)
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, replayed, err := svc.Send(context.Background(), e.principal,
				validRequest("alice@example.org", "bob@example.org"), apiOpts("race"))
			if err != nil {
				t.Errorf("Send: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			batches[res.BatchID]++
			if !replayed {
				fresh++
			}
		}()
	}
	wg.Wait()

	if len(batches) != 1 || fresh != 1 {
		t.Errorf("%d distinct batches, %d non-replayed answers; want 1 and 1", len(batches), fresh)
	}
	if n := e.count(t, "SELECT count(*) FROM messages"); n != 2 {
		t.Errorf("%d messages, want 2", n)
	}
	if n := e.count(t, "SELECT count(*) FROM river_job WHERE kind = 'postier.inject'"); n != 2 {
		t.Errorf("%d injection jobs, want 2", n)
	}
}

func TestSendingDomainIsEnforced(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.service()
	e.addDomain(t, "pending.test", nil)

	req := validRequest("alice@example.org")
	req.From = "no-reply@pending.test"
	if _, _, err := svc.Send(ctx, e.principal, req, apiOpts("")); codeOf(err) != domain.CodeDomainNotVerified {
		t.Errorf("unverified domain: %v", err)
	}
	req.From = "no-reply@unknown.test"
	if _, _, err := svc.Send(ctx, e.principal, req, apiOpts("")); codeOf(err) != domain.CodeDomainNotAllowed {
		t.Errorf("unknown domain: %v", err)
	}
	// A subdomain is another domain.
	req.From = "no-reply@mail.acme.test"
	if _, _, err := svc.Send(ctx, e.principal, req, apiOpts("")); codeOf(err) != domain.CodeDomainNotAllowed {
		t.Errorf("subdomain: %v", err)
	}
	// The case of the domain does not matter.
	req.From = "no-reply@ACME.test"
	if _, _, err := svc.Send(ctx, e.principal, req, apiOpts("")); err != nil {
		t.Errorf("case-insensitive match: %v", err)
	}

	readOnly := e.principal
	readOnly.Scopes = []string{accept.ScopeRead}
	if _, _, err := svc.Send(ctx, readOnly, validRequest("alice@example.org"), apiOpts("")); codeOf(err) != domain.CodeForbidden {
		t.Errorf("read-only key: %v", err)
	}
	if n := e.count(t, "SELECT count(*) FROM messages"); n != 1 {
		t.Errorf("%d messages stored, want 1", n)
	}
}

func TestValidation(t *testing.T) {
	e := newEnv(t)
	svc := e.service()
	long := make([]string, 51)
	for i := range long {
		long[i] = "user" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + "@example.org"
	}
	cases := map[string]func(*accept.Request){
		"no recipient":        func(r *accept.Request) { r.To = nil },
		"too many recipients": func(r *accept.Request) { r.To = long },
		"bad recipient":       func(r *accept.Request) { r.To = []string{"not-an-address"} },
		"duplicate recipient": func(r *accept.Request) { r.To = []string{"a@example.org", "A@example.org"} },
		"no subject":          func(r *accept.Request) { r.Subject = "  " },
		"multi-line subject":  func(r *accept.Request) { r.Subject = "a\r\nBcc: x@example.org" },
		"no body":             func(r *accept.Request) { r.HTML, r.Text = "", "" },
		"reserved header":     func(r *accept.Request) { r.Headers = map[string]string{"Message-ID": "<x@y>"} },
		"header injection":    func(r *accept.Request) { r.Headers = map[string]string{"X-A": "b\r\nBcc: x@example.org"} },
		"bad tag":             func(r *accept.Request) { r.Tags = []string{"has space"} },
		"bad from":            func(r *accept.Request) { r.From = "Acme" },
	}
	for name, breakIt := range cases {
		req := validRequest("alice@example.org")
		breakIt(&req)
		if _, _, err := svc.Send(context.Background(), e.principal, req, apiOpts("")); codeOf(err) != domain.CodeInvalidRequest {
			t.Errorf("%s: %v", name, err)
		}
	}
	if n := e.count(t, "SELECT count(*) FROM messages"); n != 0 {
		t.Errorf("%d messages stored from invalid requests", n)
	}
}

// TestAppRoleCannotRewriteHistory: the role of the entry points can add, not
// alter. A bug or an injection in the API cannot change a status.
func TestAppRoleCannotRewriteHistory(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, _, err := e.service().Send(ctx, e.principal, validRequest("alice@example.org"), apiOpts("")); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"UPDATE messages SET status = 'delivered'",
		"DELETE FROM messages",
		"DELETE FROM events",
		"UPDATE sending_domains SET verified_at = now()",
		"INSERT INTO tenants (id, name) VALUES (gen_random_uuid(), 'x')",
		"SELECT postier_ensure_partitions(1)",
	} {
		if _, err := e.app.Raw().Exec(ctx, stmt); err == nil {
			t.Errorf("postier_app was allowed to run: %s", stmt)
		}
	}
	// The system role can, and is the only one that creates partitions.
	if _, err := e.system.Raw().Exec(ctx, "SELECT postier_ensure_partitions(4)"); err != nil {
		t.Errorf("postier_system cannot ensure partitions: %v", err)
	}
	if _, err := e.system.Raw().Exec(ctx, "CREATE TABLE sneaky (id int)"); err == nil {
		t.Error("postier_system was allowed to create a table")
	}
}
