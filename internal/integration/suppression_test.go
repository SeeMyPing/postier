// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/dispatch"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/httpapi"
	"github.com/SeeMyPing/postier/internal/httpapi/gen"
	"github.com/SeeMyPing/postier/internal/kumo"
	"github.com/SeeMyPing/postier/internal/queue"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
	"github.com/SeeMyPing/postier/internal/suppression"
)

// suppressionOf returns the reason and scope of the active suppression of an
// address, or "" when there is none.
func (e *env) suppressionOf(t *testing.T, addr string) string {
	t.Helper()
	a, err := domain.ParseAddress(addr)
	if err != nil {
		t.Fatal(err)
	}
	h := a.Hash()
	var reason string
	var tenantWide, permanent bool
	err = e.admin.QueryRow(context.Background(), `
		SELECT reason, project_id IS NULL, expires_at IS NULL FROM suppressions
		WHERE addr_hash = $1 AND (expires_at IS NULL OR expires_at > now())`, h[:]).
		Scan(&reason, &tenantWide, &permanent)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	scope, life := "project", "temporary"
	if tenantWide {
		scope = "tenant"
	}
	if permanent {
		life = "permanent"
	}
	return fmt.Sprintf("%s/%s/%s", reason, scope, life)
}

// bounce feeds ingest a final failure for a message, the way KumoMTA would.
func (e *env) bounce(t *testing.T, id uuid.UUID, typ, class string, code int) {
	t.Helper()
	now := time.Now().UTC()
	raw, _ := json.Marshal(id.String())
	rec := kumo.Record{
		Type: typ, ID: uuid.NewString(), Recipient: kumo.Recipients{"x@example.org"},
		Timestamp: now.Unix(), EventTime: &now, BounceClassification: class,
		Response: kumo.Response{Code: code},
		Meta:     map[string]json.RawMessage{kumo.MetaPostierID: raw},
	}
	if _, err := e.ingest(t).Process(context.Background(), []kumo.Record{rec}, nil); err != nil {
		t.Fatal(err)
	}
}

// TestRealSessionFeedsTheSuppressionList: the captured session holds a hard
// bounce, an asynchronous one and a complaint. Each lands on the list with
// the right scope, once, however many times the records are replayed.
func TestRealSessionFeedsTheSuppressionList(t *testing.T) {
	e := newEnv(t)
	records := loadSession(t)
	e.restoreSession(t, records)
	svc := e.ingest(t)

	first, err := svc.Process(context.Background(), records, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.Suppressed != 3 {
		t.Errorf("first pass suppressed %d addresses, want 3", first.Suppressed)
	}
	if _, err := svc.Process(context.Background(), records, nil); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{
		"550@sink.test":     "hard_bounce/project/permanent",
		"oob-cap@sink.test": "hard_bounce/project/permanent",
		// A complaint silences every project of the tenant.
		"fbl-cap@sink.test": "complaint/tenant/permanent",
		// Delivered, cancelled by an operator, or expired without the
		// mailbox being at fault: nothing against these addresses.
		"ok@sink.test":            "",
		"defer2-cap@sink.test":    "",
		"421@sink.test":           "",
		"defer99-admin@sink.test": "",
	}
	for addr, expected := range want {
		if got := e.suppressionOf(t, addr); got != expected {
			t.Errorf("%s: %q, want %q", addr, got, expected)
		}
	}
	if n := e.count(t, "SELECT count(*) FROM suppressions"); n != 3 {
		t.Errorf("%d suppressions after a replay, want 3", n)
	}
}

// TestSenderSideFailureDoesNotSuppress replays what happened on the first
// real send: Microsoft refused a valid mailbox with a 550 because the
// sending IP was listed. The message bounces; the address stays clean.
func TestSenderSideFailureDoesNotSuppress(t *testing.T) {
	e := newEnv(t)
	id := e.sendOne(t, "valid@example.org")
	e.bounce(t, id, kumo.TypeBounce, "AuthenticationFailed", 550)

	if status, _ := e.status(t, id); status != domain.StatusBounced {
		t.Fatalf("status = %s", status)
	}
	if got := e.suppressionOf(t, "valid@example.org"); got != "" {
		t.Errorf("a valid address was suppressed: %s", got)
	}
	// The next message to it is sent normally.
	res, _, err := e.service().Send(context.Background(), e.principal, validRequest("valid@example.org"), apiOpts(""))
	if err != nil || res.Messages[0].Status != domain.StatusAccepted {
		t.Errorf("next send: %+v, %v", res.Messages, err)
	}
}

func TestRepeatedSoftBouncesSuppressForAWhile(t *testing.T) {
	e := newEnv(t)
	const addr = "full@example.org"
	for i := 1; i <= domain.SoftBounceThreshold; i++ {
		if got := e.suppressionOf(t, addr); got != "" {
			t.Fatalf("suppressed after %d soft bounces: %s", i-1, got)
		}
		e.bounce(t, e.sendOne(t, addr), kumo.TypeBounce, "QuotaIssues", 552)
	}
	if got := e.suppressionOf(t, addr); got != "soft_bounces/project/temporary" {
		t.Fatalf("after %d soft bounces: %q", domain.SoftBounceThreshold, got)
	}
	var until time.Time
	if err := e.admin.QueryRow(context.Background(), "SELECT expires_at FROM suppressions").Scan(&until); err != nil {
		t.Fatal(err)
	}
	if d := time.Until(until); d < domain.SoftBounceWindow-time.Hour || d > domain.SoftBounceWindow+time.Hour {
		t.Errorf("suppressed for %s, want about %s", d, domain.SoftBounceWindow)
	}

	// A later hard bounce makes it permanent; a soft one cannot undo that.
	if _, err := e.admin.Exec(context.Background(), "DELETE FROM suppressions"); err != nil {
		t.Fatal(err)
	}
	e.bounce(t, e.sendOne(t, addr), kumo.TypeBounce, "InvalidRecipient", 550)
	if _, err := e.admin.Exec(context.Background(), "UPDATE messages SET status = 'accepted' WHERE status = 'suppressed'"); err != nil {
		t.Fatal(err)
	}
	e.bounce(t, e.sendOne(t, "other@example.org"), kumo.TypeBounce, "QuotaIssues", 552)
	if got := e.suppressionOf(t, addr); got != "hard_bounce/project/permanent" {
		t.Errorf("after a hard bounce: %q", got)
	}
}

// TestSuppressedRecipientAtAcceptance: the request succeeds, the suppressed
// recipient gets a closed message, the others are sent.
func TestSuppressedRecipientAtAcceptance(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.bounce(t, e.sendOne(t, "gone@example.org"), kumo.TypeBounce, "InvalidRecipient", 550)
	jobs := e.count(t, "SELECT count(*) FROM river_job WHERE kind = 'postier.inject'")
	bodies := e.count(t, "SELECT count(*) FROM message_contents")

	// Another spelling of the same mailbox does not get around the list.
	res, _, err := e.service().Send(ctx, e.principal, validRequest("alice@example.org", "GONE@Example.ORG"), apiOpts(""))
	if err != nil {
		t.Fatalf("a suppressed recipient failed the request: %v", err)
	}
	if res.Messages[0].Status != domain.StatusAccepted || res.Messages[1].Status != domain.StatusSuppressed {
		t.Fatalf("statuses = %+v", res.Messages)
	}
	if got := e.eventTypes(t, res.Messages[1].ID); got != "accepted,suppressed" {
		t.Errorf("history of the suppressed message: %q", got)
	}
	if n := e.count(t, "SELECT count(*) FROM river_job WHERE kind = 'postier.inject'"); n != jobs+1 {
		t.Errorf("%d new injection jobs, want 1", n-jobs)
	}

	// Nobody left to send to: no job, and no body stored.
	res, _, err = e.service().Send(ctx, e.principal, validRequest("gone@example.org"), apiOpts(""))
	if err != nil || res.Messages[0].Status != domain.StatusSuppressed {
		t.Fatalf("all suppressed: %+v, %v", res.Messages, err)
	}
	if n := e.count(t, "SELECT count(*) FROM river_job WHERE kind = 'postier.inject'"); n != jobs+1 {
		t.Errorf("an injection job was created for a suppressed recipient")
	}
	if n := e.count(t, "SELECT count(*) FROM message_contents"); n != bodies+1 {
		t.Errorf("%d new bodies stored, want 1", n-bodies)
	}

	// Another project of the tenant is not concerned by a project-scoped
	// suppression, but is by a complaint.
	otherProject, err := sqlcgen.New(e.admin).UpsertProject(ctx, sqlcgen.UpsertProjectParams{ID: domain.NewID(), TenantID: e.tenantID, Name: "other"})
	if err != nil {
		t.Fatal(err)
	}
	gone, _ := domain.ParseAddress("gone@example.org")
	hit, err := store.SuppressedAmong(ctx, e.system.Q(), e.tenantID, otherProject, []domain.Address{gone})
	if err != nil || len(hit) != 0 {
		t.Errorf("project suppression leaked to another project: %v, %v", hit, err)
	}
	e.bounce(t, e.sendOne(t, "angry@example.org"), kumo.TypeDelivery, "", 250)
	var angry uuid.UUID
	if err := e.admin.QueryRow(ctx, "SELECT id FROM messages WHERE rcpt_addr = 'angry@example.org'").Scan(&angry); err != nil {
		t.Fatal(err)
	}
	createdAt, _ := domain.IDTime(angry)
	hash := mustAddr(t, "angry@example.org").Hash()
	if _, err := store.SuppressOnEvent(ctx, e.system.Q(), store.Recipient{
		TenantID: e.tenantID, ProjectID: e.projectID, MessageID: angry, Addr: "angry@example.org", AddrHash: hash[:],
	}, domain.SuppressForTenant, createdAt); err != nil {
		t.Fatal(err)
	}
	hit, err = store.SuppressedAmong(ctx, e.system.Q(), e.tenantID, otherProject, []domain.Address{mustAddr(t, "angry@example.org")})
	if err != nil || hit["angry@example.org"] != domain.ReasonComplaint {
		t.Errorf("a complaint does not cover the other project: %v, %v", hit, err)
	}
}

func mustAddr(t *testing.T, s string) domain.Address {
	t.Helper()
	a, err := domain.ParseAddress(s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestDispatchRechecksSuppressions: the address was suppressed after the
// message was accepted and before it was injected.
func TestDispatchRechecksSuppressions(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	waiting := e.sendOne(t, "late@example.org")
	e.bounce(t, e.sendOne(t, "late@example.org"), kumo.TypeBounce, "InvalidRecipient", 550)

	k := &fakeKumo{}
	if err := e.worker(t, k).Work(ctx, job(waiting, 1, 0)); err != nil {
		t.Fatal(err)
	}
	if k.count() != 0 {
		t.Fatalf("a message was injected for a suppressed address")
	}
	if status, _ := e.status(t, waiting); status != domain.StatusSuppressed {
		t.Errorf("status = %s", status)
	}
	if got := e.eventTypes(t, waiting); got != "accepted,suppressed" {
		t.Errorf("history = %q", got)
	}
}

// TestSuppressionAPI goes through the HTTP API, as the app role.
func TestSuppressionAPI(t *testing.T) {
	e := newEnv(t)
	srv := httptest.NewServer(httpapi.New(accept.NewAuthenticator(e.app), e.service(), suppression.NewService(e.app), e.app, quiet).Handler())
	defer srv.Close()
	key := e.issueKey(t, accept.ScopeSend, accept.ScopeRead, accept.ScopeSuppressions)

	// Add, twice: the second call returns the same entry.
	var first, again gen.Suppression
	resp := call(t, srv, http.MethodPost, "/v1/suppressions", key, `{"address":"Stop@Example.org"}`, &first)
	if resp.StatusCode != http.StatusCreated || first.Address == nil || *first.Address != "stop@example.org" ||
		first.Reason != "manual" || first.Scope != "project" || first.ExpiresAt != nil {
		t.Fatalf("POST: %d %+v", resp.StatusCode, first)
	}
	resp = call(t, srv, http.MethodPost, "/v1/suppressions", key, `{"address":"stop@example.org","expires_at":"2099-01-01T00:00:00Z"}`, &again)
	if resp.StatusCode != http.StatusOK || again.Id != first.Id || again.ExpiresAt != nil {
		t.Errorf("second POST: %d %+v (a permanent suppression must stay permanent)", resp.StatusCode, again)
	}

	// The API and the send path agree.
	var sent gen.SendEmailResponse
	call(t, srv, http.MethodPost, "/v1/emails", key,
		`{"from":"no-reply@acme.test","to":["stop@example.org"],"subject":"s","text":"t"}`, &sent)
	if len(sent.Messages) != 1 || sent.Messages[0].Status != "suppressed" {
		t.Errorf("send to a suppressed address: %+v", sent.Messages)
	}

	// List, with pagination and a filter.
	for i := range 4 {
		call(t, srv, http.MethodPost, "/v1/suppressions", key, fmt.Sprintf(`{"address":"user%d@example.org"}`, i), nil)
	}
	var page gen.SuppressionList
	call(t, srv, http.MethodGet, "/v1/suppressions?limit=3", key, "", &page)
	if len(page.Data) != 3 || page.NextCursor == nil || *page.Data[0].Address != "user3@example.org" {
		t.Fatalf("first page: %d items, cursor %v", len(page.Data), page.NextCursor)
	}
	var rest gen.SuppressionList
	call(t, srv, http.MethodGet, "/v1/suppressions?limit=3&cursor="+page.NextCursor.String(), key, "", &rest)
	if len(rest.Data) != 2 || rest.NextCursor != nil || *rest.Data[1].Address != "stop@example.org" {
		t.Errorf("second page: %d items, cursor %v", len(rest.Data), rest.NextCursor)
	}
	var one gen.SuppressionList
	call(t, srv, http.MethodGet, "/v1/suppressions?address=STOP@example.org", key, "", &one)
	if len(one.Data) != 1 || one.Data[0].Id != first.Id {
		t.Errorf("filter by address: %+v", one.Data)
	}

	// Lift it: the address can be sent to again.
	if resp := call(t, srv, http.MethodDelete, "/v1/suppressions/"+first.Id.String(), key, "", nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: %d", resp.StatusCode)
	}
	if resp := call(t, srv, http.MethodDelete, "/v1/suppressions/"+first.Id.String(), key, "", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("second DELETE: %d", resp.StatusCode)
	}
	call(t, srv, http.MethodPost, "/v1/emails", key,
		`{"from":"no-reply@acme.test","to":["stop@example.org"],"subject":"s","text":"t"}`, &sent)
	if sent.Messages[0].Status != "accepted" {
		t.Errorf("send after lifting: %s", sent.Messages[0].Status)
	}

	// Bad requests.
	for body, want := range map[string]int{
		`{"address":"nope"}`:                                              422,
		`{"address":"a@example.org","scope":"galaxy"}`:                    422,
		`{"address":"a@example.org","expires_at":"2001-01-01T00:00:00Z"}`: 422,
		`{"address":"a@example.org","scope":"tenant"}`:                    403, // needs admin
		`{"addr":"a@example.org"}`:                                        400, // unknown field
	} {
		if resp := call(t, srv, http.MethodPost, "/v1/suppressions", key, body, nil); resp.StatusCode != want {
			t.Errorf("POST %s: %d, want %d", body, resp.StatusCode, want)
		}
	}
}

func TestSuppressionAPIPermissions(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	srv := httptest.NewServer(httpapi.New(accept.NewAuthenticator(e.app), e.service(), suppression.NewService(e.app), e.app, quiet).Handler())
	defer srv.Close()

	// A key without the scope can do none of the three.
	sendOnly := e.issueKey(t, accept.ScopeSend, accept.ScopeRead)
	for _, c := range [][2]string{
		{http.MethodGet, "/v1/suppressions"},
		{http.MethodPost, "/v1/suppressions"},
		{http.MethodDelete, "/v1/suppressions/" + domain.NewID().String()},
	} {
		if resp := call(t, srv, c[0], c[1], sendOnly, `{"address":"a@example.org"}`, nil); resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s %s without the scope: %d", c[0], c[1], resp.StatusCode)
		}
	}

	// A complaint is tenant-wide: visible to a project key, but only an
	// admin key may lift it.
	hash := mustAddr(t, "angry@example.org").Hash()
	if _, err := store.SuppressOnEvent(ctx, e.system.Q(), store.Recipient{
		TenantID: e.tenantID, ProjectID: e.projectID, MessageID: domain.NewID(), Addr: "angry@example.org", AddrHash: hash[:],
	}, domain.SuppressForTenant, time.Now()); err != nil {
		t.Fatal(err)
	}
	key := e.issueKey(t, accept.ScopeSuppressions)
	var list gen.SuppressionList
	call(t, srv, http.MethodGet, "/v1/suppressions", key, "", &list)
	if len(list.Data) != 1 || list.Data[0].Scope != "tenant" || list.Data[0].Reason != "complaint" {
		t.Fatalf("list = %+v", list.Data)
	}
	path := "/v1/suppressions/" + list.Data[0].Id.String()
	if resp := call(t, srv, http.MethodDelete, path, key, "", nil); resp.StatusCode != http.StatusForbidden {
		t.Errorf("lifting a complaint without admin: %d", resp.StatusCode)
	}
	admin := e.issueKey(t, accept.ScopeAdmin)
	if resp := call(t, srv, http.MethodDelete, path, admin, "", nil); resp.StatusCode != http.StatusNoContent {
		t.Errorf("lifting a complaint with admin: %d", resp.StatusCode)
	}

	// Another tenant sees nothing of this one, and cannot delete by id.
	call(t, srv, http.MethodPost, "/v1/suppressions", key, `{"address":"mine@example.org"}`, nil)
	call(t, srv, http.MethodGet, "/v1/suppressions", key, "", &list)
	q := sqlcgen.New(e.admin)
	otherTenant, _ := q.UpsertTenant(ctx, sqlcgen.UpsertTenantParams{ID: domain.NewID(), Name: "globex"})
	otherProject, _ := q.UpsertProject(ctx, sqlcgen.UpsertProjectParams{ID: domain.NewID(), TenantID: otherTenant, Name: "default"})
	foreign, _ := accept.GenerateKey("test")
	if err := q.InsertCredential(ctx, sqlcgen.InsertCredentialParams{
		ID: domain.NewID(), TenantID: otherTenant, ProjectID: otherProject, Name: "globex",
		Prefix: foreign.Prefix, SecretHash: foreign.SecretHash, Scopes: []string{accept.ScopeAdmin},
	}); err != nil {
		t.Fatal(err)
	}
	var theirs gen.SuppressionList
	call(t, srv, http.MethodGet, "/v1/suppressions", foreign.Token, "", &theirs)
	if len(theirs.Data) != 0 {
		t.Errorf("another tenant sees %d suppressions", len(theirs.Data))
	}
	if resp := call(t, srv, http.MethodDelete, "/v1/suppressions/"+list.Data[0].Id.String(), foreign.Token, "", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("another tenant deleting by id: %d", resp.StatusCode)
	}
	if n := e.count(t, "SELECT count(*) FROM suppressions"); n != 1 {
		t.Errorf("%d suppressions left, want 1", n)
	}
}

// TestBodiesArePurgedOnceTheSendIsOver: a body outlives injection, and goes
// when the last message that needs it is closed.
func TestBodiesArePurgedOnceTheSendIsOver(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	res, _, err := e.service().Send(ctx, e.principal, validRequest("a@example.org", "b@example.org"), apiOpts(""))
	if err != nil {
		t.Fatal(err)
	}
	a, b := res.Messages[0].ID, res.Messages[1].ID
	purge := func(retention time.Duration) {
		t.Helper()
		w := &dispatch.PurgeContentsWorker{Pool: e.system, Log: quiet, Retention: retention}
		if err := w.Work(ctx, &river.Job[queue.PurgeContentsArgs]{}); err != nil {
			t.Fatal(err)
		}
	}
	bodies := func() int { return e.count(t, "SELECT count(*) FROM message_contents") }

	purge(0)
	if bodies() != 1 {
		t.Fatal("the body was purged while both messages were still to be sent")
	}
	e.bounce(t, a, kumo.TypeDelivery, "", 250)
	e.bounce(t, b, kumo.TypeTransientFailure, "", 451)
	purge(0)
	if bodies() != 1 {
		t.Fatal("the body was purged while a message was still deferred: a lost spool could not be recovered")
	}
	e.bounce(t, b, kumo.TypeDelivery, "", 250)
	purge(time.Hour)
	if bodies() != 1 {
		t.Fatal("the body was purged before the end of its retention")
	}
	purge(0)
	if bodies() != 0 {
		t.Fatal("the body was not purged although every message is delivered")
	}
	// Messages are detached, not left pointing at nothing.
	if n := e.count(t, "SELECT count(*) FROM messages WHERE content_id IS NOT NULL"); n != 0 {
		t.Errorf("%d messages still reference a purged body", n)
	}
	// A late job for a message without a body closes it instead of failing.
	if _, err := e.admin.Exec(ctx, "UPDATE messages SET status = 'accepted' WHERE id = $1", a); err != nil {
		t.Fatal(err)
	}
	k := &fakeKumo{}
	if err := e.worker(t, k).Work(ctx, job(a, 1, 0)); err != nil || k.count() != 0 {
		t.Errorf("job without a body: %d injections, %v", k.count(), err)
	}
}

func TestWaitReady(t *testing.T) {
	e := newEnv(t)
	url := e.admin.Config().ConnConfig.ConnString()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := store.WaitReady(ctx, url, quiet); err != nil {
		t.Fatalf("a migrated database is not ready: %v", err)
	}

	// A schema one migration behind what the binary needs.
	if _, err := e.admin.Exec(ctx, "UPDATE goose_db_version SET is_applied = false WHERE version_id = (SELECT max(version_id) FROM goose_db_version)"); err != nil {
		t.Fatal(err)
	}
	short, cancelShort := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShort()
	if err := store.WaitReady(short, url, quiet); err == nil {
		t.Fatal("WaitReady returned although the schema is behind")
	}
}
