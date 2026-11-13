// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/httpapi"
	"github.com/SeeMyPing/postier/internal/httpapi/gen"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

func (e *env) issueKey(t *testing.T, scopes ...string) string {
	t.Helper()
	key, err := accept.GenerateKey("test")
	if err != nil {
		t.Fatal(err)
	}
	err = sqlcgen.New(e.admin).InsertCredential(context.Background(), sqlcgen.InsertCredentialParams{
		ID: domain.NewID(), TenantID: e.tenantID, ProjectID: e.projectID, Name: "test",
		Prefix: key.Prefix, SecretHash: key.SecretHash, Scopes: scopes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return key.Token
}

func call(t *testing.T, srv *httptest.Server, method, path, key, body string, out any) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
	}
	return resp
}

// TestAPIRoundTrip goes through the real handler, the real authentication
// and the real database, as the app role.
func TestAPIRoundTrip(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	srv := httptest.NewServer(httpapi.New(accept.NewAuthenticator(e.app), e.service(), e.app, quiet).Handler())
	defer srv.Close()
	key := e.issueKey(t, accept.ScopeSend, accept.ScopeRead)

	var sent gen.SendEmailResponse
	resp := call(t, srv, http.MethodPost, "/v1/emails", key,
		`{"from":"Acme <no-reply@acme.test>","to":["alice@example.org"],"subject":"Commande","html":"<p>ok</p>","tags":["order"],"metadata":{"order_id":"48213"}}`, &sent)
	if resp.StatusCode != http.StatusAccepted || len(sent.Messages) != 1 {
		t.Fatalf("POST: status %d, %+v", resp.StatusCode, sent)
	}
	id := sent.Messages[0].Id

	var email gen.Email
	if resp := call(t, srv, http.MethodGet, "/v1/emails/"+id.String(), key, "", &email); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: status %d", resp.StatusCode)
	}
	if email.Status != "accepted" || email.To != "alice@example.org" || email.From != "no-reply@acme.test" ||
		email.Metadata["order_id"] != "48213" || len(email.Tags) != 1 || email.BatchId != sent.BatchId {
		t.Errorf("email = %+v", email)
	}

	// The status follows the events, as ingest appends them.
	createdAt, _ := domain.IDTime(id)
	code := int32(250)
	mx := "mx.example.org"
	for i, typ := range []domain.EventType{domain.EventQueued, domain.EventDelivered} {
		err := e.system.InTx(ctx, func(q *store.Queries, _ pgx.Tx) error {
			_, err := store.AppendEvent(ctx, q, store.NewEvent{
				MessageID: id, CreatedAt: createdAt, TenantID: e.tenantID, Type: typ,
				OccurredAt: createdAt.Add(time.Duration(i+1) * time.Second), DedupKey: string(typ),
				Attempts: i, SMTPCode: &code, RemoteMX: &mx, Detail: map[string]any{"response": "OK queued"},
			})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	call(t, srv, http.MethodGet, "/v1/emails/"+id.String(), key, "", &email)
	if email.Status != "delivered" || email.Attempts != 1 || !email.StatusAt.Equal(createdAt.Add(2*time.Second)) {
		t.Errorf("after delivery: %+v", email)
	}

	var events gen.EventList
	if resp := call(t, srv, http.MethodGet, "/v1/emails/"+id.String()+"/events", key, "", &events); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET events: status %d", resp.StatusCode)
	}
	if len(events.Data) != 3 || events.Data[0].Type != "accepted" || events.Data[2].Type != "delivered" {
		t.Fatalf("events = %+v", events.Data)
	}
	last := events.Data[2]
	if last.SmtpCode == nil || *last.SmtpCode != 250 || last.RemoteMx == nil || *last.RemoteMx != mx ||
		last.Response == nil || *last.Response != "OK queued" {
		t.Errorf("delivered event = %+v", last)
	}

	// A key without the read scope.
	sendOnly := e.issueKey(t, accept.ScopeSend)
	if resp := call(t, srv, http.MethodGet, "/v1/emails/"+id.String(), sendOnly, "", nil); resp.StatusCode != http.StatusForbidden {
		t.Errorf("send-only key reading: status %d", resp.StatusCode)
	}
	// An id that does not exist, and one that is not one of ours.
	for _, unknown := range []string{domain.NewID().String(), "8a6e0804-2bd0-4672-b79d-d97027f9071a"} {
		if resp := call(t, srv, http.MethodGet, "/v1/emails/"+unknown, key, "", nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("unknown id %s: status %d", unknown, resp.StatusCode)
		}
	}
}

// TestAPIDoesNotLeakAcrossProjects: a valid key of another project sees a
// 404, exactly as if the message did not exist.
func TestAPIDoesNotLeakAcrossProjects(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	srv := httptest.NewServer(httpapi.New(accept.NewAuthenticator(e.app), e.service(), e.app, quiet).Handler())
	defer srv.Close()
	id := e.sendOne(t, "alice@example.org")

	// Another tenant, with its own project and key.
	q := sqlcgen.New(e.admin)
	otherTenant, err := q.UpsertTenant(ctx, sqlcgen.UpsertTenantParams{ID: domain.NewID(), Name: "globex"})
	if err != nil {
		t.Fatal(err)
	}
	otherProject, err := q.UpsertProject(ctx, sqlcgen.UpsertProjectParams{ID: domain.NewID(), TenantID: otherTenant, Name: "default"})
	if err != nil {
		t.Fatal(err)
	}
	key, _ := accept.GenerateKey("test")
	if err := q.InsertCredential(ctx, sqlcgen.InsertCredentialParams{
		ID: domain.NewID(), TenantID: otherTenant, ProjectID: otherProject, Name: "globex",
		Prefix: key.Prefix, SecretHash: key.SecretHash, Scopes: []string{accept.ScopeSend, accept.ScopeRead},
	}); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"/v1/emails/" + id.String(), "/v1/emails/" + id.String() + "/events"} {
		if resp := call(t, srv, http.MethodGet, path, key.Token, "", nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s with another tenant's key: status %d", path, resp.StatusCode)
		}
	}
	// And it cannot send from a domain it does not own.
	var problem gen.Problem
	resp := call(t, srv, http.MethodPost, "/v1/emails", key.Token,
		`{"from":"no-reply@acme.test","to":["x@example.org"],"subject":"s","text":"t"}`, &problem)
	if resp.StatusCode != http.StatusForbidden || problem.Code != "domain_not_allowed" {
		t.Errorf("sending from another tenant's domain: %d %s", resp.StatusCode, problem.Code)
	}
}

// A revoked key stops working at once.
func TestRevokedKey(t *testing.T) {
	e := newEnv(t)
	srv := httptest.NewServer(httpapi.New(accept.NewAuthenticator(e.app), e.service(), e.app, quiet).Handler())
	defer srv.Close()
	key := e.issueKey(t, accept.ScopeSend, accept.ScopeRead)
	body := `{"from":"no-reply@acme.test","to":["x@example.org"],"subject":"s","text":"t"}`

	if resp := call(t, srv, http.MethodPost, "/v1/emails", key, body, nil); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("before revocation: status %d", resp.StatusCode)
	}
	if _, err := e.admin.Exec(context.Background(), "UPDATE api_credentials SET revoked_at = now()"); err != nil {
		t.Fatal(err)
	}
	if resp := call(t, srv, http.MethodPost, "/v1/emails", key, body, nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("after revocation: status %d", resp.StatusCode)
	}
}
