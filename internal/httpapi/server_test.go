// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/httpapi/gen"
)

type fakeAuth struct{ err error }

func (f fakeAuth) Authenticate(_ context.Context, token string) (accept.Principal, error) {
	if f.err != nil {
		return accept.Principal{}, f.err
	}
	if token != "good" {
		return accept.Principal{}, domain.Errorf(domain.CodeUnauthorized, "missing or invalid API key")
	}
	return accept.Principal{TenantID: domain.NewID(), ProjectID: domain.NewID(), Scopes: []string{accept.ScopeSend}}, nil
}

type fakeSender struct {
	req      accept.Request
	opts     accept.Options
	calls    int
	replayed bool
	err      error
}

func (f *fakeSender) Send(_ context.Context, _ accept.Principal, req accept.Request, opts accept.Options) (accept.Result, bool, error) {
	f.calls++
	f.req, f.opts = req, opts
	if f.err != nil {
		return accept.Result{}, false, f.err
	}
	res := accept.Result{BatchID: domain.NewID()}
	for _, to := range req.To {
		res.Messages = append(res.Messages, accept.Accepted{ID: domain.NewID(), To: to, Status: domain.StatusAccepted})
	}
	return res, f.replayed, nil
}

func newTestServer(sender *fakeSender, auth fakeAuth) http.Handler {
	return New(auth, sender, nil, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler()
}

const validBody = `{"from":"Acme <no-reply@acme.test>","to":["a@example.org","b@example.org"],"subject":"Hi","text":"hello","tags":["t"],"metadata":{"k":"v"}}`

func post(h http.Handler, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/emails", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer good")
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func problemOf(t *testing.T, rec *httptest.ResponseRecorder) gen.Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json (body %s)", ct, rec.Body)
	}
	var p gen.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("problem body: %v: %s", err, rec.Body)
	}
	if p.Status != rec.Code {
		t.Errorf("problem status %d, HTTP status %d", p.Status, rec.Code)
	}
	if !strings.HasPrefix(p.Type, "urn:postier:problem:") || p.Title == "" {
		t.Errorf("problem = %+v", p)
	}
	return p
}

func TestSendEmail(t *testing.T) {
	sender := &fakeSender{}
	rec := post(newTestServer(sender, fakeAuth{}), validBody, map[string]string{"Idempotency-Key": "order-1"})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var out gen.SendEmailResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Messages) != 2 || out.Messages[0].Status != "accepted" || out.Messages[1].To != "b@example.org" {
		t.Errorf("response = %+v", out)
	}
	if rec.Header().Get("Idempotent-Replayed") != "" {
		t.Error("a first answer is flagged as replayed")
	}
	if sender.req.From != "Acme <no-reply@acme.test>" || sender.req.Text != "hello" || sender.req.Metadata["k"] != "v" {
		t.Errorf("request passed on = %+v", sender.req)
	}
	if sender.opts.IdempotencyKey != "order-1" || sender.opts.Source != accept.SourceAPI {
		t.Errorf("options = %+v", sender.opts)
	}
	// The trace id of the response is the one that follows the message.
	if id := rec.Header().Get("X-Trace-Id"); len(id) != 32 || id != sender.opts.TraceID {
		t.Errorf("X-Trace-Id %q, passed on %q", id, sender.opts.TraceID)
	}
}

func TestReplayIsFlagged(t *testing.T) {
	rec := post(newTestServer(&fakeSender{replayed: true}, fakeAuth{}), validBody, nil)
	if rec.Code != http.StatusAccepted || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Errorf("status %d, Idempotent-Replayed %q", rec.Code, rec.Header().Get("Idempotent-Replayed"))
	}
}

func TestCallerTraceIsKept(t *testing.T) {
	sender := &fakeSender{}
	h := newTestServer(sender, fakeAuth{})
	const trace = "4bf92f3577b34da6a3ce929d0e0e4736"
	post(h, validBody, map[string]string{"traceparent": "00-" + trace + "-00f067aa0ba902b7-01"})
	if sender.opts.TraceID != trace {
		t.Errorf("trace = %q", sender.opts.TraceID)
	}
	// Anything else is replaced, never trusted or echoed.
	for _, bad := range []string{"garbage", "00-" + strings.Repeat("0", 32) + "-00f067aa0ba902b7-01", "00-XYZ-1-01"} {
		post(h, validBody, map[string]string{"traceparent": bad})
		if len(sender.opts.TraceID) != 32 || sender.opts.TraceID == trace || strings.Contains(bad, sender.opts.TraceID) {
			t.Errorf("traceparent %q gave trace %q", bad, sender.opts.TraceID)
		}
	}
}

func TestAuthentication(t *testing.T) {
	sender := &fakeSender{}
	h := newTestServer(sender, fakeAuth{})
	for name, header := range map[string]string{
		"missing":      "",
		"wrong scheme": "Basic Z29vZA==",
		"wrong key":    "Bearer bad",
		"no token":     "Bearer",
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/emails", strings.NewReader(validBody))
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status %d", name, rec.Code)
			continue
		}
		if p := problemOf(t, rec); p.Code != "unauthorized" {
			t.Errorf("%s: code %q", name, p.Code)
		}
		if rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: no WWW-Authenticate header", name)
		}
	}
	if sender.calls != 0 {
		t.Errorf("%d sends without authentication", sender.calls)
	}

	// The scheme is case-insensitive (RFC 9110).
	req := httptest.NewRequest(http.MethodPost, "/v1/emails", strings.NewReader(validBody))
	req.Header.Set("Authorization", "bearer good")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Errorf("lowercase scheme: status %d", rec.Code)
	}
}

func TestMalformedBodies(t *testing.T) {
	sender := &fakeSender{}
	h := newTestServer(sender, fakeAuth{})
	for name, body := range map[string]string{
		"not json":         `{"from":`,
		"empty":            ``,
		"wrong type":       `{"from":"a@acme.test","to":"b@example.org","subject":"s","text":"t"}`,
		"unknown field":    `{"from":"a@acme.test","to":["b@example.org"],"subject":"s","text":"t","bcc":["x@example.org"]}`,
		"trailing content": validBody + `{"again":true}`,
		"array":            `[]`,
	} {
		rec := post(h, body, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d: %s", name, rec.Code, rec.Body)
			continue
		}
		if p := problemOf(t, rec); p.Code != "invalid_request" {
			t.Errorf("%s: code %q", name, p.Code)
		}
	}
	if sender.calls != 0 {
		t.Errorf("%d sends from malformed bodies", sender.calls)
	}
}

func TestDomainErrorsBecomeProblems(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
		field  string
	}{
		{domain.FieldErrorf("to[0]", "invalid email address"), 422, "invalid_request", "to[0]"},
		{domain.Errorf(domain.CodeForbidden, "no"), 403, "forbidden", ""},
		{domain.Errorf(domain.CodeDomainNotAllowed, "no"), 403, "domain_not_allowed", ""},
		{domain.Errorf(domain.CodeDomainNotVerified, "no"), 403, "domain_not_verified", ""},
		{domain.Errorf(domain.CodeIdempotencyConflict, "no"), 409, "idempotency_conflict", ""},
		{domain.Errorf(domain.CodePayloadTooLarge, "no"), 413, "payload_too_large", ""},
	}
	for _, tc := range cases {
		rec := post(newTestServer(&fakeSender{err: tc.err}, fakeAuth{}), validBody, nil)
		if rec.Code != tc.status {
			t.Errorf("%s: status %d, want %d", tc.code, rec.Code, tc.status)
			continue
		}
		p := problemOf(t, rec)
		if string(p.Code) != tc.code {
			t.Errorf("code %q, want %q", p.Code, tc.code)
		}
		if tc.field != "" && (p.Field == nil || *p.Field != tc.field) {
			t.Errorf("%s: field = %v", tc.code, p.Field)
		}
		if p.TraceId == nil || len(*p.TraceId) != 32 {
			t.Errorf("%s: no trace id to quote", tc.code)
		}
	}
}

// An unexpected error says nothing about the internals.
func TestInternalErrorsAreOpaque(t *testing.T) {
	secret := errors.New(`pq: password authentication failed for user "postier_app" at 10.0.3.7`)
	for name, h := range map[string]http.Handler{
		"sender": newTestServer(&fakeSender{err: secret}, fakeAuth{}),
		"auth":   newTestServer(&fakeSender{}, fakeAuth{err: secret}),
	} {
		rec := post(h, validBody, nil)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("%s: status %d", name, rec.Code)
		}
		if p := problemOf(t, rec); p.Code != "internal_error" {
			t.Errorf("%s: code %q", name, p.Code)
		}
		if strings.Contains(rec.Body.String(), "postier_app") || strings.Contains(rec.Body.String(), "10.0.3.7") {
			t.Errorf("%s: internals leaked: %s", name, rec.Body)
		}
	}
}

func TestUnknownRoutesAndBadIDs(t *testing.T) {
	h := newTestServer(&fakeSender{}, fakeAuth{})
	for path, want := range map[string]int{
		"/":                     404,
		"/v1/nope":              404,
		"/v1/emails/not-a-uuid": 400,
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer good")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("GET %s: status %d, want %d", path, rec.Code, want)
			continue
		}
		problemOf(t, rec)
	}
}

func TestSpecIsPublished(t *testing.T) {
	rec := httptest.NewRecorder()
	newTestServer(&fakeSender{}, fakeAuth{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "openapi: 3.0.3") {
		t.Errorf("status %d", rec.Code)
	}
}
