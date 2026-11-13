// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package kumo

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testInjection() Injection {
	return Injection{
		EnvelopeSender: "b+0123.abcdefgh@bounce.acme.test",
		Recipient:      "user@example.org",
		Content:        []byte("From: a@acme.test\r\nSubject: {{ not a template }}\r\n\r\nhello\r\n"),
		PostierID:      "01a1d4d8-46d8-71ef-a51c-25a88481b45a",
		Tenant:         "tenant-1",
		Trace:          "trace-1",
	}
}

// fakeKumo answers like the injection API. The response bodies are the ones
// the pinned KumoMTA version returned on the test bench.
func fakeKumo(t *testing.T, status int, body string, got *injectRequest) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/inject/v1" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "postier" || pass != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if got != nil {
			raw, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(raw, got); err != nil {
				t.Errorf("request body: %v", err)
			}
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL+"/", "postier", "secret", 2)
}

const injectOK = `{"success_count":1,"fail_count":0,"failed_recipients":[],"errors":[]}`

func TestInjectSendsWhatThePolicyExpects(t *testing.T) {
	var got injectRequest
	c := fakeKumo(t, http.StatusOK, injectOK, &got)
	in := testInjection()
	if err := c.Inject(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if got.TemplateDialect != "Static" {
		t.Errorf("template_dialect = %q: customer content would be expanded as a template", got.TemplateDialect)
	}
	if got.Content != string(in.Content) || got.EnvelopeSender != in.EnvelopeSender {
		t.Errorf("content or envelope sender altered: %+v", got)
	}
	if len(got.Recipients) != 1 || got.Recipients[0].Email != in.Recipient {
		t.Fatalf("recipients = %+v", got.Recipients)
	}
	// The keys the Lua policy reads from the `extra` metadata.
	meta := got.Recipients[0].Metadata
	if meta["postier_id"] != in.PostierID || meta["tenant"] != in.Tenant || meta["trace"] != in.Trace {
		t.Errorf("metadata = %v", meta)
	}
	if got.TraceHeaders.SupplementalHeader || got.TraceHeaders.ReceivedHeader {
		t.Errorf("trace headers enabled: %+v", got.TraceHeaders)
	}
}

func TestInjectClassifiesFailures(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		rejected bool
	}{
		{
			name:     "policy refuses permanently",
			status:   http.StatusOK,
			body:     `{"success_count":0,"fail_count":1,"failed_recipients":["user@example.org"],"errors":["user@example.org: 550 5.7.1 injection without a postier_id\nstack traceback:\n\t[C]: in function 'kumo.reject'\n"]}`,
			rejected: true,
		},
		{
			name:   "policy defers: the data file lags behind",
			status: http.StatusOK,
			body:   `{"success_count":0,"fail_count":1,"failed_recipients":["user@example.org"],"errors":["user@example.org: 451 4.7.1 sending domain not configured yet: other.test\nstack traceback:\n"]}`,
		},
		{name: "failure without a code", status: http.StatusOK, body: `{"success_count":0,"fail_count":1,"failed_recipients":[],"errors":["something broke"]}`},
		{name: "empty answer", status: http.StatusOK, body: `{"success_count":0,"fail_count":0,"failed_recipients":[],"errors":[]}`},
		{name: "not json", status: http.StatusOK, body: `<html>`},
		{name: "malformed request", status: http.StatusBadRequest, body: "bad", rejected: true},
		{name: "unprocessable", status: http.StatusUnprocessableEntity, body: "bad", rejected: true},
		{name: "server error", status: http.StatusInternalServerError, body: "boom"},
		{name: "overloaded", status: http.StatusServiceUnavailable, body: "busy"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := fakeKumo(t, tc.status, tc.body, nil).Inject(context.Background(), testInjection())
			if err == nil {
				t.Fatal("no error")
			}
			if got := errors.Is(err, ErrRejected); got != tc.rejected {
				t.Fatalf("rejected = %v, want %v (%v)", got, tc.rejected, err)
			}
			if strings.Contains(err.Error(), "stack traceback") {
				t.Errorf("Lua traceback leaked into the error: %v", err)
			}
		})
	}
}

// A wrong password is an operator error: it must be retried, never turned
// into a rejected message.
func TestInjectDoesNotRejectOnAuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	err := NewClient(srv.URL, "postier", "wrong", 1).Inject(context.Background(), testInjection())
	if err == nil || errors.Is(err, ErrRejected) {
		t.Fatalf("err = %v", err)
	}
}

func TestInjectFailsWhenUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	err := NewClient(srv.URL, "postier", "secret", 1).Inject(context.Background(), testInjection())
	if err == nil || errors.Is(err, ErrRejected) {
		t.Fatalf("err = %v", err)
	}
}

func TestInjectRefusesNonUTF8Content(t *testing.T) {
	c := fakeKumo(t, http.StatusOK, injectOK, nil)
	in := testInjection()
	in.Content = []byte{'h', 'i', 0xff, 0xfe}
	if err := c.Inject(context.Background(), in); !errors.Is(err, ErrRejected) {
		t.Fatalf("err = %v", err)
	}
}

// TestBenchDataFileMatchesTheFormat keeps the hand-written data file of the
// test bench in line with the type the policy data is defined by.
func TestBenchDataFileMatchesTheFormat(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "compose", "kumo", "data.json"))
	if err != nil {
		t.Fatal(err)
	}
	var data PolicyData
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&data); err != nil {
		t.Fatalf("data.json has a field the format does not define: %v", err)
	}
	rendered, err := data.Render()
	if err != nil {
		t.Fatal(err)
	}
	if string(rendered) != string(raw) {
		t.Errorf("data.json is not in canonical form; Render gives:\n%s", rendered)
	}
}

func TestPolicyDataValidate(t *testing.T) {
	valid := func() PolicyData {
		return PolicyData{
			Version: PolicyDataVersion, Hostname: "mta.example.org",
			LogHook: LogHook{URL: "http://ingest:8081/v1/kumo/records"},
			Domains: map[string]DomainData{"acme.example": {Selector: "pst1", KeyFile: "/keys/acme.key"}},
		}
	}
	d := valid()
	if err := d.Validate(); err != nil {
		t.Fatalf("valid data refused: %v", err)
	}
	broken := map[string]func(*PolicyData){
		"version":  func(d *PolicyData) { d.Version = 2 },
		"hostname": func(d *PolicyData) { d.Hostname = "" },
		"hook url": func(d *PolicyData) { d.LogHook.URL = "" },
		"batch":    func(d *PolicyData) { d.LogHook.BatchSize = -1 },
		"selector": func(d *PolicyData) { d.Domains["acme.example"] = DomainData{KeyFile: "/k"} },
		"key file": func(d *PolicyData) { d.Domains["acme.example"] = DomainData{Selector: "s"} },
		"route":    func(d *PolicyData) { d.Routes = map[string]Route{"*": {}} },
	}
	for name, breakIt := range broken {
		d := valid()
		breakIt(&d)
		if err := d.Validate(); err == nil {
			t.Errorf("%s: invalid data accepted", name)
		}
	}
}

// TestChartShipsTheSamePolicy: Helm can only read files inside the chart, so
// the chart carries a copy of the policy. `make generate` refreshes it; this
// test fails when someone forgets.
func TestChartShipsTheSamePolicy(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "kumo", "policy", "init.lua"))
	if err != nil {
		t.Fatal(err)
	}
	copied, err := os.ReadFile(filepath.Join("..", "..", "deploy", "helm", "postier", "files", "init.lua"))
	if err != nil {
		t.Fatal(err)
	}
	if string(source) != string(copied) {
		t.Fatal("deploy/helm/postier/files/init.lua differs from kumo/policy/init.lua: run `make generate`")
	}
}
