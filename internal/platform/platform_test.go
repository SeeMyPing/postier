// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SeeMyPing/postier/internal/domain"
)

func flagsWithEnv(env map[string]string) *Flags {
	f := NewFlags("test")
	f.fs.SetOutput(io.Discard)
	f.getenv = func(k string) string { return env[k] }
	return f
}

func TestEnvName(t *testing.T) {
	if got := EnvName("kumo-url"); got != "POSTIER_KUMO_URL" {
		t.Errorf("EnvName = %q", got)
	}
}

func TestFlagsPrecedence(t *testing.T) {
	var (
		url  string
		n    int
		d    time.Duration
		b    bool
		keep string
	)
	f := flagsWithEnv(map[string]string{
		"POSTIER_KUMO_URL": "http://from-env", "POSTIER_CONCURRENCY": "8",
		"POSTIER_GUARD_DELAY": "5s", "POSTIER_VERBOSE": "true",
	})
	f.String(&url, "kumo-url", "http://default", "")
	f.Int(&n, "concurrency", 16, "")
	f.Duration(&d, "guard-delay", time.Minute, "")
	f.Bool(&b, "verbose", false, "")
	f.String(&keep, "untouched", "default", "")
	// The flag wins over the environment, the environment over the default.
	if err := f.Parse([]string{"--kumo-url", "http://from-flag", "rest"}); err != nil {
		t.Fatal(err)
	}
	if url != "http://from-flag" || n != 8 || d != 5*time.Second || !b || keep != "default" {
		t.Errorf("url=%q n=%d d=%s b=%v keep=%q", url, n, d, b, keep)
	}
	if args := f.Args(); len(args) != 1 || args[0] != "rest" {
		t.Errorf("Args = %v", args)
	}
}

func TestFlagsReportInvalidEnvironment(t *testing.T) {
	var n int
	var d time.Duration
	f := flagsWithEnv(map[string]string{"POSTIER_CONCURRENCY": "many", "POSTIER_GUARD_DELAY": "soon"})
	f.Int(&n, "concurrency", 16, "")
	f.Duration(&d, "guard-delay", time.Minute, "")
	err := f.Parse(nil)
	if err == nil || !strings.Contains(err.Error(), "POSTIER_CONCURRENCY") || !strings.Contains(err.Error(), "POSTIER_GUARD_DELAY") {
		t.Fatalf("err = %v", err)
	}
}

// A secret can only come from the environment: never from argv, where `ps`
// and the pod spec would show it.
func TestSecretIsEnvironmentOnly(t *testing.T) {
	var key string
	f := flagsWithEnv(map[string]string{"POSTIER_SIGNING_KEY": "from-env"})
	f.Secret(&key, "signing-key")
	if err := f.Parse([]string{"--signing-key", "from-flag"}); err == nil {
		t.Error("a secret was accepted on the command line")
	}
	if key != "from-env" {
		t.Errorf("key = %q", key)
	}
}

func TestRequire(t *testing.T) {
	if err := Require(map[string]string{"a": "x", "b": "y"}); err != nil {
		t.Errorf("err = %v", err)
	}
	err := Require(map[string]string{"database-url": "", "kumo-url": "", "ok": "x"})
	if err == nil {
		t.Fatal("no error")
	}
	// Both names, flag and variable, in a stable order.
	want := "--database-url / POSTIER_DATABASE_URL, --kumo-url / POSTIER_KUMO_URL"
	if !strings.Contains(err.Error(), want) {
		t.Errorf("err = %v", err)
	}
}

func TestLogger(t *testing.T) {
	var buf bytes.Buffer
	log, err := NewLogger(&buf, "info", "json", "api")
	if err != nil {
		t.Fatal(err)
	}
	log.Debug("hidden")
	addr, _ := domain.ParseAddress("alice@example.org")
	log.Info("accepted", AddrAttr("rcpt", addr))

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("not one JSON line: %v: %s", err, buf.String())
	}
	if line["role"] != "api" || line["msg"] != "accepted" {
		t.Errorf("line = %v", line)
	}
	// The address itself never reaches the logs, only a prefix of its hash.
	if strings.Contains(buf.String(), "alice") || len(line["rcpt"].(string)) != 16 {
		t.Errorf("address logged: %s", buf.String())
	}

	if _, err := NewLogger(&buf, "loud", "json", "api"); err == nil {
		t.Error("invalid level accepted")
	}
	if _, err := NewLogger(&buf, "info", "xml", "api"); err == nil {
		t.Error("invalid format accepted")
	}
}

func probe(h http.Handler, path string) (int, map[string]string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body := map[string]string{}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestProbes(t *testing.T) {
	p := NewProbes()
	h := p.Handler()
	if code, _ := probe(h, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d", code)
	}
	if code, _ := probe(h, "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz without checks = %d", code)
	}

	var dbErr error
	p.AddCheck("database", func(context.Context) error { return dbErr })
	if code, body := probe(h, "/readyz"); code != http.StatusOK || body["database"] != "ok" {
		t.Errorf("/readyz = %d %v", code, body)
	}
	dbErr = errors.New("connection refused")
	if code, body := probe(h, "/readyz"); code != http.StatusServiceUnavailable || body["database"] != "connection refused" {
		t.Errorf("/readyz with a failing check = %d %v", code, body)
	}
	// Liveness does not depend on the database: restarting the pod would
	// not bring Postgres back.
	if code, _ := probe(h, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz with a failing check = %d", code)
	}

	dbErr = nil
	p.draining.Store(true)
	if code, body := probe(h, "/readyz"); code != http.StatusServiceUnavailable || body["shutdown"] != "draining" {
		t.Errorf("/readyz while draining = %d %v", code, body)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

// TestGracefulShutdown: a request in flight when the stop signal arrives is
// answered, not cut.
func TestGracefulShutdown(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	addr := freeAddr(t)
	started := make(chan struct{})
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		// The request context must survive the cancellation of the service.
		if r.Context().Err() != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("done"))
	})

	ctx, stop := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- Run(ctx, log, HTTPService("test", addr, handler, log)) }()

	type result struct {
		body string
		err  error
	}
	got := make(chan result, 1)
	go func() {
		var resp *http.Response
		var err error
		for range 50 { // wait for the listener
			if resp, err = http.Get("http://" + addr + "/"); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			got <- result{err: err}
			return
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		got <- result{body: string(body), err: err}
	}()

	<-started
	stop() // SIGTERM
	select {
	case err := <-finished:
		t.Fatalf("Run returned while a request was in flight: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)

	if r := <-got; r.err != nil || r.body != "done" {
		t.Errorf("in-flight request: %q, %v", r.body, r.err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Errorf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the request finished")
	}
}

func TestRunReturnsTheFirstFailure(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	boom := errors.New("listen: address already in use")
	stopped := make(chan struct{})
	err := Run(context.Background(), log,
		func(context.Context) error { return boom },
		func(ctx context.Context) error { <-ctx.Done(); close(stopped); return nil },
	)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Error("the other service was not stopped")
	}
}
