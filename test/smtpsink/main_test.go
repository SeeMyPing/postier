// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/emersion/go-smtp"
)

func startSMTP(t *testing.T, be smtp.Backend) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := smtp.NewServer(be)
	srv.Domain = "sink.test"
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

const message = "From: Acme <no-reply@acme.test>\r\nTo: <x@sink.test>\r\nSubject: Test\r\n" +
	"Message-ID: <0123.abcd@acme.test>\r\n\r\nHello\r\n"

func send(t *testing.T, addr, from, to string) error {
	t.Helper()
	c, err := smtp.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	return c.SendMail(from, []string{to}, strings.NewReader(message))
}

func codeOf(err error) int {
	var se *smtp.SMTPError
	if errors.As(err, &se) {
		return se.Code
	}
	return 0
}

func TestBehaviours(t *testing.T) {
	s := newSink(config{maxStored: 10})
	addr := startSMTP(t, s)
	const from = "b+id.tag@bounce.acme.test"

	if err := send(t, addr, from, "anyone@sink.test"); err != nil {
		t.Errorf("plain recipient: %v", err)
	}
	for rcpt, want := range map[string]int{
		"550@sink.test":      550,
		"552-full@sink.test": 552,
		"421@sink.test":      421,
		"451+tag@sink.test":  451,
		"data-554@sink.test": 554,
		"DATA-554@sink.test": 554,
	} {
		if got := codeOf(send(t, addr, from, rcpt)); got != want {
			t.Errorf("%s: code %d, want %d", rcpt, got, want)
		}
	}
	// Not a behaviour: a local part that merely starts with digits.
	if err := send(t, addr, from, "5501234@sink.test"); err != nil {
		t.Errorf("5501234@: %v", err)
	}

	st := s.snapshot()
	if st.Accepted != 2 || st.Rejected["550"] != 1 || st.Rejected["554"] != 2 || st.Rejected["421"] != 1 {
		t.Errorf("stats = %+v", st)
	}
}

func TestDeferThenAccept(t *testing.T) {
	s := newSink(config{})
	addr := startSMTP(t, s)
	for attempt := 1; attempt <= 2; attempt++ {
		if got := codeOf(send(t, addr, "a@b.test", "defer2-x@sink.test")); got != 451 {
			t.Fatalf("attempt %d: code %d", attempt, got)
		}
	}
	if err := send(t, addr, "a@b.test", "defer2-x@sink.test"); err != nil {
		t.Fatalf("third attempt: %v", err)
	}
	// The counter is per recipient.
	if got := codeOf(send(t, addr, "a@b.test", "defer2-y@sink.test")); got != 451 {
		t.Errorf("another recipient: code %d", got)
	}
}

// TestDuplicatesAreCounted: the same Message-ID delivered twice to the same
// recipient is what "a duplicate" means for the milestone.
func TestDuplicatesAreCounted(t *testing.T) {
	s := newSink(config{})
	addr := startSMTP(t, s)
	for range 3 {
		if err := send(t, addr, "a@b.test", "x@sink.test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := send(t, addr, "a@b.test", "y@sink.test"); err != nil {
		t.Fatal(err)
	}
	if st := s.snapshot(); st.Duplicates != 1 || st.Accepted != 4 || st.Recipients != 2 {
		t.Errorf("stats = %+v", st)
	}
	s.reset()
	if st := s.snapshot(); st.Accepted != 0 || st.Duplicates != 0 {
		t.Errorf("stats after reset = %+v", st)
	}
}

// collector is the MTA that receives the reports.
type collector struct {
	got chan received
}

type received struct{ from, to, raw string }

type collectSession struct {
	c        *collector
	from, to string
}

func (c *collector) NewSession(*smtp.Conn) (smtp.Session, error) { return &collectSession{c: c}, nil }
func (s *collectSession) Mail(from string, _ *smtp.MailOptions) error {
	s.from = from
	return nil
}
func (s *collectSession) Rcpt(to string, _ *smtp.RcptOptions) error {
	s.to = to
	return nil
}
func (s *collectSession) Data(r io.Reader) error {
	raw, err := io.ReadAll(r)
	s.c.got <- received{s.from, s.to, string(raw)}
	return err
}
func (s *collectSession) Reset()        {}
func (s *collectSession) Logout() error { return nil }

func reportParts(t *testing.T, raw, wantType string) map[string]string {
	t.Helper()
	m, err := mail.ReadMessage(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	mediaType, params, err := mime.ParseMediaType(m.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/report" || params["report-type"] != wantType {
		t.Fatalf("Content-Type = %q %v, %v", mediaType, params, err)
	}
	parts := map[string]string{}
	mr := multipart.NewReader(m.Body, params["boundary"])
	for {
		p, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(p)
		ct, _, _ := mime.ParseMediaType(p.Header.Get("Content-Type"))
		parts[ct] = string(body)
	}
	return parts
}

func TestAsynchronousBounce(t *testing.T) {
	col := &collector{got: make(chan received, 1)}
	mta := startSMTP(t, col)
	s := newSink(config{reportTo: mta, hostname: "sink.test", reportWait: time.Millisecond})
	addr := startSMTP(t, s)
	const verp = "b+0123.abcd@bounce.acme.test"

	if err := send(t, addr, verp, "oob-1@sink.test"); err != nil {
		t.Fatalf("the message itself must be accepted: %v", err)
	}
	var r received
	select {
	case r = <-col.got:
	case <-time.After(5 * time.Second):
		t.Fatal("no DSN was sent")
	}
	// RFC 3464: null reverse-path, addressed to the envelope sender.
	if r.from != "" || r.to != verp {
		t.Errorf("DSN envelope: from %q to %q", r.from, r.to)
	}
	parts := reportParts(t, r.raw, "delivery-status")
	status := parts["message/delivery-status"]
	for _, want := range []string{"Final-Recipient: rfc822; oob-1@sink.test", "Action: failed", "Status: 5.1.1"} {
		if !strings.Contains(status, want) {
			t.Errorf("delivery-status lacks %q:\n%s", want, status)
		}
	}
	if !strings.Contains(parts["text/rfc822-headers"], "Message-ID: <0123.abcd@acme.test>") {
		t.Errorf("original headers not quoted: %q", parts["text/rfc822-headers"])
	}
	// The counter moves once the SMTP session that carried the DSN is over,
	// slightly after the collector saw the message.
	deadline := time.Now().Add(5 * time.Second)
	for s.snapshot().Reports["dsn"] != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("stats = %+v", s.snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestComplaint(t *testing.T) {
	col := &collector{got: make(chan received, 1)}
	mta := startSMTP(t, col)
	s := newSink(config{reportTo: mta, fblAddress: "fbl@fbl.postier.test", hostname: "sink.test", reportWait: time.Millisecond})
	addr := startSMTP(t, s)
	const verp = "b+0123.abcd@bounce.acme.test"

	if err := send(t, addr, verp, "fbl-1@sink.test"); err != nil {
		t.Fatal(err)
	}
	var r received
	select {
	case r = <-col.got:
	case <-time.After(5 * time.Second):
		t.Fatal("no ARF report was sent")
	}
	if r.to != "fbl@fbl.postier.test" {
		t.Errorf("ARF sent to %q", r.to)
	}
	parts := reportParts(t, r.raw, "feedback-report")
	report := parts["message/feedback-report"]
	for _, want := range []string{"Feedback-Type: abuse", "Version: 1", "Original-Mail-From: <" + verp + ">", "Original-Rcpt-To: <fbl-1@sink.test>"} {
		if !strings.Contains(report, want) {
			t.Errorf("feedback-report lacks %q:\n%s", want, report)
		}
	}
	if !strings.Contains(parts["message/rfc822"], "Message-ID: <0123.abcd@acme.test>") {
		t.Error("original message not attached")
	}
}

func TestHTTP(t *testing.T) {
	s := newSink(config{maxStored: 2})
	addr := startSMTP(t, s)
	for _, rcpt := range []string{"a@sink.test", "b@sink.test", "c@sink.test"} {
		if err := send(t, addr, "x@y.test", rcpt); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(s.httpHandler())
	defer srv.Close()

	get := func(path string, out any) {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	var st stats
	get("/stats", &st)
	if st.Accepted != 3 {
		t.Errorf("stats = %+v", st)
	}
	// Only the last two are kept.
	var all, one []stored
	get("/messages", &all)
	get("/messages?to=C@sink.test", &one)
	if len(all) != 2 || len(one) != 1 || !strings.Contains(one[0].Raw, "Subject: Test") {
		t.Errorf("messages: %d kept, %d for c@", len(all), len(one))
	}
	resp, err := http.Post(srv.URL+"/reset", "", nil)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("reset: %v", err)
	}
	get("/stats", &st)
	if st.Accepted != 0 {
		t.Errorf("stats after reset = %+v", st)
	}
}
