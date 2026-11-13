// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package render

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/SeeMyPing/postier/internal/domain"
)

func addr(t *testing.T, s string) domain.Address {
	t.Helper()
	a, err := domain.ParseAddress(s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func sample(t *testing.T) Message {
	t.Helper()
	return Message{
		From:    domain.Mailbox{Name: "Société Générale d'Épicerie", Address: addr(t, "no-reply@acme.test")},
		To:      []domain.Address{addr(t, "alice@example.org"), addr(t, "bob@example.org")},
		ReplyTo: []domain.Address{addr(t, "support@acme.test")},
		Subject: "Votre commande n°48213 est expédiée",
		Text:    "Bonjour,\nvotre colis est parti. À bientôt !\n",
		HTML:    "<p>Bonjour,<br>votre colis est parti. <b>À bientôt !</b> {{ not_a_template }}</p>",
		Headers: map[string]string{"X-Campaign": "orders", "List-Unsubscribe": "<mailto:unsub@acme.test>"},
		Date:    time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC),
	}
}

func parse(t *testing.T, raw []byte) *mail.Message {
	t.Helper()
	m, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("unparsable message: %v\n%s", err, raw)
	}
	return m
}

func TestBuildAlternative(t *testing.T) {
	raw, err := Build(sample(t))
	if err != nil {
		t.Fatal(err)
	}
	m := parse(t, raw)

	dec := new(mime.WordDecoder)
	if got, _ := dec.DecodeHeader(m.Header.Get("Subject")); got != "Votre commande n°48213 est expédiée" {
		t.Errorf("Subject = %q", got)
	}
	from, err := mail.ParseAddress(m.Header.Get("From"))
	if err != nil || from.Name != "Société Générale d'Épicerie" || from.Address != "no-reply@acme.test" {
		t.Errorf("From = %v, %v", from, err)
	}
	to, err := mail.ParseAddressList(m.Header.Get("To"))
	if err != nil || len(to) != 2 {
		t.Errorf("To = %v, %v", to, err)
	}
	if m.Header.Get("Reply-To") == "" || m.Header.Get("Mime-Version") != "1.0" || m.Header.Get("X-Campaign") != "orders" {
		t.Errorf("headers = %v", m.Header)
	}
	if date, err := m.Header.Date(); err != nil || !date.Equal(sample(t).Date) {
		t.Errorf("Date = %v, %v", date, err)
	}

	mediaType, params, err := mime.ParseMediaType(m.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/alternative" {
		t.Fatalf("Content-Type = %q, %v", mediaType, err)
	}
	mr := multipart.NewReader(m.Body, params["boundary"])
	var types, bodies []string
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		ct, _, _ := mime.ParseMediaType(part.Header.Get("Content-Type"))
		types = append(types, ct)
		// multipart.Reader decodes quoted-printable transparently.
		body, _ := io.ReadAll(part)
		bodies = append(bodies, string(body))
	}
	// The richest alternative goes last (RFC 2046 §5.1.4).
	if strings.Join(types, ",") != "text/plain,text/html" {
		t.Fatalf("parts = %v", types)
	}
	if !strings.Contains(bodies[0], "À bientôt") || !strings.Contains(bodies[1], "{{ not_a_template }}") {
		t.Errorf("bodies altered: %q", bodies)
	}
}

func TestBuildSinglePart(t *testing.T) {
	for name, edit := range map[string]func(*Message){
		"text/plain": func(m *Message) { m.HTML = "" },
		"text/html":  func(m *Message) { m.Text = "" },
	} {
		msg := sample(t)
		edit(&msg)
		raw, err := Build(msg)
		if err != nil {
			t.Fatal(err)
		}
		m := parse(t, raw)
		ct, params, _ := mime.ParseMediaType(m.Header.Get("Content-Type"))
		if ct != name || !strings.EqualFold(params["charset"], "utf-8") {
			t.Errorf("%s: Content-Type = %q %v", name, ct, params)
		}
		if m.Header.Get("Content-Transfer-Encoding") != "quoted-printable" {
			t.Errorf("%s: encoding = %q", name, m.Header.Get("Content-Transfer-Encoding"))
		}
		body, _ := io.ReadAll(quotedprintable.NewReader(m.Body))
		if !strings.Contains(string(body), "À bientôt") {
			t.Errorf("%s: body = %q", name, body)
		}
	}
}

// TestOutputIsWireSafe: 7-bit, CRLF, and no line over the SMTP limit, so
// the message survives any relay and the JSON transport to KumoMTA.
func TestOutputIsWireSafe(t *testing.T) {
	msg := sample(t)
	msg.HTML = "<p>" + strings.Repeat("Ligne très longue sans retour, éèà. ", 400) + "</p>"
	msg.Subject = strings.Repeat("Sujet interminable é ", 40)
	raw, err := Build(msg)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range raw {
		if b >= 0x80 {
			t.Fatalf("non-ASCII byte 0x%x at offset %d", b, i)
		}
		if b == '\n' && (i == 0 || raw[i-1] != '\r') {
			t.Fatalf("bare LF at offset %d", i)
		}
	}
	for _, line := range bytes.Split(raw, []byte("\r\n")) {
		if len(line) > 998 {
			t.Fatalf("line of %d bytes", len(line))
		}
	}
}

func TestBuildIsDeterministic(t *testing.T) {
	a, err := Build(sample(t))
	if err != nil {
		t.Fatal(err)
	}
	// The MIME boundary is random; everything before it must not be.
	b, _ := Build(sample(t))
	headersOf := func(raw []byte) string {
		var kept []string
		head, _, found := bytes.Cut(raw, []byte("\r\n\r\n"))
		if !found {
			t.Fatal("message without a header block")
		}
		for _, l := range strings.Split(string(head), "\r\n") {
			if !strings.Contains(l, "boundary=") {
				kept = append(kept, l)
			}
		}
		return strings.Join(kept, "\n")
	}
	if headersOf(a) != headersOf(b) {
		t.Errorf("headers differ between two builds:\n%s\n---\n%s", headersOf(a), headersOf(b))
	}
}

func TestBuildRequiresBodyAndRecipient(t *testing.T) {
	msg := sample(t)
	msg.Text, msg.HTML = "", ""
	if _, err := Build(msg); err == nil {
		t.Error("a message without a body was built")
	}
	msg = sample(t)
	msg.To = nil
	if _, err := Build(msg); err == nil {
		t.Error("a message without a recipient was built")
	}
}

func TestCheckHeader(t *testing.T) {
	for _, name := range []string{"X-Campaign", "List-Unsubscribe", "List-Unsubscribe-Post", "X-Entity-Ref-ID"} {
		if err := CheckHeader(name, "value"); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	refused := map[string]string{
		"From":                  "x@y.z",
		"to":                    "x@y.z",
		"BCC":                   "x@y.z",
		"Message-ID":            "<a@b>",
		"message-id":            "<a@b>",
		"DKIM-Signature":        "v=1",
		"Return-Path":           "<a@b>",
		"Received":              "by x",
		"Content-Type":          "text/plain",
		"X-Postier-Id":          "1",
		"":                      "v",
		"Bad Name":              "v",
		"Bad:Name":              "v",
		"X-Ünïcode":             "v",
		strings.Repeat("X", 65): "v",
	}
	for name, value := range refused {
		if err := CheckHeader(name, value); !errors.Is(err, ErrHeaderNotAllowed) {
			t.Errorf("%q accepted: %v", name, err)
		}
	}
	// Header injection through the value.
	for _, value := range []string{"a\r\nBcc: evil@example.org", "a\nb", "a\x00b", strings.Repeat("v", 999)} {
		if err := CheckHeader("X-Ok", value); !errors.Is(err, ErrHeaderNotAllowed) {
			t.Errorf("value %q accepted", value[:min(len(value), 20)])
		}
	}
}

func TestBuildRefusesReservedHeaders(t *testing.T) {
	msg := sample(t)
	msg.Headers = map[string]string{"Message-ID": "<forged@evil.example>"}
	if _, err := Build(msg); !errors.Is(err, ErrHeaderNotAllowed) {
		t.Fatalf("err = %v", err)
	}
}

// TestWithMessageID: the stored body has no Message-ID, so that it can be
// shared; dispatch adds one per recipient.
func TestWithMessageID(t *testing.T) {
	raw, err := Build(sample(t))
	if err != nil {
		t.Fatal(err)
	}
	if parse(t, raw).Header.Get("Message-Id") != "" {
		t.Fatal("the stored body already has a Message-ID")
	}
	a := WithMessageID(raw, "<aaa.tag@acme.test>")
	b := WithMessageID(raw, "<bbb.tag@acme.test>")
	if parse(t, a).Header.Get("Message-Id") != "<aaa.tag@acme.test>" {
		t.Errorf("Message-ID = %q", parse(t, a).Header.Get("Message-Id"))
	}
	if !bytes.HasSuffix(a, raw) || !bytes.HasSuffix(b, raw) {
		t.Error("the body was altered")
	}
	if parse(t, a).Header.Get("Subject") != parse(t, raw).Header.Get("Subject") {
		t.Error("headers after the Message-ID were lost")
	}
}
