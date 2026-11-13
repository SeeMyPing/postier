// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package render turns an accepted request into a MIME message.
package render

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"sort"
	"strings"
	"time"

	"github.com/emersion/go-message/mail"

	"github.com/SeeMyPing/postier/internal/domain"
)

// Message is the content of a send, already validated and normalised.
type Message struct {
	From    domain.Mailbox
	To      []domain.Address
	ReplyTo []domain.Address
	Subject string
	Text    string
	HTML    string
	Headers map[string]string
	Date    time.Time
}

// ErrHeaderNotAllowed is returned for a custom header Postier owns or that
// would let a client alter routing or authentication.
var ErrHeaderNotAllowed = errors.New("header not allowed")

// reservedHeaders cannot be set by a client: Postier or the MTA writes them.
var reservedHeaders = map[string]bool{
	"From": true, "To": true, "Cc": true, "Bcc": true, "Sender": true,
	"Reply-To": true, "Subject": true, "Date": true, "Message-Id": true,
	"Mime-Version": true, "Content-Type": true, "Content-Transfer-Encoding": true,
	"Return-Path": true, "Received": true, "Dkim-Signature": true,
	"Authentication-Results": true, "Arc-Seal": true,
	"Arc-Message-Signature": true, "Arc-Authentication-Results": true,
}

// CheckHeader validates a custom header.
func CheckHeader(name, value string) error {
	canon := textproto.CanonicalMIMEHeaderKey(name)
	if name == "" || len(name) > 64 {
		return fmt.Errorf("%w: %q: invalid name", ErrHeaderNotAllowed, name)
	}
	for _, r := range name {
		// RFC 5322 ftext: printable ASCII except colon.
		if r <= ' ' || r >= 0x7f || r == ':' {
			return fmt.Errorf("%w: %q: invalid name", ErrHeaderNotAllowed, name)
		}
	}
	if reservedHeaders[canon] || strings.HasPrefix(canon, "X-Postier-") {
		return fmt.Errorf("%w: %q is set by Postier", ErrHeaderNotAllowed, name)
	}
	if len(value) > 998 || strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("%w: %q: invalid value", ErrHeaderNotAllowed, name)
	}
	return nil
}

// Build renders the message, without a Message-ID.
//
// The Message-ID is unique per recipient and is added by dispatch at
// injection time (see WithMessageID). Leaving it out here is what lets all
// the recipients of a send share a single stored body.
//
// The output is pure ASCII with CRLF line endings: headers are RFC 2047
// encoded and bodies quoted-printable, so the message survives any relay and
// the JSON transport of the injection API.
func Build(m Message) ([]byte, error) {
	if m.Text == "" && m.HTML == "" {
		return nil, errors.New("render: a message needs a text or an HTML body")
	}
	if len(m.To) == 0 {
		return nil, errors.New("render: a message needs a recipient")
	}

	var h mail.Header
	h.SetDate(m.Date)
	h.SetAddressList("From", []*mail.Address{{Name: m.From.Name, Address: m.From.Address.String()}})
	h.SetAddressList("To", addressList(m.To))
	if len(m.ReplyTo) > 0 {
		h.SetAddressList("Reply-To", addressList(m.ReplyTo))
	}
	h.SetSubject(m.Subject)
	h.Set("MIME-Version", "1.0")

	names := make([]string, 0, len(m.Headers))
	for name := range m.Headers {
		names = append(names, name)
	}
	sort.Strings(names) // stable output: same request, same bytes
	for _, name := range names {
		if err := CheckHeader(name, m.Headers[name]); err != nil {
			return nil, err
		}
		h.Set(name, m.Headers[name])
	}

	var buf bytes.Buffer
	switch {
	case m.Text != "" && m.HTML != "":
		w, err := mail.CreateInlineWriter(&buf, h)
		if err != nil {
			return nil, err
		}
		// multipart/alternative: the richest part goes last.
		if err := writePart(w, "text/plain", m.Text); err != nil {
			return nil, err
		}
		if err := writePart(w, "text/html", m.HTML); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
	default:
		ctype, body := "text/plain", m.Text
		if m.HTML != "" {
			ctype, body = "text/html", m.HTML
		}
		h.SetContentType(ctype, map[string]string{"charset": "utf-8"})
		h.Set("Content-Transfer-Encoding", "quoted-printable")
		w, err := mail.CreateSingleInlineWriter(&buf, h)
		if err != nil {
			return nil, err
		}
		if _, err := io.WriteString(w, body); err != nil {
			return nil, err
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func writePart(w *mail.InlineWriter, ctype, body string) error {
	var ih mail.InlineHeader
	ih.SetContentType(ctype, map[string]string{"charset": "utf-8"})
	ih.Set("Content-Transfer-Encoding", "quoted-printable")
	pw, err := w.CreatePart(ih)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(pw, body); err != nil {
		return err
	}
	return pw.Close()
}

func addressList(addrs []domain.Address) []*mail.Address {
	out := make([]*mail.Address, len(addrs))
	for i, a := range addrs {
		out[i] = &mail.Address{Address: a.String()}
	}
	return out
}

// WithMessageID returns the message with a Message-ID header prepended.
// messageID includes its angle brackets.
func WithMessageID(mime []byte, messageID string) []byte {
	out := make([]byte, 0, len(mime)+len(messageID)+16)
	out = append(out, "Message-ID: "...)
	out = append(out, messageID...)
	out = append(out, "\r\n"...)
	return append(out, mime...)
}
