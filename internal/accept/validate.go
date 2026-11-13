// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package accept

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/render"
)

// Request is a send, as submitted. It is transport-neutral: the REST API
// fills it from JSON, SMTP submission will fill it from a parsed message.
//
// Its JSON form is also what the idempotency hash is computed on, so field
// names and order are part of the contract: changing them invalidates the
// keys in flight during a rolling update.
type Request struct {
	From     string            `json:"from"`
	To       []string          `json:"to"`
	ReplyTo  []string          `json:"reply_to,omitempty"`
	Subject  string            `json:"subject"`
	Text     string            `json:"text,omitempty"`
	HTML     string            `json:"html,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
	Tags     []string          `json:"tags,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Limits bound a request. They are fixed in phase 0 and become per-tenant
// settings with the Tenant CRD.
type Limits struct {
	MaxRecipients    int
	MaxBodyBytes     int // text + HTML
	MaxTags          int
	MaxMetadataKeys  int
	MaxCustomHeaders int
}

// DefaultLimits are the limits of docs/architecture.md.
func DefaultLimits() Limits {
	return Limits{
		MaxRecipients:    50,
		MaxBodyBytes:     10 << 20,
		MaxTags:          10,
		MaxMetadataKeys:  20,
		MaxCustomHeaders: 20,
	}
}

const (
	maxSubjectBytes       = 998 // one header line, RFC 5322 §2.1.1
	maxTagBytes           = 64
	maxMetadataKeyBytes   = 64
	maxMetadataValueBytes = 512
	maxIdempotencyKeyLen  = 255
)

var reTag = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

// validated is a request whose every field has been checked and normalised.
type validated struct {
	content  render.Message
	tags     []string
	metadata map[string]string
}

func (l Limits) validate(req Request) (validated, error) {
	var v validated

	from, err := domain.ParseMailbox(req.From)
	if err != nil {
		return v, domain.FieldErrorf("from", "%v", err)
	}
	v.content.From = from

	if len(req.To) == 0 {
		return v, domain.FieldErrorf("to", "at least one recipient is required")
	}
	if len(req.To) > l.MaxRecipients {
		return v, domain.FieldErrorf("to", "at most %d recipients per request, got %d", l.MaxRecipients, len(req.To))
	}
	seen := make(map[domain.Address]bool, len(req.To))
	for i, raw := range req.To {
		a, err := domain.ParseAddress(raw)
		if err != nil {
			return v, domain.FieldErrorf(fmt.Sprintf("to[%d]", i), "%v", err)
		}
		if seen[a] {
			// One recipient is one message: a duplicate would be sent twice.
			return v, domain.FieldErrorf(fmt.Sprintf("to[%d]", i), "duplicate recipient")
		}
		seen[a] = true
		v.content.To = append(v.content.To, a)
	}

	for i, raw := range req.ReplyTo {
		a, err := domain.ParseAddress(raw)
		if err != nil {
			return v, domain.FieldErrorf(fmt.Sprintf("reply_to[%d]", i), "%v", err)
		}
		v.content.ReplyTo = append(v.content.ReplyTo, a)
	}
	if len(req.ReplyTo) > 5 {
		return v, domain.FieldErrorf("reply_to", "at most 5 addresses")
	}

	subject := strings.TrimSpace(req.Subject)
	switch {
	case subject == "":
		return v, domain.FieldErrorf("subject", "a subject is required")
	case len(subject) > maxSubjectBytes:
		return v, domain.FieldErrorf("subject", "longer than %d bytes", maxSubjectBytes)
	case strings.ContainsAny(subject, "\r\n\x00"):
		return v, domain.FieldErrorf("subject", "must be a single line")
	case !utf8.ValidString(subject):
		return v, domain.FieldErrorf("subject", "must be valid UTF-8")
	}
	v.content.Subject = subject

	if req.Text == "" && req.HTML == "" {
		return v, domain.FieldErrorf("html", "a text or an html body is required")
	}
	if !utf8.ValidString(req.Text) {
		return v, domain.FieldErrorf("text", "must be valid UTF-8")
	}
	if !utf8.ValidString(req.HTML) {
		return v, domain.FieldErrorf("html", "must be valid UTF-8")
	}
	if n := len(req.Text) + len(req.HTML); n > l.MaxBodyBytes {
		return v, &domain.Error{
			Code:   domain.CodePayloadTooLarge,
			Detail: fmt.Sprintf("body of %d bytes exceeds the limit of %d", n, l.MaxBodyBytes),
		}
	}
	v.content.Text, v.content.HTML = req.Text, req.HTML

	if len(req.Headers) > l.MaxCustomHeaders {
		return v, domain.FieldErrorf("headers", "at most %d custom headers", l.MaxCustomHeaders)
	}
	for name, value := range req.Headers {
		if err := render.CheckHeader(name, value); err != nil {
			return v, domain.FieldErrorf("headers", "%v", err)
		}
	}
	v.content.Headers = req.Headers

	if len(req.Tags) > l.MaxTags {
		return v, domain.FieldErrorf("tags", "at most %d tags", l.MaxTags)
	}
	v.tags = []string{}
	for i, tag := range req.Tags {
		if len(tag) > maxTagBytes || !reTag.MatchString(tag) {
			return v, domain.FieldErrorf(fmt.Sprintf("tags[%d]", i),
				"a tag is 1 to %d characters among letters, digits, '_', '.', ':' and '-'", maxTagBytes)
		}
		v.tags = append(v.tags, tag)
	}

	if len(req.Metadata) > l.MaxMetadataKeys {
		return v, domain.FieldErrorf("metadata", "at most %d keys", l.MaxMetadataKeys)
	}
	v.metadata = map[string]string{}
	for k, val := range req.Metadata {
		if k == "" || len(k) > maxMetadataKeyBytes || len(val) > maxMetadataValueBytes {
			return v, domain.FieldErrorf("metadata",
				"keys are 1 to %d bytes, values at most %d bytes", maxMetadataKeyBytes, maxMetadataValueBytes)
		}
		v.metadata[k] = val
	}
	return v, nil
}

func validateIdempotencyKey(key string) error {
	if len(key) > maxIdempotencyKeyLen {
		return domain.FieldErrorf("Idempotency-Key", "longer than %d characters", maxIdempotencyKeyLen)
	}
	for _, r := range key {
		if r < 0x21 || r > 0x7e {
			return domain.FieldErrorf("Idempotency-Key", "must be printable ASCII without spaces")
		}
	}
	return nil
}
