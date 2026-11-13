// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package kumo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ErrRejected is a permanent refusal of a message by KumoMTA: injecting it
// again would fail the same way. Every other error is transient.
var ErrRejected = errors.New("kumo: message rejected")

// Injection is one message for one recipient.
//
// Postier injects per recipient rather than using the multi-recipient form of
// the API: each recipient has its own VERP envelope sender and its own
// Message-ID, and the API takes a single envelope sender per request.
type Injection struct {
	EnvelopeSender string
	Recipient      string
	Content        []byte // complete RFC 5322 message
	PostierID      string
	Tenant         string
	Trace          string
}

// Client talks to the HTTP injection API of one KumoMTA node.
type Client struct {
	url      string
	user     string
	password string
	http     *http.Client
}

// NewClient returns a client for the node at baseURL. Connections are kept
// alive and capped at maxConns: the injection rate of dispatch is bounded by
// its worker count, and the transport must not open more than that.
func NewClient(baseURL, user, password string, maxConns int) *Client {
	tr := &http.Transport{
		MaxConnsPerHost:       maxConns,
		MaxIdleConnsPerHost:   maxConns,
		MaxIdleConns:          maxConns,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	return &Client{
		url:      strings.TrimRight(baseURL, "/") + "/api/inject/v1",
		user:     user,
		password: password,
		http:     &http.Client{Transport: tr, Timeout: 60 * time.Second},
	}
}

type injectRequest struct {
	EnvelopeSender  string            `json:"envelope_sender"`
	Content         string            `json:"content"`
	Recipients      []injectRecipient `json:"recipients"`
	TemplateDialect string            `json:"template_dialect"`
	TraceHeaders    traceHeaders      `json:"trace_headers"`
}

type injectRecipient struct {
	Email    string            `json:"email"`
	Metadata map[string]string `json:"metadata"`
}

type traceHeaders struct {
	ReceivedHeader     bool `json:"received_header"`
	SupplementalHeader bool `json:"supplemental_header"`
}

type injectResponse struct {
	SuccessCount     int      `json:"success_count"`
	FailCount        int      `json:"fail_count"`
	FailedRecipients []string `json:"failed_recipients"`
	Errors           []string `json:"errors"`
}

// Inject hands a message to KumoMTA. A nil error means the message is in the
// spool and a Reception record will follow.
func (c *Client) Inject(ctx context.Context, in Injection) error {
	// The API takes the message as a JSON string. Postier only produces
	// ASCII messages (encoded headers, quoted-printable bodies); anything
	// else would be silently altered by the JSON encoder.
	if !utf8.Valid(in.Content) {
		return fmt.Errorf("%w: content is not valid UTF-8", ErrRejected)
	}
	body, err := json.Marshal(injectRequest{
		EnvelopeSender: in.EnvelopeSender,
		Content:        string(in.Content),
		Recipients: []injectRecipient{{
			Email: in.Recipient,
			Metadata: map[string]string{
				"postier_id": in.PostierID,
				"tenant":     in.Tenant,
				"trace":      in.Trace,
			},
		}},
		// The message is final. Without "Static", KumoMTA would expand
		// "{{ … }}" found in a customer's HTML as a template.
		TemplateDialect: "Static",
		// No Received header for the internal hop, and no X-KumoRef: Postier
		// ties complaints back with its own signed Message-ID.
		TraceHeaders: traceHeaders{},
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.user, c.password)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("kumo: inject: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("kumo: inject: reading response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("kumo: inject: HTTP %d: %s", resp.StatusCode, firstLine(string(raw)))
		switch resp.StatusCode {
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
			// The request itself is wrong; it will not get better.
			return fmt.Errorf("%w: %s", ErrRejected, msg)
		default:
			// 401/403 included: a credential mismatch is an operator error
			// to fix, not a reason to fail the customer's message.
			return errors.New(msg)
		}
	}

	var out injectResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("kumo: inject: decoding response: %w", err)
	}
	if out.FailCount == 0 && out.SuccessCount == 1 {
		return nil
	}
	return classifyFailure(out)
}

// A refusal by the policy comes back as HTTP 200 with fail_count > 0 and an
// error such as "user@example.org: 550 5.7.1 reason\nstack traceback: …".
var reFailureCode = regexp.MustCompile(`^[^\s:]+: ([45]\d\d)\b`)

func classifyFailure(out injectResponse) error {
	detail := "no detail"
	if len(out.Errors) > 0 {
		detail = firstLine(out.Errors[0])
	}
	if m := reFailureCode.FindStringSubmatch(detail); m != nil {
		if code, _ := strconv.Atoi(m[1]); code >= 500 {
			return fmt.Errorf("%w: %s", ErrRejected, detail)
		}
	}
	return fmt.Errorf("kumo: inject: %s", detail)
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}
