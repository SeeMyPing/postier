// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package kumo isolates everything that depends on KumoMTA: the injection
// client, the log record format and the data file read by the Lua policy.
// An upstream change should never have to be chased outside this package.
package kumo

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Record types emitted by KumoMTA. Types Postier has no use for (AdminRebind,
// Delayed, XferIn…) are decoded like the others and ignored by ingest.
const (
	TypeReception        = "Reception"
	TypeDelivery         = "Delivery"
	TypeBounce           = "Bounce"
	TypeTransientFailure = "TransientFailure"
	TypeExpiration       = "Expiration"
	TypeAdminBounce      = "AdminBounce"
	TypeOOB              = "OOB"
	TypeFeedback         = "Feedback"
	TypeRejection        = "Rejection"
)

// Metadata keys set by the Lua policy (kumo/policy/init.lua) and copied by
// KumoMTA into every record of a message.
const (
	MetaPostierID = "x_postier_id"
	MetaTrace     = "x_postier_trace"
	MetaTenant    = "tenant"
)

// ProtocolLogRecord is the reception protocol of the records KumoMTA writes
// about the delivery of its own log hook. They must never be ingested.
const ProtocolLogRecord = "LogRecord"

// Record is a KumoMTA log record, as documented in
// https://docs.kumomta.com/reference/log_record/ and as observed on the
// pinned version (see testdata/records).
type Record struct {
	Type      string     `json:"type"`
	ID        string     `json:"id"` // spool id, not the Postier id
	Sender    string     `json:"sender"`
	Recipient Recipients `json:"recipient"`
	Queue     string     `json:"queue"`
	Site      string     `json:"site"`
	Size      int64      `json:"size"`

	Response    Response     `json:"response"`
	PeerAddress *PeerAddress `json:"peer_address"`

	Timestamp   int64      `json:"timestamp"`
	EventTime   *time.Time `json:"event_time"`
	Created     int64      `json:"created"`
	CreatedTime *time.Time `json:"created_time"`

	NumAttempts          int            `json:"num_attempts"`
	BounceClassification string         `json:"bounce_classification"`
	EgressPool           *string        `json:"egress_pool"`
	EgressSource         *string        `json:"egress_source"`
	SourceAddress        *SourceAddress `json:"source_address"`

	FeedbackReport *FeedbackReport `json:"feedback_report"`

	Meta    map[string]json.RawMessage `json:"meta"`
	Headers map[string]json.RawMessage `json:"headers"`

	DeliveryProtocol  *string `json:"delivery_protocol"`
	ReceptionProtocol *string `json:"reception_protocol"`
	NodeID            string  `json:"nodeid"`
	SessionID         *string `json:"session_id"`
}

// Recipients is the envelope recipient: a string, or an array for a message
// that carries several (possible since KumoMTA 2025.12).
type Recipients []string

// UnmarshalJSON accepts both shapes.
func (r *Recipients) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if bytes.Equal(b, []byte("null")) {
		*r = nil
		return nil
	}
	if len(b) > 0 && b[0] == '[' {
		var list []string
		if err := json.Unmarshal(b, &list); err != nil {
			return err
		}
		*r = list
		return nil
	}
	var one string
	if err := json.Unmarshal(b, &one); err != nil {
		return err
	}
	*r = Recipients{one}
	return nil
}

// First returns the first recipient, or "".
func (r Recipients) First() string {
	if len(r) == 0 {
		return ""
	}
	return r[0]
}

// Response is the SMTP reply attached to a record.
type Response struct {
	Code         int           `json:"code"`
	EnhancedCode *EnhancedCode `json:"enhanced_code"`
	Content      string        `json:"content"`
	Command      *string       `json:"command"`
}

// EnhancedCode is an RFC 3463 status code, split into its three fields.
type EnhancedCode struct {
	Class   int `json:"class"`
	Subject int `json:"subject"`
	Detail  int `json:"detail"`
}

// String renders the code as "5.1.1".
func (e *EnhancedCode) String() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%d.%d.%d", e.Class, e.Subject, e.Detail)
}

// PeerAddress is the remote end: the MX when delivering, the client when
// receiving.
type PeerAddress struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

// SourceAddress is the local address a delivery was made from.
type SourceAddress struct {
	Address  string  `json:"address"`
	Protocol *string `json:"protocol"`
	Server   *string `json:"server"`
}

// FeedbackReport is a parsed ARF report (RFC 5965). Only the fields Postier
// reads are typed; the rest is ignored on purpose.
type FeedbackReport struct {
	FeedbackType     string   `json:"feedback_type"`
	UserAgent        string   `json:"user_agent"`
	OriginalMailFrom string   `json:"original_mail_from"`
	OriginalRcptTo   []string `json:"original_rcpto_to"` // sic: upstream spelling
	// OriginalMessage is a string when the bytes are valid UTF-8, an array of
	// byte values otherwise.
	OriginalMessage json.RawMessage `json:"original_message"`
}

// OriginalMessageText returns the reported message when KumoMTA sent it as
// text, which is the case for anything Postier produced (ASCII headers,
// quoted-printable bodies).
func (f *FeedbackReport) OriginalMessageText() string {
	if f == nil || len(f.OriginalMessage) == 0 || f.OriginalMessage[0] != '"' {
		return ""
	}
	var s string
	if err := json.Unmarshal(f.OriginalMessage, &s); err != nil {
		return ""
	}
	return s
}

// MaxRecordsPerBatch bounds what DecodeBatch accepts. The policy batches by
// 100; the margin leaves room for tuning without letting one request pin an
// arbitrary amount of memory.
const MaxRecordsPerBatch = 10_000

// Invalid is an element of a batch that is not a usable record.
type Invalid struct {
	Raw json.RawMessage
	Err error
}

// DecodeBatch decodes the body of a log hook request: a JSON array of
// records. A single object is accepted too (log_hook.batch_size = 1).
//
// Records are decoded one by one. An element that does not decode is
// returned apart, so that one unexpected record cannot take down the batch
// it travels with. An error is only returned when the body as a whole is
// not what the hook sends.
func DecodeBatch(body []byte) ([]Record, []Invalid, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, nil, fmt.Errorf("kumo: empty batch")
	}
	var raws []json.RawMessage
	switch body[0] {
	case '[':
		if err := json.Unmarshal(body, &raws); err != nil {
			return nil, nil, fmt.Errorf("kumo: batch: %w", err)
		}
	case '{':
		if !json.Valid(body) {
			return nil, nil, fmt.Errorf("kumo: record is not valid JSON")
		}
		raws = []json.RawMessage{body}
	default:
		return nil, nil, fmt.Errorf("kumo: batch is neither an array nor an object")
	}
	if len(raws) > MaxRecordsPerBatch {
		return nil, nil, fmt.Errorf("kumo: batch of %d records exceeds %d", len(raws), MaxRecordsPerBatch)
	}

	records := make([]Record, 0, len(raws))
	var invalid []Invalid
	for _, raw := range raws {
		var rec Record
		err := json.Unmarshal(raw, &rec)
		if err == nil && rec.Type == "" {
			err = fmt.Errorf("record has no type")
		}
		if err != nil {
			invalid = append(invalid, Invalid{Raw: raw, Err: err})
			continue
		}
		records = append(records, rec)
	}
	return records, invalid, nil
}

// MetaString returns a metadata value when it is a non-empty string.
func (r *Record) MetaString(key string) string {
	raw, ok := r.Meta[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// IsLogHookEcho reports whether the record is about the delivery of a log
// record itself. The policy already filters those out; this is the second
// line of defence against an infinite loop.
func (r *Record) IsLogHookEcho() bool {
	return r.ReceptionProtocol != nil && *r.ReceptionProtocol == ProtocolLogRecord
}

// eventTime is the time of the event with the best available precision.
func (r *Record) eventTime() time.Time {
	if r.EventTime != nil {
		return r.EventTime.UTC()
	}
	return time.Unix(r.Timestamp, 0).UTC()
}

func (r *Record) createdTime() time.Time {
	if r.CreatedTime != nil {
		return r.CreatedTime.UTC()
	}
	return time.Unix(r.Created, 0).UTC()
}

// OccurredAt is the timestamp Postier stores for the event, and the
// partition key of the events table.
//
// It comes from the record, never from the clock of ingest: a replayed
// record must produce the same value for the deduplication to hold.
//
// OOB and Feedback records are the exception to "event_time". Their
// event_time is copied from the report (Arrival-Date, Last-Attempt-Date),
// that is, chosen by whoever wrote the bounce. Using it would let a remote
// party pick the partition, or a date with no partition at all. For those
// two types the time KumoMTA received the report is used instead.
func (r *Record) OccurredAt() time.Time {
	var t time.Time
	switch r.Type {
	case TypeOOB, TypeFeedback:
		t = r.createdTime()
	default:
		t = r.eventTime()
	}
	// Postgres stores microseconds. Truncating here keeps the Go value equal
	// to what is read back.
	return t.Truncate(time.Microsecond)
}

// ReportedAt is the date claimed by an OOB or Feedback report, for display.
func (r *Record) ReportedAt() time.Time { return r.eventTime() }

// DedupKey identifies a record across redeliveries of the log hook:
//
//	sha256(spool id ‖ type ‖ event time ‖ num_attempts ‖ recipient)
//
// A retried batch carries byte-identical records, so the key is stable.
// The fields are not redundant: one message produces several records of the
// same type (a TransientFailure per attempt), and one DSN produces an OOB
// record per failed recipient, all with the same spool id.
func (r *Record) DedupKey() string {
	h := sha256.New()
	write := func(s string) {
		// Length-prefixed, so that ("ab","c") and ("a","bc") differ.
		h.Write([]byte(strconv.Itoa(len(s))))
		h.Write([]byte{':'})
		h.Write([]byte(s))
	}
	write(r.ID)
	write(r.Type)
	write(r.eventTime().Format(time.RFC3339Nano))
	write(strconv.Itoa(r.NumAttempts))
	write(r.Recipient.First())
	return hex.EncodeToString(h.Sum(nil))
}
