// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package ingest turns KumoMTA log records into Postier events.
package ingest

import (
	"errors"
	"net/mail"
	"net/netip"
	"strings"

	"github.com/google/uuid"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/kumo"
)

// eventTypes is the mapping of docs/architecture.md, "États d'un message".
// A record type that is absent produces no event.
var eventTypes = map[string]domain.EventType{
	kumo.TypeReception:        domain.EventQueued,
	kumo.TypeTransientFailure: domain.EventDeferred,
	kumo.TypeDelivery:         domain.EventDelivered,
	kumo.TypeBounce:           domain.EventBounced,
	kumo.TypeOOB:              domain.EventBounced,
	kumo.TypeFeedback:         domain.EventComplained,
	kumo.TypeExpiration:       domain.EventExpired,
	kumo.TypeAdminBounce:      domain.EventCanceled,
	kumo.TypeRejection:        domain.EventRejected,
}

// Reasons a record yields no event. They are counted, not errors: KumoMTA
// logs more than Postier needs (its own inbound traffic, queue moves).
var (
	errUnknownType = errors.New("record type not mapped")
	errNoMessage   = errors.New("no Postier message referenced")
	errUnverified  = errors.New("reference to a message failed verification")
)

// resolveMessageID finds which Postier message a record is about.
//
// Records about a message Postier injected carry its id in the metadata set
// by the Lua policy. That metadata is trusted: it never leaves KumoMTA, and
// the log hook is authenticated.
//
// OOB and Feedback records are different. They describe mail received from
// the outside world, they carry no metadata, and everything in them is
// attacker-controlled. The message is identified by a value Postier signed
// when sending: the VERP return path, or the Message-ID. The HMAC is
// verified here, so a forged bounce cannot suppress someone's address.
func resolveMessageID(rec *kumo.Record, signer *domain.Signer) (uuid.UUID, error) {
	switch rec.Type {
	case kumo.TypeOOB:
		// KumoMTA puts the envelope recipient of the bounce, the VERP
		// address, in the sender field of the OOB record.
		return verified(signer.ParseReturnPath(rec.Sender))

	case kumo.TypeFeedback:
		fr := rec.FeedbackReport
		if fr == nil {
			return uuid.Nil, errNoMessage
		}
		// The Message-ID of the reported message first: providers keep it
		// even when they redact addresses (Microsoft JMRP).
		if mid := originalMessageID(fr.OriginalMessageText()); mid != "" {
			if id, err := signer.ParseMessageID(mid); err == nil {
				return id, nil
			} else if errors.Is(err, domain.ErrBadSignature) {
				return uuid.Nil, errUnverified
			}
		}
		if fr.OriginalMailFrom != "" {
			return verified(signer.ParseReturnPath(fr.OriginalMailFrom))
		}
		return uuid.Nil, errNoMessage

	default:
		raw := rec.MetaString(kumo.MetaPostierID)
		if raw == "" {
			return uuid.Nil, errNoMessage
		}
		id, err := uuid.Parse(raw)
		if err != nil {
			return uuid.Nil, errNoMessage
		}
		return id, nil
	}
}

func verified(id uuid.UUID, err error) (uuid.UUID, error) {
	switch {
	case err == nil:
		return id, nil
	case errors.Is(err, domain.ErrBadSignature):
		return uuid.Nil, errUnverified
	default:
		return uuid.Nil, errNoMessage
	}
}

// originalMessageID extracts the Message-ID header of a reported message.
func originalMessageID(original string) string {
	if original == "" {
		return ""
	}
	m, err := mail.ReadMessage(strings.NewReader(original))
	if err != nil {
		// A report may carry headers only, without the blank line that ends
		// them: give the parser one.
		m, err = mail.ReadMessage(strings.NewReader(original + "\r\n\r\n"))
		if err != nil {
			return ""
		}
	}
	return m.Header.Get("Message-Id")
}

// attempts converts KumoMTA's counter into "attempts made so far".
// num_attempts counts the attempts that came before the one a record
// describes: a first-try delivery has num_attempts = 0.
func attempts(rec *kumo.Record) int {
	switch rec.Type {
	case kumo.TypeDelivery, kumo.TypeBounce, kumo.TypeTransientFailure:
		return rec.NumAttempts + 1
	case kumo.TypeExpiration, kumo.TypeAdminBounce:
		return rec.NumAttempts
	default:
		// Reception, and the reports about mail that was already delivered.
		return 0
	}
}

// sourceIP extracts the local address of a delivery ("10.0.0.1:53210").
func sourceIP(rec *kumo.Record) *netip.Addr {
	if rec.SourceAddress == nil || rec.SourceAddress.Address == "" {
		return nil
	}
	if ap, err := netip.ParseAddrPort(rec.SourceAddress.Address); err == nil {
		a := ap.Addr()
		return &a
	}
	if a, err := netip.ParseAddr(rec.SourceAddress.Address); err == nil {
		return &a
	}
	return nil
}

// detail is the free-form part of an event, kept for support and debugging.
func detail(rec *kumo.Record) map[string]any {
	d := map[string]any{
		"kumo_id": rec.ID,
		"node":    rec.NodeID,
	}
	if rec.Response.Content != "" {
		d["response"] = rec.Response.Content
	}
	if rec.Response.Command != nil && *rec.Response.Command != "" {
		d["command"] = strings.TrimSpace(*rec.Response.Command)
	}
	if rec.Site != "" {
		d["site"] = rec.Site
	}
	if rec.EgressSource != nil {
		d["egress_source"] = *rec.EgressSource
	}
	switch rec.Type {
	case kumo.TypeOOB:
		d["reported_at"] = rec.ReportedAt()
		d["asynchronous"] = true
	case kumo.TypeFeedback:
		d["reported_at"] = rec.ReportedAt()
		if rec.FeedbackReport != nil {
			d["feedback_type"] = rec.FeedbackReport.FeedbackType
			d["user_agent"] = rec.FeedbackReport.UserAgent
		}
	}
	return d
}
