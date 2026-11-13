// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package domain

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Signer authenticates the two identifiers Postier puts on the wire and later
// reads back from untrusted mail:
//
//   - the VERP return path, "b+<id>.<tag>@bounce.<domain>", carried by
//     asynchronous bounces;
//   - the Message-ID, "<id.tag@domain>", found in the original message
//     attached to ARF complaints.
//
// Without the tag anyone could forge a bounce for a guessed id (a UUIDv7 is
// mostly a timestamp) and get a legitimate address suppressed.
type Signer struct {
	key []byte
}

// MinSigningKeyLen is the minimum length of the HMAC key, in bytes.
const MinSigningKeyLen = 32

// tagLen is the length of the truncated HMAC, in base32 characters (40 bits).
// An attacker gets one guess per forged message, so 2^40 is ample, and a
// short tag keeps the local part well under 64 octets.
const tagLen = 8

var (
	// ErrBadSignature means the address or Message-ID was not produced by this
	// installation, or was altered.
	ErrBadSignature = errors.New("signature mismatch")
	// ErrNotSigned means the value does not have the expected shape at all.
	ErrNotSigned = errors.New("not a signed Postier identifier")
)

var tagEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// NewSigner builds a Signer from a secret shared by api, dispatch and ingest.
func NewSigner(key []byte) (*Signer, error) {
	if len(key) < MinSigningKeyLen {
		return nil, fmt.Errorf("signing key must be at least %d bytes, got %d", MinSigningKeyLen, len(key))
	}
	return &Signer{key: append([]byte(nil), key...)}, nil
}

// tag computes the truncated HMAC. purpose separates the two uses so that a
// tag lifted from a Message-ID (visible to every recipient) is useless for
// forging a return path.
func (s *Signer) tag(purpose string, id uuid.UUID) string {
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(purpose))
	mac.Write([]byte{0})
	mac.Write(id[:])
	return strings.ToLower(tagEncoding.EncodeToString(mac.Sum(nil)))[:tagLen]
}

func (s *Signer) verify(purpose string, id uuid.UUID, tag string) bool {
	want := s.tag(purpose, id)
	return subtle.ConstantTimeCompare([]byte(want), []byte(strings.ToLower(tag))) == 1
}

const (
	purposeReturnPath = "verp"
	purposeMessageID  = "msgid"
	returnPathPrefix  = "b+"
)

// ReturnPath builds the envelope sender for a message. bounceDomain is the
// return-path domain of the sending domain (e.g. "bounce.acme.example").
func (s *Signer) ReturnPath(id uuid.UUID, bounceDomain string) string {
	return returnPathPrefix + CompactID(id) + "." + s.tag(purposeReturnPath, id) + "@" + bounceDomain
}

// ParseReturnPath extracts and authenticates the message id of a VERP
// address. The comparison is case-insensitive: relays are free to change the
// case of a local part.
func (s *Signer) ParseReturnPath(addr string) (uuid.UUID, error) {
	addr = strings.Trim(strings.TrimSpace(addr), "<>")
	at := strings.LastIndexByte(addr, '@')
	if at < 0 {
		return uuid.Nil, ErrNotSigned
	}
	local := strings.ToLower(addr[:at])
	if !strings.HasPrefix(local, returnPathPrefix) {
		return uuid.Nil, ErrNotSigned
	}
	return s.parseSigned(purposeReturnPath, strings.TrimPrefix(local, returnPathPrefix))
}

// MessageID builds the Message-ID header value, angle brackets included.
func (s *Signer) MessageID(id uuid.UUID, domain string) string {
	return "<" + CompactID(id) + "." + s.tag(purposeMessageID, id) + "@" + domain + ">"
}

// ParseMessageID extracts and authenticates the message id of a Message-ID.
func (s *Signer) ParseMessageID(v string) (uuid.UUID, error) {
	v = strings.Trim(strings.TrimSpace(v), "<>")
	at := strings.LastIndexByte(v, '@')
	if at < 0 {
		return uuid.Nil, ErrNotSigned
	}
	return s.parseSigned(purposeMessageID, strings.ToLower(v[:at]))
}

func (s *Signer) parseSigned(purpose, v string) (uuid.UUID, error) {
	idPart, tag, ok := strings.Cut(v, ".")
	if !ok || len(idPart) != 32 || len(tag) != tagLen {
		return uuid.Nil, ErrNotSigned
	}
	id, err := ParseCompactID(idPart)
	if err != nil {
		return uuid.Nil, ErrNotSigned
	}
	if !s.verify(purpose, id, tag) {
		return uuid.Nil, ErrBadSignature
	}
	return id, nil
}
