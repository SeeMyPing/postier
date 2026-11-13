// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package domain

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/idna"
	"golang.org/x/text/unicode/norm"
)

// Address is a normalised email address. Two spellings of the same mailbox
// normalise to the same Address, hence to the same Hash, which is what the
// suppression list and the right to erasure are keyed on.
type Address struct {
	Local  string // NFC, lowercase
	Domain string // ASCII (punycode), lowercase, no trailing dot
}

// Mailbox is an address with its optional display name, as found in From.
type Mailbox struct {
	Name    string
	Address Address
}

// ErrInvalidAddress wraps every address parsing failure.
var ErrInvalidAddress = errors.New("invalid email address")

const (
	maxLocalLen  = 64  // RFC 5321 §4.5.3.1.1, in octets
	maxDomainLen = 253 // longest name that fits in a DNS message
)

// domainProfile is the IDNA2008 lookup profile, plus the DNS length checks
// (63 octets per label) that idna.Lookup leaves out.
var domainProfile = idna.New(
	idna.MapForLookup(),
	idna.BidiRule(),
	idna.CheckHyphens(true),
	idna.CheckJoiners(true),
	idna.StrictDomainName(true),
	idna.VerifyDNSLength(true),
)

// ParseAddress normalises a bare address ("user@example.org"):
//
//   - the domain is lowercased and converted to punycode (IDNA2008 lookup
//     profile), so "bücher.example" and "xn--bcher-kva.example" are equal;
//   - the local part is put in NFC and lowercased. RFC 5321 says the local
//     part is case-sensitive, but no mailbox provider treats it that way, and
//     a suppression that a different capitalisation can bypass is worthless.
//
// Quoted local parts and address literals are rejected: they have no use in
// transactional email and are a classic source of parsing discrepancies.
func ParseAddress(s string) (Address, error) {
	s = strings.TrimSpace(s)
	at := strings.LastIndexByte(s, '@')
	if at <= 0 || at == len(s)-1 {
		return Address{}, fmt.Errorf("%w: missing local part or domain", ErrInvalidAddress)
	}
	local, dom := s[:at], s[at+1:]

	if !utf8.ValidString(local) {
		return Address{}, fmt.Errorf("%w: local part is not valid UTF-8", ErrInvalidAddress)
	}
	local = strings.ToLower(norm.NFC.String(local))
	if len(local) > maxLocalLen {
		return Address{}, fmt.Errorf("%w: local part longer than %d octets", ErrInvalidAddress, maxLocalLen)
	}
	if err := checkLocal(local); err != nil {
		return Address{}, err
	}

	dom = strings.TrimSuffix(dom, ".")
	if strings.HasPrefix(dom, "[") {
		return Address{}, fmt.Errorf("%w: address literals are not accepted", ErrInvalidAddress)
	}
	ascii, err := domainProfile.ToASCII(dom)
	if err != nil {
		return Address{}, fmt.Errorf("%w: domain: %w", ErrInvalidAddress, err)
	}
	if ascii == "" || len(ascii) > maxDomainLen || !strings.Contains(ascii, ".") {
		return Address{}, fmt.Errorf("%w: domain is not a fully qualified name", ErrInvalidAddress)
	}
	return Address{Local: local, Domain: ascii}, nil
}

// checkLocal accepts a dot-atom (RFC 5322 atext, plus UTF-8 per RFC 6531).
func checkLocal(local string) error {
	if strings.HasPrefix(local, ".") || strings.HasSuffix(local, ".") || strings.Contains(local, "..") {
		return fmt.Errorf("%w: misplaced dot in local part", ErrInvalidAddress)
	}
	for _, r := range local {
		switch {
		case r >= utf8.RuneSelf:
			if !unicode.IsPrint(r) || unicode.IsSpace(r) {
				return fmt.Errorf("%w: forbidden character in local part", ErrInvalidAddress)
			}
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case strings.ContainsRune("!#$%&'*+-/=?^_`{|}~.", r):
		default:
			return fmt.Errorf("%w: forbidden character %q in local part", ErrInvalidAddress, r)
		}
	}
	return nil
}

// ParseMailbox normalises an address that may carry a display name
// ("Acme <no-reply@acme.example>").
func ParseMailbox(s string) (Mailbox, error) {
	s = strings.TrimSpace(s)
	if !strings.ContainsAny(s, "<\"") {
		a, err := ParseAddress(s)
		return Mailbox{Address: a}, err
	}
	m, err := mail.ParseAddress(s)
	if err != nil {
		return Mailbox{}, fmt.Errorf("%w: %w", ErrInvalidAddress, err)
	}
	a, err := ParseAddress(m.Address)
	if err != nil {
		return Mailbox{}, err
	}
	if strings.ContainsAny(m.Name, "\r\n") {
		return Mailbox{}, fmt.Errorf("%w: line break in display name", ErrInvalidAddress)
	}
	return Mailbox{Name: m.Name, Address: a}, nil
}

// String returns the normalised address.
func (a Address) String() string { return a.Local + "@" + a.Domain }

// IsZero reports whether the address is unset.
func (a Address) IsZero() bool { return a.Local == "" && a.Domain == "" }

// Hash is the SHA-256 of the normalised address. It is what remains after an
// erasure request, and the only form of an address allowed in logs.
func (a Address) Hash() [sha256.Size]byte { return sha256.Sum256([]byte(a.String())) }

// String formats the mailbox for a header, without any encoding: callers that
// build MIME must go through a header encoder for non-ASCII names.
func (m Mailbox) String() string {
	if m.Name == "" {
		return m.Address.String()
	}
	return (&mail.Address{Name: m.Name, Address: m.Address.String()}).String()
}
