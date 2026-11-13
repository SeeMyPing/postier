// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package domain

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"pgregory.net/rapid"
)

func TestIDRoundTripsItsTimestamp(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ms := rapid.Int64Range(0, 1<<47).Draw(t, "ms")
		at := time.UnixMilli(ms).UTC()
		id := NewIDAt(at)
		if id.Version() != 7 || id.Variant() != uuid.RFC4122 {
			t.Fatalf("not a UUIDv7: %s", id)
		}
		got, err := IDTime(id)
		if err != nil {
			t.Fatal(err)
		}
		if !got.Equal(at) {
			t.Fatalf("IDTime = %s, want %s", got, at)
		}
	})
}

func TestIDTimeRejectsOtherVersions(t *testing.T) {
	if _, err := IDTime(uuid.New()); !errors.Is(err, ErrNotUUIDv7) {
		t.Fatalf("UUIDv4 accepted: %v", err)
	}
}

func TestIDsSortByTime(t *testing.T) {
	a := NewIDAt(time.UnixMilli(1_700_000_000_000))
	b := NewIDAt(time.UnixMilli(1_700_000_000_001))
	if a.String() >= b.String() {
		t.Fatalf("%s should sort before %s", a, b)
	}
}

func TestCompactID(t *testing.T) {
	id := NewID()
	c := CompactID(id)
	if len(c) != 32 || strings.ContainsRune(c, '-') {
		t.Fatalf("CompactID = %q", c)
	}
	back, err := ParseCompactID(c)
	if err != nil || back != id {
		t.Fatalf("ParseCompactID(%q) = %s, %v", c, back, err)
	}
}

func TestParseAddress(t *testing.T) {
	cases := []struct{ in, want string }{
		{"User@Example.ORG", "user@example.org"},
		{"  user@example.org  ", "user@example.org"},
		{"user@example.org.", "user@example.org"},
		{"user+tag@example.org", "user+tag@example.org"},
		{"first.last@sub.example.org", "first.last@sub.example.org"},
		// IDN: unicode and punycode spellings are the same mailbox.
		{"info@Bücher.example", "info@xn--bcher-kva.example"},
		{"info@xn--bcher-kva.example", "info@xn--bcher-kva.example"},
		// NFC: "é" precomposed and "e" + combining acute are the same mailbox.
		{"José@example.org", "josé@example.org"},
		{"José@example.org", "josé@example.org"},
	}
	for _, c := range cases {
		got, err := ParseAddress(c.in)
		if err != nil {
			t.Errorf("ParseAddress(%q): %v", c.in, err)
			continue
		}
		if got.String() != c.want {
			t.Errorf("ParseAddress(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseAddressRejects(t *testing.T) {
	for _, in := range []string{
		"", "user", "@example.org", "user@", "user@localhost", "user@[192.0.2.1]",
		"a b@example.org", ".user@example.org", "user.@example.org", "us..er@example.org",
		"\"quoted\"@example.org", "user@exa mple.org", "user@-example.org",
		"user\r\n@example.org", "user@example.org\r\nBcc: x@y.z",
		strings.Repeat("a", 65) + "@example.org",
		"user@" + strings.Repeat("a", 64) + ".org",
	} {
		if a, err := ParseAddress(in); err == nil {
			t.Errorf("ParseAddress(%q) accepted as %q", in, a)
		} else if !errors.Is(err, ErrInvalidAddress) {
			t.Errorf("ParseAddress(%q): error %v does not wrap ErrInvalidAddress", in, err)
		}
	}
}

func TestEquivalentSpellingsShareAHash(t *testing.T) {
	a, _ := ParseAddress("José@Bücher.example")
	b, _ := ParseAddress("josé@xn--bcher-kva.example")
	if a.Hash() != b.Hash() {
		t.Fatal("equivalent spellings must hash alike")
	}
	c, _ := ParseAddress("other@xn--bcher-kva.example")
	if a.Hash() == c.Hash() {
		t.Fatal("different mailboxes must not collide")
	}
}

// TestNormalisationIsIdempotent: normalising an already normalised address is
// a no-op, so a stored address can always be re-parsed.
func TestNormalisationIsIdempotent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		local := rapid.StringMatching(`[A-Za-z0-9éÉüñ][A-Za-z0-9éÉüñ+_-]{0,20}`).Draw(t, "local")
		dom := rapid.StringMatching(`[A-Za-z][A-Za-z0-9]{0,10}\.(org|example|fr)`).Draw(t, "domain")
		a, err := ParseAddress(local + "@" + dom)
		if err != nil {
			t.Skip(err)
		}
		b, err := ParseAddress(a.String())
		if err != nil {
			t.Fatalf("normalised form %q does not parse: %v", a, err)
		}
		if a != b {
			t.Fatalf("not idempotent: %q then %q", a, b)
		}
	})
}

func TestParseMailbox(t *testing.T) {
	m, err := ParseMailbox("Acme <No-Reply@Notifications.Acme.FR>")
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "Acme" || m.Address.String() != "no-reply@notifications.acme.fr" {
		t.Fatalf("got %+v", m)
	}
	if m, err = ParseMailbox("plain@example.org"); err != nil || m.Name != "" {
		t.Fatalf("bare address: %+v, %v", m, err)
	}
	if _, err = ParseMailbox("Acme <a@example.org>, Evil <b@example.org>"); err == nil {
		t.Fatal("an address list must not parse as one mailbox")
	}
}

func testSigner(t *testing.T) *Signer {
	t.Helper()
	s, err := NewSigner([]byte(strings.Repeat("k", MinSigningKeyLen)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSignerRejectsShortKey(t *testing.T) {
	if _, err := NewSigner([]byte("short")); err == nil {
		t.Fatal("short key accepted")
	}
}

func TestReturnPathRoundTrip(t *testing.T) {
	s := testSigner(t)
	id := NewID()
	rp := s.ReturnPath(id, "bounce.acme.example")
	local, _, _ := strings.Cut(rp, "@")
	if len(local) > maxLocalLen {
		t.Fatalf("local part too long (%d): %s", len(local), rp)
	}
	for _, form := range []string{rp, "<" + rp + ">", strings.ToUpper(rp)} {
		got, err := s.ParseReturnPath(form)
		if err != nil || got != id {
			t.Errorf("ParseReturnPath(%q) = %s, %v", form, got, err)
		}
	}
}

func TestReturnPathForgery(t *testing.T) {
	s := testSigner(t)
	id := NewID()
	rp := s.ReturnPath(id, "bounce.acme.example")

	// Same shape, another id: the attacker keeps a tag seen elsewhere.
	other := NewID()
	forged := strings.Replace(rp, CompactID(id), CompactID(other), 1)
	if _, err := s.ParseReturnPath(forged); !errors.Is(err, ErrBadSignature) {
		t.Errorf("forged id accepted: %v", err)
	}
	// A tag taken from the public Message-ID must not open the return path.
	mid := strings.Trim(s.MessageID(id, "acme.example"), "<>")
	midLocal, _, _ := strings.Cut(mid, "@")
	if _, err := s.ParseReturnPath("b+" + midLocal + "@bounce.acme.example"); !errors.Is(err, ErrBadSignature) {
		t.Errorf("Message-ID tag accepted as return path: %v", err)
	}
	// Another installation, another key.
	other2, _ := NewSigner([]byte(strings.Repeat("z", MinSigningKeyLen)))
	if _, err := other2.ParseReturnPath(rp); !errors.Is(err, ErrBadSignature) {
		t.Errorf("foreign key accepted: %v", err)
	}
	for _, junk := range []string{"", "postmaster@acme.example", "b+short.aaaaaaaa@x.example", "b+nodot@x.example"} {
		if _, err := s.ParseReturnPath(junk); !errors.Is(err, ErrNotSigned) {
			t.Errorf("ParseReturnPath(%q) = %v, want ErrNotSigned", junk, err)
		}
	}
}

func TestMessageIDRoundTrip(t *testing.T) {
	s := testSigner(t)
	id := NewID()
	mid := s.MessageID(id, "acme.example")
	if !strings.HasPrefix(mid, "<") || !strings.HasSuffix(mid, "@acme.example>") {
		t.Fatalf("MessageID = %q", mid)
	}
	got, err := s.ParseMessageID(mid)
	if err != nil || got != id {
		t.Fatalf("ParseMessageID = %s, %v", got, err)
	}
	if _, err := s.ParseMessageID("<random.value@elsewhere.example>"); !errors.Is(err, ErrNotSigned) {
		t.Fatalf("foreign Message-ID: %v", err)
	}
}
