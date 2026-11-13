// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package accept is the pipeline shared by every way of submitting mail:
// authenticate, validate, check the sending domain, render, then persist the
// messages and their injection jobs in one transaction.
package accept

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/store"
)

// Scopes of an API credential.
const (
	ScopeSend         = "send"
	ScopeRead         = "read"
	ScopeSuppressions = "suppressions"
	ScopeAdmin        = "admin"
)

// Principal is an authenticated caller.
type Principal struct {
	CredentialID uuid.UUID
	TenantID     uuid.UUID
	ProjectID    uuid.UUID
	Scopes       []string
}

// Can reports whether the principal holds a scope. admin implies the others.
func (p Principal) Can(scope string) bool {
	return slices.Contains(p.Scopes, scope) || slices.Contains(p.Scopes, ScopeAdmin)
}

// An API key is "pst_<env>_<prefix>_<secret>":
//
//	env     "live" or "test", so that a leaked key says what it opens
//	prefix  8 characters, public, indexed: it finds the credential
//	secret  32 random bytes, base64url. Only its SHA-256 is stored.
//
// A fast hash is enough here, and is what makes authentication cheap: the
// secret has 256 bits of entropy, there is nothing to brute-force. Password
// hashing (argon2, bcrypt) exists for low-entropy secrets.
const (
	keyScheme = "pst"
	prefixLen = 8
	secretLen = 32
)

var (
	reEnv    = regexp.MustCompile(`^[a-z]{2,16}$`)
	rePrefix = regexp.MustCompile(`^[a-z2-7]{8}$`)

	prefixEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)
)

// Key is a freshly generated API key. Token is shown once and never stored.
type Key struct {
	Token      string
	Prefix     string
	SecretHash []byte
}

// GenerateKey creates a new API key for the given environment.
func GenerateKey(env string) (Key, error) {
	if !reEnv.MatchString(env) {
		return Key{}, fmt.Errorf("invalid key environment %q", env)
	}
	// crypto/rand.Read never fails on supported platforms (Go 1.24+).
	raw := make([]byte, 5+secretLen)
	_, _ = rand.Read(raw)
	prefix := strings.ToLower(prefixEncoding.EncodeToString(raw[:5]))
	secret := base64.RawURLEncoding.EncodeToString(raw[5:])
	return Key{
		Token:      strings.Join([]string{keyScheme, env, prefix, secret}, "_"),
		Prefix:     prefix,
		SecretHash: HashSecret(secret),
	}, nil
}

// ParseKey splits a token into its prefix and secret, checking its shape.
func ParseKey(token string) (prefix, secret string, err error) {
	// The secret is base64url and may itself contain underscores: split in
	// four at most.
	parts := strings.SplitN(token, "_", 4)
	if len(parts) != 4 || parts[0] != keyScheme || !reEnv.MatchString(parts[1]) ||
		!rePrefix.MatchString(parts[2]) {
		return "", "", errors.New("malformed API key")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil || len(raw) != secretLen {
		return "", "", errors.New("malformed API key")
	}
	return parts[2], parts[3], nil
}

// HashSecret returns what is stored for a secret.
func HashSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// CredentialStore finds a credential by the public prefix of its key.
type CredentialStore interface {
	CredentialByPrefix(ctx context.Context, prefix string) (store.Credential, error)
}

// Authenticator turns a bearer token into a Principal.
type Authenticator struct {
	store CredentialStore
}

// NewAuthenticator returns an Authenticator backed by the given store.
func NewAuthenticator(s CredentialStore) *Authenticator {
	return &Authenticator{store: s}
}

var errUnauthorized = domain.Errorf(domain.CodeUnauthorized, "missing or invalid API key")

// dummyHash stands in for the stored hash when the prefix is unknown, so
// that an unknown prefix and a wrong secret take the same code path.
var dummyHash = HashSecret("postier-unknown-credential")

// Authenticate validates an API key. Every failure returns the same error:
// the caller must not learn whether the prefix exists.
func (a *Authenticator) Authenticate(ctx context.Context, token string) (Principal, error) {
	prefix, secret, err := ParseKey(token)
	if err != nil {
		return Principal{}, errUnauthorized
	}
	cred, err := a.store.CredentialByPrefix(ctx, prefix)
	found := err == nil
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return Principal{}, fmt.Errorf("credential lookup: %w", err)
	}
	want := dummyHash
	if found {
		want = cred.SecretHash
	}
	match := subtle.ConstantTimeCompare(HashSecret(secret), want) == 1
	if !found || !match || cred.Revoked {
		return Principal{}, errUnauthorized
	}
	return Principal{
		CredentialID: cred.ID,
		TenantID:     cred.TenantID,
		ProjectID:    cred.ProjectID,
		Scopes:       cred.Scopes,
	}, nil
}
