// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package accept

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/store"
)

type fakeCredentials struct {
	byPrefix map[string]store.Credential
	err      error
	lookups  int
}

func (f *fakeCredentials) CredentialByPrefix(_ context.Context, prefix string) (store.Credential, error) {
	f.lookups++
	if f.err != nil {
		return store.Credential{}, f.err
	}
	c, ok := f.byPrefix[prefix]
	if !ok {
		return store.Credential{}, store.ErrNotFound
	}
	return c, nil
}

func newKey(t *testing.T) (Key, *fakeCredentials) {
	t.Helper()
	key, err := GenerateKey("live")
	if err != nil {
		t.Fatal(err)
	}
	creds := &fakeCredentials{byPrefix: map[string]store.Credential{
		key.Prefix: {
			ID: domain.NewID(), TenantID: domain.NewID(), ProjectID: domain.NewID(),
			SecretHash: key.SecretHash, Scopes: []string{ScopeSend},
		},
	}}
	return key, creds
}

func isUnauthorized(err error) bool {
	var de *domain.Error
	return errors.As(err, &de) && de.Code == domain.CodeUnauthorized
}

func TestGenerateKey(t *testing.T) {
	a, err := GenerateKey("live")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := GenerateKey("live")
	if a.Token == b.Token || a.Prefix == b.Prefix {
		t.Fatal("two generated keys are equal")
	}
	if !strings.HasPrefix(a.Token, "pst_live_"+a.Prefix+"_") {
		t.Errorf("token = %q", a.Token)
	}
	prefix, secret, err := ParseKey(a.Token)
	if err != nil || prefix != a.Prefix {
		t.Fatalf("ParseKey: %q, %v", prefix, err)
	}
	// What is stored never contains the secret.
	if strings.Contains(string(a.SecretHash), secret) || len(a.SecretHash) != 32 {
		t.Error("the stored hash is not a SHA-256")
	}
	if _, err := GenerateKey("Live!"); err == nil {
		t.Error("invalid environment accepted")
	}
}

func TestParseKeyRejects(t *testing.T) {
	good, _ := GenerateKey("test")
	for _, token := range []string{
		"", "pst", "pst_live", "pst_live_abcdefgh",
		"sk_live_abcdefgh_" + strings.Repeat("A", 43),
		"pst_LIVE_abcdefgh_" + strings.Repeat("A", 43),
		"pst_live_ABCDEFGH_" + strings.Repeat("A", 43),
		"pst_live_short_" + strings.Repeat("A", 43),
		"pst_live_abcdefgh_tooshort",
		"pst_live_abcdefgh_" + strings.Repeat("!", 43),
		good.Token + "A",
		" " + good.Token,
	} {
		if _, _, err := ParseKey(token); err == nil {
			t.Errorf("ParseKey(%q) accepted", token)
		}
	}
}

func TestAuthenticate(t *testing.T) {
	key, creds := newKey(t)
	auth := NewAuthenticator(creds)

	p, err := auth.Authenticate(context.Background(), key.Token)
	if err != nil {
		t.Fatal(err)
	}
	want := creds.byPrefix[key.Prefix]
	if p.TenantID != want.TenantID || p.ProjectID != want.ProjectID || p.CredentialID != want.ID {
		t.Errorf("principal = %+v", p)
	}
	if !p.Can(ScopeSend) || p.Can(ScopeRead) {
		t.Errorf("scopes = %v", p.Scopes)
	}
}

func TestAuthenticateFailuresAreIndistinguishable(t *testing.T) {
	key, creds := newKey(t)
	auth := NewAuthenticator(creds)
	other, _ := GenerateKey("live")

	// Right prefix, wrong secret.
	_, wrongSecret, _ := ParseKey(other.Token)
	forged := "pst_live_" + key.Prefix + "_" + wrongSecret

	revokedKey, _ := GenerateKey("live")
	creds.byPrefix[revokedKey.Prefix] = store.Credential{SecretHash: revokedKey.SecretHash, Revoked: true}

	var messages []string
	for name, token := range map[string]string{
		"unknown prefix": other.Token,
		"wrong secret":   forged,
		"revoked":        revokedKey.Token,
		"malformed":      "not-a-key",
		"empty":          "",
	} {
		_, err := auth.Authenticate(context.Background(), token)
		if !isUnauthorized(err) {
			t.Errorf("%s: %v", name, err)
			continue
		}
		messages = append(messages, err.Error())
	}
	for _, m := range messages {
		if m != messages[0] {
			t.Errorf("failures can be told apart: %q vs %q", m, messages[0])
		}
	}
}

// A malformed key never reaches the database.
func TestMalformedKeyIsNotLookedUp(t *testing.T) {
	_, creds := newKey(t)
	_, _ = NewAuthenticator(creds).Authenticate(context.Background(), "pst_live_x")
	if creds.lookups != 0 {
		t.Errorf("%d lookups for a malformed key", creds.lookups)
	}
}

// A database outage is not "wrong key": the client must get a 5xx and retry,
// not conclude that its key was revoked.
func TestAuthenticateSurfacesStoreErrors(t *testing.T) {
	key, creds := newKey(t)
	creds.err = errors.New("connection refused")
	_, err := NewAuthenticator(creds).Authenticate(context.Background(), key.Token)
	if err == nil || isUnauthorized(err) {
		t.Fatalf("err = %v", err)
	}
}

func TestAdminImpliesEveryScope(t *testing.T) {
	p := Principal{Scopes: []string{ScopeAdmin}}
	for _, s := range []string{ScopeSend, ScopeRead, ScopeSuppressions, ScopeAdmin} {
		if !p.Can(s) {
			t.Errorf("admin cannot %s", s)
		}
	}
	if (Principal{}).Can(ScopeRead) {
		t.Error("a principal without scopes can read")
	}
}

func TestRequestHashIgnoresMapOrder(t *testing.T) {
	a := Request{From: "a@acme.test", To: []string{"b@example.org"}, Subject: "s", Text: "t",
		Metadata: map[string]string{"a": "1", "b": "2", "c": "3"}}
	b := a
	b.Metadata = map[string]string{"c": "3", "b": "2", "a": "1"}
	if string(hashRequest(a)) != string(hashRequest(b)) {
		t.Error("equal requests hash differently")
	}
	b.Subject = "other"
	if string(hashRequest(a)) == string(hashRequest(b)) {
		t.Error("different requests hash alike")
	}
}
