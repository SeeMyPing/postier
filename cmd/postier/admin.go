// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/platform"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
)

// runSeed stands in for the operator until v0.1: it writes the rows that the
// Tenant, SendingDomain and ApiCredential CRDs will mirror. It is idempotent,
// except that each run without --api-key issues a new key.
func runSeed(ctx context.Context, args []string) error {
	var (
		dbURL      string
		tenant     string
		project    string
		sendDomain string
		returnPath string
		selector   string
		keyEnv     string
		apiKey     string
		unverified bool
	)
	f := platform.NewFlags("seed")
	f.String(&tenant, "tenant", "default", "tenant name")
	f.String(&project, "project", "default", "project name")
	f.String(&sendDomain, "domain", "", "sending domain, e.g. notifications.acme.fr")
	f.String(&returnPath, "return-path-domain", "", "return-path domain (default: bounce.<domain>)")
	f.String(&selector, "selector", "pst1", "DKIM selector")
	f.String(&keyEnv, "key-env", "live", "environment tag of the API key: live or test")
	f.Bool(&unverified, "unverified", false, "register the domain without marking it verified")
	f.Secret(&dbURL, "system-database-url")
	// For reproducible test benches only: register this key instead of
	// generating one. Read from the environment, like every secret.
	f.Secret(&apiKey, "seed-api-key")
	if err := f.Parse(args); err != nil {
		return err
	}
	if err := platform.Require(map[string]string{"system-database-url": dbURL, "domain": sendDomain}); err != nil {
		return err
	}

	// Normalise the domain exactly like a From address will be.
	probe, err := domain.ParseAddress("probe@" + sendDomain)
	if err != nil {
		return fmt.Errorf("--domain: %w", err)
	}
	sendDomain = probe.Domain
	if returnPath == "" {
		returnPath = "bounce." + sendDomain
	}

	var key accept.Key
	if apiKey != "" {
		prefix, secret, err := accept.ParseKey(apiKey)
		if err != nil {
			return fmt.Errorf("POSTIER_SEED_API_KEY: %w", err)
		}
		key = accept.Key{Token: apiKey, Prefix: prefix, SecretHash: accept.HashSecret(secret)}
	} else if key, err = accept.GenerateKey(keyEnv); err != nil {
		return err
	}

	pool, err := store.OpenSystem(ctx, dbURL, "seed")
	if err != nil {
		return err
	}
	defer pool.Close()

	issued := true
	err = pool.InTx(ctx, func(q *store.Queries, _ pgx.Tx) error {
		tenantID, err := q.UpsertTenant(ctx, sqlcgen.UpsertTenantParams{ID: domain.NewID(), Name: tenant})
		if err != nil {
			return err
		}
		projectID, err := q.UpsertProject(ctx, sqlcgen.UpsertProjectParams{ID: domain.NewID(), TenantID: tenantID, Name: project})
		if err != nil {
			return err
		}
		var verifiedAt *time.Time
		if !unverified {
			now := time.Now().UTC()
			verifiedAt = &now
		}
		_, err = q.UpsertSendingDomain(ctx, sqlcgen.UpsertSendingDomainParams{
			ID: domain.NewID(), TenantID: tenantID, ProjectID: projectID,
			Domain: sendDomain, ReturnPathDomain: returnPath, DkimSelector: selector, VerifiedAt: verifiedAt,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("domain %s already belongs to another tenant", sendDomain)
		}
		if err != nil {
			return err
		}

		if _, err := q.GetCredentialByPrefix(ctx, key.Prefix); err == nil {
			issued = false // the fixed key of a test bench, already registered
			return nil
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		return q.InsertCredential(ctx, sqlcgen.InsertCredentialParams{
			ID: domain.NewID(), TenantID: tenantID, ProjectID: projectID, Name: "seed",
			Prefix: key.Prefix, SecretHash: key.SecretHash,
			Scopes: []string{accept.ScopeSend, accept.ScopeRead},
		})
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "tenant %q, project %q, sending domain %s (return path %s, selector %s)\n",
		tenant, project, sendDomain, returnPath, selector)
	switch {
	case apiKey != "" && !issued:
		fmt.Fprintln(os.Stderr, "the provided API key was already registered")
	case apiKey != "":
		fmt.Fprintln(os.Stderr, "the provided API key is registered")
	default:
		fmt.Fprintln(os.Stderr, "API key (shown once, only its hash is stored):")
		// The key alone on stdout, so that it can be captured by a script.
		fmt.Println(key.Token)
	}
	return nil
}

// runDKIM generates the signing key of a domain. Until the operator manages
// keys (v0.1), this is the manual step of setting up a sending domain.
func runDKIM(_ context.Context, args []string) error {
	var (
		sendDomain string
		selector   string
		out        string
		bits       int
		ifMissing  bool
	)
	f := platform.NewFlags("dkim")
	f.String(&sendDomain, "domain", "", "sending domain")
	f.String(&selector, "selector", "pst1", "DKIM selector")
	f.String(&out, "out", "", "file to write the private key to (PEM, PKCS#8)")
	f.Int(&bits, "bits", 2048, "RSA key size")
	f.Bool(&ifMissing, "if-missing", false, "do nothing, successfully, when the key file already exists")
	if err := f.Parse(args); err != nil {
		return err
	}
	if err := platform.Require(map[string]string{"domain": sendDomain, "out": out}); err != nil {
		return err
	}
	if bits < 2048 {
		return errors.New("--bits: receivers reject DKIM keys shorter than 2048 bits")
	}
	if _, err := os.Stat(out); err == nil && ifMissing {
		fmt.Fprintf(os.Stderr, "%s already exists, left untouched\n", out)
		return nil
	}
	if _, err := os.Stat(out); err == nil {
		// Overwriting a key in use would break signatures until DNS follows.
		return fmt.Errorf("%s already exists: remove it or choose another selector", out)
	}

	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return err
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return err
	}
	// 0644: KumoMTA drops to an unprivileged user and must read the file.
	// In Kubernetes the key lives in a Secret and the mode is set there.
	if err := os.WriteFile(out, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0o644); err != nil { //nolint:gosec // see above
		return err
	}
	fmt.Fprintf(os.Stderr, "private key written to %s\n\nPublish this DNS record:\n\n", out)
	fmt.Printf("%s._domainkey.%s. IN TXT \"v=DKIM1; k=rsa; p=%s\"\n",
		selector, sendDomain, base64.StdEncoding.EncodeToString(public))
	return nil
}
