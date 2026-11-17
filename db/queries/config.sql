-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- name: GetCredentialByPrefix :one
SELECT id, tenant_id, project_id, secret_hash, scopes, revoked_at
FROM api_credentials
WHERE prefix = $1;

-- name: GetSendingDomain :one
SELECT id, tenant_id, project_id, domain, return_path_domain, dkim_selector, verified_at
FROM sending_domains
WHERE domain = $1 AND deleted_at IS NULL;

-- name: UpsertTenant :one
INSERT INTO tenants (id, name) VALUES ($1, $2)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;

-- name: UpsertProject :one
INSERT INTO projects (id, tenant_id, name) VALUES ($1, $2, $3)
ON CONFLICT (tenant_id, name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;

-- name: UpsertSendingDomain :one
INSERT INTO sending_domains (id, tenant_id, project_id, domain, return_path_domain, dkim_selector, verified_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (domain) WHERE deleted_at IS NULL DO UPDATE
  SET project_id = EXCLUDED.project_id,
      return_path_domain = EXCLUDED.return_path_domain,
      dkim_selector = EXCLUDED.dkim_selector,
      -- The date of the first verification is kept while the domain stays
      -- verified.
      verified_at = CASE
        WHEN EXCLUDED.verified_at IS NULL THEN NULL
        ELSE COALESCE(sending_domains.verified_at, EXCLUDED.verified_at)
      END
  -- No row comes back when the domain belongs to another tenant: a tenant
  -- cannot take over, or alter, the domain of another.
  WHERE sending_domains.tenant_id = EXCLUDED.tenant_id
RETURNING id;

-- name: InsertCredential :exec
INSERT INTO api_credentials (id, tenant_id, project_id, name, prefix, secret_hash, scopes)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: RetireSendingDomain :execrows
-- The resource is gone: the domain can no longer send, and is free to be
-- declared again.
UPDATE sending_domains
SET deleted_at = now(), verified_at = NULL
WHERE domain = sqlc.arg(domain) AND tenant_id = sqlc.arg(tenant_id) AND deleted_at IS NULL;

-- name: UpsertCredentialByExternalID :exec
-- The Secret holding the key is the source of truth: whatever key it holds
-- is the one that works, and a revoked credential that reappears is live.
INSERT INTO api_credentials (id, tenant_id, project_id, name, prefix, secret_hash, scopes, external_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (external_id) DO UPDATE
  SET project_id = EXCLUDED.project_id,
      name = EXCLUDED.name,
      prefix = EXCLUDED.prefix,
      secret_hash = EXCLUDED.secret_hash,
      scopes = EXCLUDED.scopes,
      revoked_at = NULL;

-- name: RevokeCredentialByExternalID :execrows
UPDATE api_credentials SET revoked_at = now()
WHERE external_id = $1 AND revoked_at IS NULL;
