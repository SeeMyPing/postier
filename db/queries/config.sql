-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- name: GetCredentialByPrefix :one
SELECT id, tenant_id, project_id, secret_hash, scopes, revoked_at
FROM api_credentials
WHERE prefix = $1;

-- name: GetSendingDomain :one
SELECT id, tenant_id, project_id, domain, return_path_domain, dkim_selector, verified_at
FROM sending_domains
WHERE domain = $1;

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
ON CONFLICT (domain) DO UPDATE
  SET return_path_domain = EXCLUDED.return_path_domain,
      dkim_selector = EXCLUDED.dkim_selector,
      verified_at = EXCLUDED.verified_at
  WHERE sending_domains.tenant_id = EXCLUDED.tenant_id
RETURNING id;

-- name: InsertCredential :exec
INSERT INTO api_credentials (id, tenant_id, project_id, name, prefix, secret_hash, scopes)
VALUES ($1, $2, $3, $4, $5, $6, $7);
