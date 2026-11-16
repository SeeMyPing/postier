-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- name: FindSuppressions :many
-- Which of these addresses are suppressed for this project, by a suppression
-- of the project or of the whole tenant.
SELECT addr_hash, reason
FROM suppressions
WHERE tenant_id = sqlc.arg(tenant_id)
  AND (project_id IS NULL OR project_id = sqlc.arg(project_id))
  AND addr_hash = ANY(sqlc.arg(hashes)::bytea[])
  AND (expires_at IS NULL OR expires_at > now());

-- name: UpsertSuppression :one
-- Adding a suppression never weakens one that is still in force: a permanent
-- one stays permanent, a temporary one can only be extended. An expired row
-- is simply taken over.
INSERT INTO suppressions (id, tenant_id, project_id, addr_hash, addr, reason, message_id, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, project_id, addr_hash) DO UPDATE
SET addr = EXCLUDED.addr,
    reason = CASE
      WHEN suppressions.expires_at IS NULL AND EXCLUDED.expires_at IS NOT NULL THEN suppressions.reason
      ELSE EXCLUDED.reason
    END,
    message_id = COALESCE(EXCLUDED.message_id, suppressions.message_id),
    expires_at = CASE
      WHEN suppressions.expires_at IS NOT NULL AND suppressions.expires_at <= now() THEN EXCLUDED.expires_at
      WHEN suppressions.expires_at IS NULL OR EXCLUDED.expires_at IS NULL THEN NULL
      ELSE GREATEST(suppressions.expires_at, EXCLUDED.expires_at)
    END
RETURNING id, project_id, addr, reason, expires_at, created_at, (xmax = 0)::boolean AS created;

-- name: ListSuppressions :many
-- Newest first. Identifiers are UUIDv7, so ordering by id is ordering by
-- creation time, and the last id of a page is the cursor of the next.
SELECT id, project_id, addr, reason, expires_at, created_at
FROM suppressions
WHERE tenant_id = sqlc.arg(tenant_id)
  AND (project_id IS NULL OR project_id = sqlc.arg(project_id))
  AND (expires_at IS NULL OR expires_at > now())
  AND (sqlc.narg(addr_hash)::bytea IS NULL OR addr_hash = sqlc.narg(addr_hash))
  AND (sqlc.narg(before_id)::uuid IS NULL OR id < sqlc.narg(before_id))
ORDER BY id DESC
LIMIT sqlc.arg(max_rows);

-- name: GetSuppression :one
SELECT id, project_id
FROM suppressions
WHERE id = sqlc.arg(id) AND tenant_id = sqlc.arg(tenant_id)
  AND (project_id IS NULL OR project_id = sqlc.arg(project_id));

-- name: DeleteSuppression :execrows
DELETE FROM suppressions WHERE id = sqlc.arg(id) AND tenant_id = sqlc.arg(tenant_id);

-- name: DeleteExpiredSuppressions :execrows
DELETE FROM suppressions WHERE expires_at IS NOT NULL AND expires_at <= now();

-- name: CountSoftFailures :one
-- How many messages to this recipient ended in failure, since a given date,
-- after a response of one of the given classes (mailbox full…). Failures
-- that say nothing about the recipient, a blocked sending IP for instance,
-- carry other classes and are not counted.
SELECT count(DISTINCT m.id)
FROM messages m
JOIN events e ON e.message_id = m.id AND e.occurred_at >= sqlc.arg(since)
WHERE m.project_id = sqlc.arg(project_id)
  AND m.rcpt_hash = sqlc.arg(rcpt_hash)
  AND m.created_at >= sqlc.arg(since)
  AND m.status IN ('bounced', 'expired')
  AND e.bounce_class = ANY(sqlc.arg(classes)::text[]);
