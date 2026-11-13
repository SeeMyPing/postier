-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- Every query on messages or events gives the partition key (created_at,
-- occurred_at), derived from the UUIDv7, so that it prunes to one partition.

-- name: InsertContent :exec
INSERT INTO message_contents (id, tenant_id, created_at, encoding, mime, sha256, size_bytes)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: InsertMessage :exec
INSERT INTO messages (
  id, tenant_id, project_id, batch_id, content_id, sending_domain_id, source,
  from_addr, rcpt_addr, rcpt_domain, rcpt_hash, subject, status, status_at,
  tags, metadata, size_bytes, trace_id, created_at
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
);

-- name: GetMessage :one
SELECT id, batch_id, source, from_addr, rcpt_addr, subject, status, status_at,
       attempts, tags, metadata, created_at
FROM messages
WHERE id = $1 AND created_at = $2 AND project_id = $3;

-- name: GetMessageForDispatch :one
SELECT m.id, m.tenant_id, m.status, m.rcpt_addr, m.trace_id, m.created_at,
       d.domain AS sending_domain, d.return_path_domain, d.verified_at AS domain_verified_at,
       c.encoding AS content_encoding, c.mime AS content_mime
FROM messages m
JOIN sending_domains d ON d.id = m.sending_domain_id
LEFT JOIN message_contents c ON c.id = m.content_id
WHERE m.id = $1 AND m.created_at = $2;

-- name: TransitionMessage :execrows
-- The precedence rule, atomically: the status only changes if the current one
-- is in the allowed set, computed by domain.AllowedFrom. An event that
-- confirms the current status (a second deferral) only raises the attempt
-- count and leaves status_at alone. Anything else matches no row: the event
-- is kept in the history, the status does not move.
UPDATE messages
SET status_at = CASE WHEN status = sqlc.arg(status) THEN status_at ELSE sqlc.arg(status_at) END,
    status = sqlc.arg(status),
    attempts = GREATEST(attempts, sqlc.arg(attempts)::int)
WHERE id = sqlc.arg(id) AND created_at = sqlc.arg(created_at)
  AND (status = ANY(sqlc.arg(allowed_from)::text[]) OR status = sqlc.arg(status));

-- name: GetMessageRef :one
SELECT tenant_id, status FROM messages WHERE id = $1 AND created_at = $2;

-- name: InsertEvent :execrows
INSERT INTO events (
  id, message_id, tenant_id, type, occurred_at, dedup_key,
  smtp_code, enhanced_code, bounce_class, remote_mx, source_ip, detail
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (dedup_key, occurred_at) DO NOTHING;

-- name: ListMessageEvents :many
SELECT id, type, occurred_at, smtp_code, enhanced_code, bounce_class, remote_mx, detail
FROM events
WHERE message_id = sqlc.arg(message_id) AND tenant_id = sqlc.arg(tenant_id)
  AND occurred_at >= sqlc.arg(since)
ORDER BY occurred_at, id;

-- name: HasDeliveryEvent :one
-- Whether KumoMTA already reported anything for this message: the guard
-- against injecting twice after a dispatch crash.
SELECT EXISTS (
  SELECT 1 FROM events
  WHERE message_id = sqlc.arg(message_id) AND occurred_at >= sqlc.arg(since)
    AND type <> 'accepted'
);

-- name: EnsurePartitions :one
SELECT postier_ensure_partitions(sqlc.arg(weeks_ahead)::int)::int AS created;

-- name: InsertDeadRecord :exec
INSERT INTO kumo_dead_records (error, record) VALUES ($1, $2);

-- name: RaiseAttempts :exec
-- For an event that did not move the status (it arrived after a later one):
-- the attempt count it reports still stands.
UPDATE messages
SET attempts = sqlc.arg(attempts)::int
WHERE id = sqlc.arg(id) AND created_at = sqlc.arg(created_at)
  AND attempts < sqlc.arg(attempts)::int;
