-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- name: ClaimIdempotencyKey :execrows
-- Inserted first in the transaction that accepts the message. A concurrent
-- request with the same key blocks on the primary key until this transaction
-- ends, then sees 0 rows and replays the stored response. An expired key is
-- taken over in place.
INSERT INTO idempotency_keys (project_id, key, request_hash, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (project_id, key) DO UPDATE
  SET request_hash = EXCLUDED.request_hash,
      status_code = NULL,
      response = NULL,
      created_at = now(),
      expires_at = EXCLUDED.expires_at
  WHERE idempotency_keys.expires_at <= now();

-- name: GetIdempotencyKey :one
SELECT request_hash, status_code, response
FROM idempotency_keys
WHERE project_id = $1 AND key = $2;

-- name: CompleteIdempotencyKey :exec
UPDATE idempotency_keys
SET status_code = $3, response = $4
WHERE project_id = $1 AND key = $2;

-- name: DeleteExpiredIdempotencyKeys :execrows
DELETE FROM idempotency_keys WHERE expires_at <= now();
