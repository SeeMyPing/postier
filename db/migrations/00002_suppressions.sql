-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- +goose Up

-- Addresses Postier must not send to any more: they bounced for good, their
-- owner complained, or someone added them by hand.
--
-- A suppression belongs to a project, or to the whole tenant when project_id
-- is NULL (complaints: one angry recipient must not hear from any project of
-- the tenant again). It is keyed on the hash of the normalised address. The
-- address itself is kept for display only and is erased on request, the
-- hash staying behind so that the suppression still holds.
CREATE TABLE suppressions (
  id          uuid        PRIMARY KEY,
  tenant_id   uuid        NOT NULL REFERENCES tenants (id),
  project_id  uuid        REFERENCES projects (id),
  addr_hash   bytea       NOT NULL,
  addr        text,
  reason      text        NOT NULL,   -- hard_bounce | complaint | soft_bounces | manual
  message_id  uuid,                   -- the message that caused it, if any
  expires_at  timestamptz,            -- NULL: permanent
  created_at  timestamptz NOT NULL DEFAULT now(),
  -- One row per address and scope. NULLS NOT DISTINCT makes the tenant-wide
  -- scope (project_id IS NULL) unique too.
  UNIQUE NULLS NOT DISTINCT (tenant_id, project_id, addr_hash)
);

-- The lookup at acceptance: by tenant and hash, whatever the scope.
CREATE INDEX suppressions_lookup_idx ON suppressions (tenant_id, addr_hash);

-- Past sends to one recipient: counts repeated soft bounces, and will serve
-- the search by recipient.
CREATE INDEX messages_rcpt_idx ON messages (project_id, rcpt_hash, created_at DESC);

-- The purge of message bodies walks them oldest first.
CREATE INDEX message_contents_created_idx ON message_contents (created_at);

-- +goose Down
DROP INDEX message_contents_created_idx;
DROP INDEX messages_rcpt_idx;
DROP TABLE suppressions;
