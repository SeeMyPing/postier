-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- Migrations follow expand/contract: a migration only adds, so that two
-- consecutive Postier versions run on the same schema during a rolling update.
-- Removals come in a later release, once no running version reads the column.

-- +goose Up

-- Two roles, two connection pools (docs/implementation-plan.md §2.5):
--   postier_app     api, smtp, tracking. Subject to row-level security.
--   postier_system  dispatch, ingest, webhooks, operator sync. Works across
--                   tenants, hence BYPASSRLS.
-- A deployment normally creates them beforehand, with LOGIN and a password.
-- They are created here only as a fallback, which needs a superuser.
-- +goose StatementBegin
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postier_app') THEN
    CREATE ROLE postier_app NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postier_system') THEN
    CREATE ROLE postier_system NOLOGIN BYPASSRLS;
  END IF;
END
$$;
-- +goose StatementEnd

-- Configuration tables. From v0.1 they mirror the CRDs; in phase 0 they are
-- filled by `postier seed`.

CREATE TABLE tenants (
  id          uuid        PRIMARY KEY,
  name        text        NOT NULL UNIQUE,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE projects (
  id          uuid        PRIMARY KEY,
  tenant_id   uuid        NOT NULL REFERENCES tenants (id),
  name        text        NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name)
);

CREATE TABLE api_credentials (
  id           uuid        PRIMARY KEY,
  tenant_id    uuid        NOT NULL REFERENCES tenants (id),
  project_id   uuid        NOT NULL REFERENCES projects (id),
  name         text        NOT NULL,
  -- Public part of the key, used for the lookup. The secret is never stored:
  -- only its SHA-256, enough for a 32-byte random value.
  prefix       text        NOT NULL UNIQUE,
  secret_hash  bytea       NOT NULL,
  scopes       text[]      NOT NULL DEFAULT '{send,read}',
  created_at   timestamptz NOT NULL DEFAULT now(),
  revoked_at   timestamptz
);

CREATE TABLE sending_domains (
  id                  uuid        PRIMARY KEY,
  tenant_id           uuid        NOT NULL REFERENCES tenants (id),
  project_id          uuid        NOT NULL REFERENCES projects (id),
  -- Unique across tenants: one tenant cannot send for another's domain.
  domain              text        NOT NULL UNIQUE,
  return_path_domain  text        NOT NULL,
  dkim_selector       text        NOT NULL,
  -- NULL until DNS ownership is proven. An unverified domain cannot send.
  verified_at         timestamptz,
  created_at          timestamptz NOT NULL DEFAULT now()
);

-- The MIME of a send, shared by every recipient of a batch. It is kept until
-- the last of them reaches a terminal status, not until injection: recovering
-- from a lost KumoMTA spool needs it.
CREATE TABLE message_contents (
  id          uuid        PRIMARY KEY,
  tenant_id   uuid        NOT NULL,
  created_at  timestamptz NOT NULL,
  encoding    text        NOT NULL DEFAULT 'zstd',
  mime        bytea       NOT NULL,
  sha256      bytea       NOT NULL,   -- of the uncompressed MIME
  size_bytes  int         NOT NULL    -- uncompressed
);
-- Already compressed by the application: TOAST compression would only burn CPU.
ALTER TABLE message_contents ALTER COLUMN mime SET STORAGE EXTERNAL;

-- One row per recipient. created_at is not now(): it is the timestamp embedded
-- in the UUIDv7 id, so that a lookup by id alone prunes to one partition.
CREATE TABLE messages (
  id                 uuid        NOT NULL,
  tenant_id          uuid        NOT NULL,
  project_id         uuid        NOT NULL,
  batch_id           uuid        NOT NULL,
  content_id         uuid,                    -- NULL once the body is purged
  sending_domain_id  uuid        NOT NULL,
  source             text        NOT NULL,    -- 'api' | 'smtp'
  from_addr          text        NOT NULL,
  rcpt_addr          text        NOT NULL,
  rcpt_domain        text        NOT NULL,
  rcpt_hash          bytea       NOT NULL,    -- SHA-256 of the normalised address
  subject            text,
  status             text        NOT NULL,
  status_at          timestamptz NOT NULL,
  attempts           int         NOT NULL DEFAULT 0,
  send_at            timestamptz,
  tags               text[]      NOT NULL DEFAULT '{}',
  metadata           jsonb       NOT NULL DEFAULT '{}',
  ip_pool            text,
  size_bytes         int,
  trace_id           text,
  created_at         timestamptz NOT NULL,
  PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX messages_project_created_idx ON messages (project_id, created_at DESC);
CREATE INDEX messages_batch_idx ON messages (batch_id);
CREATE INDEX messages_content_idx ON messages (content_id) WHERE content_id IS NOT NULL;

-- Append-only history. The unique constraint must include the partition key;
-- it deduplicates because a replayed log record carries the same occurred_at.
CREATE TABLE events (
  id             uuid        NOT NULL,
  message_id     uuid        NOT NULL,
  tenant_id      uuid        NOT NULL,
  type           text        NOT NULL,
  occurred_at    timestamptz NOT NULL,
  dedup_key      text        NOT NULL,
  smtp_code      int,
  enhanced_code  text,
  bounce_class   text,
  remote_mx      text,
  source_ip      inet,
  detail         jsonb       NOT NULL DEFAULT '{}',
  PRIMARY KEY (id, occurred_at),
  UNIQUE (dedup_key, occurred_at)
) PARTITION BY RANGE (occurred_at);

CREATE INDEX events_message_idx ON events (message_id, occurred_at);

CREATE TABLE idempotency_keys (
  project_id    uuid        NOT NULL,
  key           text        NOT NULL,
  request_hash  bytea       NOT NULL,
  status_code   int,                          -- NULL while the first request runs
  response      jsonb,
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,
  PRIMARY KEY (project_id, key)
);
CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);

-- Log records that ingest could not decode. A record KumoMTA sends in a shape
-- this version does not understand must neither block its batch nor vanish:
-- it is parked here, to be replayed once the decoder is fixed.
CREATE TABLE kumo_dead_records (
  id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  received_at  timestamptz NOT NULL DEFAULT now(),
  error        text        NOT NULL,
  record       jsonb       NOT NULL
);

-- Weekly partitions for messages and events, from last week to weeks_ahead.
-- SECURITY DEFINER: creating a partition requires owning the parent table,
-- which postier_system does not. Called by `postier migrate` and by a
-- periodic job; idempotent.
-- +goose StatementBegin
CREATE FUNCTION postier_ensure_partitions(weeks_ahead int) RETURNS int
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_temp
AS $$
DECLARE
  parent  text;
  wk      timestamptz;
  part    text;
  created int := 0;
BEGIN
  IF weeks_ahead < 0 OR weeks_ahead > 520 THEN
    RAISE EXCEPTION 'weeks_ahead out of range: %', weeks_ahead;
  END IF;
  FOREACH parent IN ARRAY ARRAY['messages', 'events'] LOOP
    FOR i IN -1..weeks_ahead LOOP
      wk := date_trunc('week', now(), 'UTC') + make_interval(weeks => i);
      part := format('%s_%s', parent, to_char(wk AT TIME ZONE 'UTC', 'IYYY"w"IW'));
      IF to_regclass(format('public.%I', part)) IS NULL THEN
        EXECUTE format(
          'CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES FROM (%L) TO (%L)',
          part, parent, wk, wk + interval '7 days');
        created := created + 1;
      END IF;
    END LOOP;
  END LOOP;
  RETURN created;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION postier_ensure_partitions(int) FROM PUBLIC;

SELECT postier_ensure_partitions(4);

-- +goose Down
DROP FUNCTION postier_ensure_partitions(int);
DROP TABLE kumo_dead_records;
DROP TABLE idempotency_keys;
DROP TABLE events;
DROP TABLE messages;
DROP TABLE message_contents;
DROP TABLE sending_domains;
DROP TABLE api_credentials;
DROP TABLE projects;
DROP TABLE tenants;
