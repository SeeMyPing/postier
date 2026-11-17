-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- What the operator needs to mirror custom resources into these tables.

-- +goose Up

-- A credential is tied to the resource that declares it, by its Kubernetes
-- UID. NULL for the keys issued by `postier seed`.
ALTER TABLE api_credentials ADD COLUMN external_id text UNIQUE;

-- A sending domain whose resource is deleted keeps its row: messages still
-- point at it, and their history must stay readable. It stops counting for
-- uniqueness, so that the domain can be declared again, by anyone.
ALTER TABLE sending_domains ADD COLUMN deleted_at timestamptz;
ALTER TABLE sending_domains DROP CONSTRAINT sending_domains_domain_key;
CREATE UNIQUE INDEX sending_domains_domain_key ON sending_domains (domain) WHERE deleted_at IS NULL;

-- +goose Down
DROP INDEX sending_domains_domain_key;
ALTER TABLE sending_domains ADD CONSTRAINT sending_domains_domain_key UNIQUE (domain);
ALTER TABLE sending_domains DROP COLUMN deleted_at;
ALTER TABLE api_credentials DROP COLUMN external_id;
