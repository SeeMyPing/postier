-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- Applied by `postier migrate` after the goose and River migrations.
-- Idempotent: GRANT is a no-op when the privilege is already held.

GRANT USAGE ON SCHEMA public TO postier_app, postier_system;

-- postier_system: the workers, across tenants.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postier_system;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postier_system;
GRANT EXECUTE ON FUNCTION postier_ensure_partitions(int) TO postier_system;

-- postier_app: the entry points. Reads configuration, accepts messages,
-- enqueues the injection job. It can neither rewrite history nor touch
-- another role's tables.
GRANT SELECT ON tenants, projects, api_credentials, sending_domains TO postier_app;
GRANT SELECT, INSERT ON messages, events, message_contents TO postier_app;
GRANT SELECT, INSERT, UPDATE ON idempotency_keys TO postier_app;
-- River's insert is an upsert on the job's unique key, hence UPDATE.
GRANT SELECT, INSERT, UPDATE ON river_job TO postier_app;
GRANT USAGE, SELECT ON SEQUENCE river_job_id_seq TO postier_app;
