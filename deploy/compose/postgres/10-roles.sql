-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- Local test bench only: these passwords are public.
-- Postier connects with two roles (docs/implementation-plan.md §2.5).
CREATE ROLE postier_app LOGIN PASSWORD 'app-dev-password';
CREATE ROLE postier_system LOGIN PASSWORD 'system-dev-password' BYPASSRLS;
