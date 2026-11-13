// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package db embeds the SQL migrations into the binary, so that
// `postier migrate` needs nothing but a database URL.
package db

import "embed"

// Migrations holds the goose migrations, under the "migrations" directory.
//
//go:embed migrations/*.sql
var Migrations embed.FS

// Grants is applied after every migration run. It is idempotent, and separate
// from the versioned migrations because it also covers the tables River
// creates with its own migrator.
//
//go:embed grants.sql
var Grants string
