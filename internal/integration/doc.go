// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package integration holds the tests that need a real Postgres: the rules
// they check live in SQL (unique indexes, row locks, grants, partitions) and
// cannot be faked.
//
// They run when POSTIER_TEST_DATABASE_URL points at a server where the user
// may create databases and roles; they are skipped otherwise. `make
// test-integration` starts a throwaway Postgres and sets the variable.
package integration
