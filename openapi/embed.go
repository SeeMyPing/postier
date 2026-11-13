// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package openapi embeds the API contract, so that the server can publish
// exactly the spec it was generated from.
package openapi

import _ "embed"

// Spec is the OpenAPI document of the public API.
//
//go:embed postier.yaml
var Spec []byte
