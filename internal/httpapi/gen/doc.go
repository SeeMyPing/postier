// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package gen holds the types and the router generated from
// openapi/postier.yaml. Do not edit api.gen.go: change the spec and run
// `make generate`.
package gen

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.8.2 -config config.yaml ../../../openapi/postier.yaml
