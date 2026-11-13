// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package domain

import "fmt"

// ErrorCode is a stable, machine-readable error identifier. Codes are part of
// the public API: clients branch on them, so they are never renamed.
type ErrorCode string

// The error codes of the public API.
const (
	CodeInvalidRequest      ErrorCode = "invalid_request"
	CodeUnauthorized        ErrorCode = "unauthorized"
	CodeForbidden           ErrorCode = "forbidden"
	CodeNotFound            ErrorCode = "not_found"
	CodeDomainNotAllowed    ErrorCode = "domain_not_allowed"
	CodeDomainNotVerified   ErrorCode = "domain_not_verified"
	CodeIdempotencyConflict ErrorCode = "idempotency_conflict"
	CodePayloadTooLarge     ErrorCode = "payload_too_large"
	CodeInternal            ErrorCode = "internal_error"
)

// Error is an error the caller can act on. Everything else is an internal
// error, whose detail is logged and never shown.
//
// Errors are typed here, close to the rules that raise them, and translated
// into a transport's vocabulary (RFC 9457 for HTTP, a reply code for SMTP)
// in one place per transport.
type Error struct {
	Code   ErrorCode
	Detail string
	Field  string // offending request field, when there is one
}

func (e *Error) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s: %s: %s", e.Code, e.Field, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

// Errorf builds an Error.
func Errorf(code ErrorCode, format string, args ...any) *Error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// FieldErrorf builds an invalid_request Error about one field.
func FieldErrorf(field, format string, args ...any) *Error {
	return &Error{Code: CodeInvalidRequest, Field: field, Detail: fmt.Sprintf(format, args...)}
}
