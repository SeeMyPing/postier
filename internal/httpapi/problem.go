// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/httpapi/gen"
)

// problemStatus is the single place where a domain error code becomes an
// HTTP status.
var problemStatus = map[domain.ErrorCode]int{
	domain.CodeInvalidRequest:      http.StatusUnprocessableEntity,
	domain.CodeUnauthorized:        http.StatusUnauthorized,
	domain.CodeForbidden:           http.StatusForbidden,
	domain.CodeNotFound:            http.StatusNotFound,
	domain.CodeDomainNotAllowed:    http.StatusForbidden,
	domain.CodeDomainNotVerified:   http.StatusForbidden,
	domain.CodeIdempotencyConflict: http.StatusConflict,
	domain.CodePayloadTooLarge:     http.StatusRequestEntityTooLarge,
	domain.CodeInternal:            http.StatusInternalServerError,
}

var problemTitle = map[domain.ErrorCode]string{
	domain.CodeInvalidRequest:      "Invalid request",
	domain.CodeUnauthorized:        "Authentication required",
	domain.CodeForbidden:           "Not allowed",
	domain.CodeNotFound:            "Not found",
	domain.CodeDomainNotAllowed:    "Sending domain not allowed",
	domain.CodeDomainNotVerified:   "Sending domain not verified",
	domain.CodeIdempotencyConflict: "Idempotency key reused",
	domain.CodePayloadTooLarge:     "Payload too large",
	domain.CodeInternal:            "Internal error",
}

// writeError renders any error as an RFC 9457 problem. A domain.Error is
// shown as it is; anything else is logged and hidden behind a 500, so that
// internals (SQL, hostnames) never leak to a client.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var de *domain.Error
	if !errors.As(err, &de) {
		s.log.ErrorContext(r.Context(), "request failed", "err", err, "trace_id", traceID(r.Context()))
		de = domain.Errorf(domain.CodeInternal, "an internal error occurred")
	}
	s.writeProblem(w, r, problemStatus[de.Code], de)
}

func (s *Server) writeProblem(w http.ResponseWriter, r *http.Request, status int, de *domain.Error) {
	if status == 0 {
		status = http.StatusInternalServerError
	}
	p := gen.Problem{
		// A URN: stable and unique without pretending to be a web page.
		Type:   "urn:postier:problem:" + string(de.Code),
		Title:  problemTitle[de.Code],
		Status: status,
		Code:   gen.ProblemCode(de.Code),
	}
	if de.Detail != "" {
		p.Detail = &de.Detail
	}
	if de.Field != "" {
		p.Field = &de.Field
	}
	if id := traceID(r.Context()); id != "" {
		p.TraceId = &id
	}
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="postier"`)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(p); err != nil {
		s.log.Log(r.Context(), slog.LevelDebug, "writing problem", "err", err)
	}
}
