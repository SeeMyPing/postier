// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package httpapi implements the REST API described by openapi/postier.yaml.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/SeeMyPing/postier/internal/accept"
	"github.com/SeeMyPing/postier/internal/domain"
	"github.com/SeeMyPing/postier/internal/httpapi/gen"
	"github.com/SeeMyPing/postier/internal/store"
	"github.com/SeeMyPing/postier/internal/store/sqlcgen"
	"github.com/SeeMyPing/postier/openapi"
)

// maxRequestBytes bounds a request body: the body limit of a message, plus
// room for JSON escaping and the other fields.
const maxRequestBytes = 16 << 20

// Sender accepts messages. Implemented by accept.Service.
type Sender interface {
	Send(ctx context.Context, p accept.Principal, req accept.Request, opts accept.Options) (accept.Result, bool, error)
}

// Authenticator validates an API key. Implemented by accept.Authenticator.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (accept.Principal, error)
}

// Server implements gen.ServerInterface.
type Server struct {
	auth   Authenticator
	sender Sender
	pool   *store.AppPool
	log    *slog.Logger
}

var _ gen.ServerInterface = (*Server)(nil)

// New returns the API server.
func New(auth Authenticator, sender Sender, pool *store.AppPool, log *slog.Logger) *Server {
	return &Server{auth: auth, sender: sender, pool: pool, log: log}
}

// Handler returns the HTTP handler: generated routes, plus the spec itself.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openapi.Spec)
	})
	// Anything the generated router does not know is a 404 in the same
	// format as every other error.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.writeError(w, r, domain.Errorf(domain.CodeNotFound, "no such endpoint"))
	})
	h := gen.HandlerWithOptions(s, gen.StdHTTPServerOptions{
		BaseRouter: mux,
		ErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			// Raised by the generated code for a malformed path or header
			// parameter, before any handler runs.
			s.writeProblem(w, r, http.StatusBadRequest, domain.Errorf(domain.CodeInvalidRequest, "%v", err))
		},
	})
	return s.withTrace(s.withAccessLog(h))
}

// --- middleware --------------------------------------------------------------

type ctxKey int

const traceKey ctxKey = iota

func traceID(ctx context.Context) string {
	id, _ := ctx.Value(traceKey).(string)
	return id
}

// W3C Trace Context: version-traceid-parentid-flags.
var reTraceparent = regexp.MustCompile(`^[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$`)

// withTrace gives every request a trace id: the caller's, when it sends a
// valid traceparent header, a new one otherwise. The id follows the message
// through KumoMTA's metadata and comes back in its log records, which ties a
// delivery that happens hours later to the request that caused it. OTel
// spans are built on top of it in v0.5.
func (s *Server) withTrace(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id string
		if m := reTraceparent.FindStringSubmatch(r.Header.Get("traceparent")); m != nil && m[1] != strings.Repeat("0", 32) {
			id = m[1]
		} else {
			var b [16]byte
			_, _ = rand.Read(b[:])
			id = hex.EncodeToString(b[:])
		}
		w.Header().Set("X-Trace-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), traceKey, id)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) withAccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		// r.Pattern is the route ("GET /v1/emails/{id}"), not the path: no
		// identifier and no address ends up in the logs.
		s.log.InfoContext(r.Context(), "http request",
			"method", r.Method, "route", r.Pattern, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(), "trace_id", traceID(r.Context()))
	})
}

// --- helpers -----------------------------------------------------------------

func (s *Server) authenticate(r *http.Request) (accept.Principal, error) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return accept.Principal{}, domain.Errorf(domain.CodeUnauthorized, "missing or invalid API key")
	}
	return s.auth.Authenticate(r.Context(), strings.TrimSpace(token))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// --- handlers ----------------------------------------------------------------

// SendEmail implements POST /v1/emails.
func (s *Server) SendEmail(w http.ResponseWriter, r *http.Request, params gen.SendEmailParams) {
	p, err := s.authenticate(r)
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	var body gen.SendEmailRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	dec.DisallowUnknownFields() // the schema says additionalProperties: false
	if err := dec.Decode(&body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			s.writeError(w, r, domain.Errorf(domain.CodePayloadTooLarge, "request body exceeds %d bytes", maxRequestBytes))
			return
		}
		s.writeProblem(w, r, http.StatusBadRequest, domain.Errorf(domain.CodeInvalidRequest, "malformed JSON body: %v", err))
		return
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		s.writeProblem(w, r, http.StatusBadRequest, domain.Errorf(domain.CodeInvalidRequest, "unexpected content after the JSON body"))
		return
	}

	result, replayed, err := s.sender.Send(r.Context(), p, accept.Request{
		From:     body.From,
		To:       body.To,
		ReplyTo:  deref(body.ReplyTo),
		Subject:  body.Subject,
		Text:     deref(body.Text),
		HTML:     deref(body.Html),
		Headers:  deref(body.Headers),
		Tags:     deref(body.Tags),
		Metadata: deref(body.Metadata),
	}, accept.Options{
		Source:         accept.SourceAPI,
		IdempotencyKey: deref(params.IdempotencyKey),
		TraceID:        traceID(r.Context()),
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	out := gen.SendEmailResponse{BatchId: result.BatchID, Messages: make([]gen.AcceptedMessage, len(result.Messages))}
	for i, m := range result.Messages {
		out.Messages[i] = gen.AcceptedMessage{Id: m.ID, To: m.To, Status: gen.Status(m.Status)}
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusAccepted, out)
}

// findMessage loads a message of the caller's project. An id that is not a
// UUIDv7 cannot be one of ours: it is a 404, not a 400, like any unknown id.
func (s *Server) findMessage(ctx context.Context, tx store.TenantTx, p accept.Principal, id uuid.UUID) (sqlcgen.GetMessageRow, error) {
	notFound := domain.Errorf(domain.CodeNotFound, "no such message")
	createdAt, err := domain.IDTime(id)
	if err != nil {
		return sqlcgen.GetMessageRow{}, notFound
	}
	row, err := tx.Q().GetMessage(ctx, sqlcgen.GetMessageParams{ID: id, CreatedAt: createdAt, ProjectID: p.ProjectID})
	if errors.Is(store.NotFound(err), store.ErrNotFound) {
		return sqlcgen.GetMessageRow{}, notFound
	}
	return row, err
}

// GetEmail implements GET /v1/emails/{id}.
func (s *Server) GetEmail(w http.ResponseWriter, r *http.Request, id gen.MessageId) {
	p, err := s.authenticate(r)
	if err == nil && !p.Can(accept.ScopeRead) {
		err = domain.Errorf(domain.CodeForbidden, "this API key cannot read messages")
	}
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	var out gen.Email
	err = s.pool.InTenantTx(r.Context(), p.TenantID, func(tx store.TenantTx) error {
		row, err := s.findMessage(r.Context(), tx, p, id)
		if err != nil {
			return err
		}
		metadata := map[string]string{}
		if err := json.Unmarshal(row.Metadata, &metadata); err != nil {
			return err
		}
		out = gen.Email{
			Id: row.ID, BatchId: row.BatchID, From: row.FromAddr, To: row.RcptAddr,
			Subject: row.Subject, Status: gen.Status(row.Status), StatusAt: row.StatusAt,
			Attempts: int(row.Attempts), Tags: row.Tags, Metadata: metadata, CreatedAt: row.CreatedAt,
		}
		return nil
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// ListEmailEvents implements GET /v1/emails/{id}/events.
func (s *Server) ListEmailEvents(w http.ResponseWriter, r *http.Request, id gen.MessageId) {
	p, err := s.authenticate(r)
	if err == nil && !p.Can(accept.ScopeRead) {
		err = domain.Errorf(domain.CodeForbidden, "this API key cannot read messages")
	}
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	out := gen.EventList{Data: []gen.Event{}}
	err = s.pool.InTenantTx(r.Context(), p.TenantID, func(tx store.TenantTx) error {
		msg, err := s.findMessage(r.Context(), tx, p, id)
		if err != nil {
			return err
		}
		rows, err := tx.Q().ListMessageEvents(r.Context(), sqlcgen.ListMessageEventsParams{
			MessageID: id, TenantID: p.TenantID,
			// Events cannot predate their message. The bound lets Postgres
			// skip older partitions; the margin absorbs clock skew between
			// Postier and the MTA.
			Since: msg.CreatedAt.Add(-time.Hour),
		})
		if err != nil {
			return err
		}
		for _, row := range rows {
			ev := gen.Event{
				Id: row.ID, Type: gen.EventType(row.Type), OccurredAt: row.OccurredAt,
				EnhancedCode: row.EnhancedCode, BounceClass: row.BounceClass, RemoteMx: row.RemoteMx,
			}
			if row.SmtpCode != nil {
				code := int(*row.SmtpCode)
				ev.SmtpCode = &code
			}
			var detail struct {
				Response string `json:"response"`
			}
			if json.Unmarshal(row.Detail, &detail) == nil && detail.Response != "" {
				ev.Response = &detail.Response
			}
			out.Data = append(out.Data, ev)
		}
		return nil
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
