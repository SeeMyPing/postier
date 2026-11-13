// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package domain holds Postier's pure types: no I/O, no HTTP, no SQL.
package domain

import "slices"

// Status is the current state of a message. It is a projection of the
// message's events: events are append-only, the status only moves forward.
type Status string

// The statuses of a message. See transitions for how they follow each other.
const (
	StatusAccepted   Status = "accepted"
	StatusScheduled  Status = "scheduled"
	StatusQueued     Status = "queued"
	StatusDeferred   Status = "deferred"
	StatusDelivered  Status = "delivered"
	StatusBounced    Status = "bounced"
	StatusComplained Status = "complained"
	StatusExpired    Status = "expired"
	StatusSuppressed Status = "suppressed"
	StatusRejected   Status = "rejected"
	StatusCanceled   Status = "canceled"
)

// transitions is the explicit matrix: for each status, the statuses it may
// move to. Anything absent is forbidden.
//
// It is wider than the diagram in docs/architecture.md, for one reason:
// KumoMTA does not guarantee the order of its log records. The matrix is
// built so that the final status of a message does not depend on the order
// in which its records arrive:
//
//   - a Delivery can reach ingest before the Reception of the same message,
//     so a message still in accepted (or scheduled) can jump straight to any
//     later state instead of staying stuck on a stale one;
//   - a complaint proves the message was delivered, so it is accepted from
//     any earlier state, without waiting for the Delivery record;
//   - a complaint is the last word: it also replaces a late bounce, whichever
//     of the two reports is processed first.
var transitions = map[Status][]Status{
	StatusAccepted: {
		StatusScheduled, StatusQueued, StatusSuppressed, StatusRejected, StatusCanceled,
		StatusDeferred, StatusDelivered, StatusBounced, StatusExpired, StatusComplained,
	},
	StatusScheduled: {
		StatusQueued, StatusCanceled, StatusRejected,
		StatusDeferred, StatusDelivered, StatusBounced, StatusExpired, StatusComplained,
	},
	StatusQueued: {
		StatusDeferred, StatusDelivered, StatusBounced, StatusExpired,
		StatusCanceled, StatusRejected, StatusComplained,
	},
	StatusDeferred: {
		StatusDelivered, StatusBounced, StatusExpired, StatusCanceled, StatusComplained,
	},
	// delivered is terminal, except for an asynchronous bounce or a complaint.
	StatusDelivered:  {StatusBounced, StatusComplained},
	StatusBounced:    {StatusComplained},
	StatusComplained: {},
	StatusExpired:    {},
	StatusSuppressed: {},
	StatusRejected:   {},
	StatusCanceled:   {},
}

// allowedFrom is the transposed matrix, computed once.
var allowedFrom = func() map[Status][]Status {
	out := make(map[Status][]Status, len(transitions))
	for _, s := range AllStatuses() {
		out[s] = []Status{}
	}
	for _, from := range AllStatuses() {
		for _, to := range transitions[from] {
			out[to] = append(out[to], from)
		}
	}
	return out
}()

// AllStatuses lists every status, in a stable order.
func AllStatuses() []Status {
	return []Status{
		StatusAccepted, StatusScheduled, StatusQueued, StatusDeferred, StatusDelivered,
		StatusBounced, StatusComplained, StatusExpired, StatusSuppressed, StatusRejected,
		StatusCanceled,
	}
}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	_, ok := transitions[s]
	return ok
}

// Terminal reports whether no further delivery attempt will happen. A
// terminal status can still be replaced by a report that arrives later: a
// bounce or a complaint after delivered, a complaint after bounced.
func (s Status) Terminal() bool {
	switch s {
	case StatusDelivered, StatusBounced, StatusComplained, StatusExpired,
		StatusSuppressed, StatusRejected, StatusCanceled:
		return true
	}
	return false
}

// CanTransition reports whether a message may move from one status to another.
func CanTransition(from, to Status) bool {
	return slices.Contains(transitions[from], to)
}

// AllowedFrom returns the statuses from which a message may move to the given
// one. The result is used verbatim in
//
//	UPDATE messages SET status = $to WHERE … AND status = ANY($allowed)
//
// which makes the precedence rule atomic without an application lock.
func AllowedFrom(to Status) []Status {
	return slices.Clone(allowedFrom[to])
}

// AllowedFromStrings is AllowedFrom for a text[] query parameter.
func AllowedFromStrings(to Status) []string {
	from := allowedFrom[to]
	out := make([]string, len(from))
	for i, s := range from {
		out[i] = string(s)
	}
	return out
}

// EventType is the kind of an event in a message's history.
type EventType string

// The event types that move a status. Each is named after the status it
// leads to.
const (
	EventAccepted   EventType = "accepted"
	EventQueued     EventType = "queued"
	EventDeferred   EventType = "deferred"
	EventDelivered  EventType = "delivered"
	EventBounced    EventType = "bounced"
	EventComplained EventType = "complained"
	EventExpired    EventType = "expired"
	EventSuppressed EventType = "suppressed"
	EventRejected   EventType = "rejected"
	EventCanceled   EventType = "canceled"
)

// TargetStatus returns the status an event tries to move its message to.
// Every event type of phase 0 maps to a status; opens and clicks, which do
// not, come later.
func (e EventType) TargetStatus() (Status, bool) {
	s := Status(e)
	if !s.Valid() || s == StatusScheduled {
		return "", false
	}
	return s, true
}
