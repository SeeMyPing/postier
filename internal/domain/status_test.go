// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package domain

import (
	"slices"
	"testing"

	"pgregory.net/rapid"
)

// rank orders statuses along the life of a message. Every allowed transition
// must strictly increase it: that is what "no regression" means, and it also
// proves the matrix has no cycle.
func rank(s Status) int {
	switch s {
	case StatusAccepted:
		return 0
	case StatusScheduled:
		return 1
	case StatusQueued:
		return 2
	case StatusDeferred:
		return 3
	case StatusDelivered:
		return 4
	case StatusComplained:
		return 6
	default:
		return 5
	}
}

func TestMatrixCoversEveryStatus(t *testing.T) {
	for _, s := range AllStatuses() {
		if _, ok := transitions[s]; !ok {
			t.Errorf("status %q has no row in the matrix", s)
		}
	}
	if len(transitions) != len(AllStatuses()) {
		t.Errorf("matrix has %d rows, AllStatuses has %d", len(transitions), len(AllStatuses()))
	}
}

func TestEveryTransitionMovesForward(t *testing.T) {
	for from, tos := range transitions {
		for _, to := range tos {
			if !to.Valid() {
				t.Errorf("%s -> %s: unknown target", from, to)
			}
			if rank(to) <= rank(from) {
				t.Errorf("%s -> %s is a regression", from, to)
			}
		}
	}
}

// TestMatrixExhaustive pins the full matrix: any change to it has to be made
// here too, on purpose.
func TestMatrixExhaustive(t *testing.T) {
	want := map[Status]string{
		StatusAccepted:   "scheduled queued suppressed rejected canceled deferred delivered bounced expired complained",
		StatusScheduled:  "queued canceled rejected deferred delivered bounced expired complained",
		StatusQueued:     "deferred delivered bounced expired canceled rejected complained",
		StatusDeferred:   "delivered bounced expired canceled complained",
		StatusDelivered:  "bounced complained",
		StatusBounced:    "complained",
		StatusComplained: "",
		StatusExpired:    "",
		StatusSuppressed: "",
		StatusRejected:   "",
		StatusCanceled:   "",
	}
	for _, from := range AllStatuses() {
		for _, to := range AllStatuses() {
			allowed := false
			for _, w := range splitWords(want[from]) {
				if Status(w) == to {
					allowed = true
				}
			}
			if got := CanTransition(from, to); got != allowed {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, allowed)
			}
			if got := slices.Contains(AllowedFrom(to), from); got != allowed {
				t.Errorf("AllowedFrom(%s) contains %s = %v, want %v", to, from, got, allowed)
			}
		}
	}
}

// TestWhatATerminalStatusCanBecome: once delivery attempts are over, only a
// report received later can change the status.
func TestWhatATerminalStatusCanBecome(t *testing.T) {
	want := map[Status][]Status{
		StatusDelivered: {StatusBounced, StatusComplained},
		StatusBounced:   {StatusComplained},
	}
	for _, s := range AllStatuses() {
		if !s.Terminal() {
			continue
		}
		if !slices.Equal(transitions[s], want[s]) {
			t.Errorf("terminal status %s can become %v, want %v", s, transitions[s], want[s])
		}
	}
}

func TestAllowedFromIsACopy(t *testing.T) {
	a := AllowedFrom(StatusDelivered)
	a[0] = "tampered"
	if AllowedFrom(StatusDelivered)[0] == "tampered" {
		t.Fatal("AllowedFrom leaks its internal slice")
	}
}

func TestEventTargetStatus(t *testing.T) {
	for _, e := range []EventType{
		EventAccepted, EventQueued, EventDeferred, EventDelivered, EventBounced,
		EventComplained, EventExpired, EventSuppressed, EventRejected, EventCanceled,
	} {
		s, ok := e.TargetStatus()
		if !ok || string(s) != string(e) {
			t.Errorf("TargetStatus(%s) = %q, %v", e, s, ok)
		}
	}
	if _, ok := EventType("opened").TargetStatus(); ok {
		t.Error("an open must not change the status")
	}
}

// apply mimics ingest: the event is always recorded, the status only changes
// when the matrix allows it.
func apply(cur Status, e EventType) Status {
	to, ok := e.TargetStatus()
	if !ok || !slices.Contains(AllowedFrom(to), cur) {
		return cur
	}
	return to
}

// TestNoSequenceRegresses is the property the whole event pipeline relies on:
// whatever the order in which log records arrive, a status never goes back,
// and a closed message stays closed.
func TestNoSequenceRegresses(t *testing.T) {
	events := []EventType{
		EventAccepted, EventQueued, EventDeferred, EventDelivered, EventBounced,
		EventComplained, EventExpired, EventSuppressed, EventRejected, EventCanceled,
	}
	rapid.Check(t, func(t *rapid.T) {
		seq := rapid.SliceOfN(rapid.SampledFrom(events), 0, 40).Draw(t, "events")
		cur := StatusAccepted
		for _, e := range seq {
			next := apply(cur, e)
			if rank(next) < rank(cur) {
				t.Fatalf("%s regressed to %s on %s", cur, next, e)
			}
			if cur.Terminal() && next != cur && next != StatusBounced && next != StatusComplained {
				t.Fatalf("closed status %s changed to %s on %s", cur, next, e)
			}
			cur = next
		}
	})
}

// TestOrderIndependentOutcome checks the practical consequence: a delivery
// whose records arrive shuffled still ends up delivered.
func TestOrderIndependentOutcome(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		seq := rapid.Permutation([]EventType{
			EventQueued, EventDeferred, EventDeferred, EventDelivered,
		}).Draw(t, "order")
		cur := StatusAccepted
		for _, e := range seq {
			cur = apply(cur, e)
		}
		if cur != StatusDelivered {
			t.Fatalf("order %v ended in %s, want delivered", seq, cur)
		}
	})
}

// TestConsistentHistoriesAreOrderIndependent: for every history a message
// can really have, the final status is the same whatever the arrival order.
func TestConsistentHistoriesAreOrderIndependent(t *testing.T) {
	histories := map[Status][]EventType{
		StatusDelivered:  {EventQueued, EventDelivered},
		StatusBounced:    {EventQueued, EventDeferred, EventBounced},
		StatusExpired:    {EventQueued, EventDeferred, EventDeferred, EventExpired},
		StatusCanceled:   {EventQueued, EventDeferred, EventCanceled},
		StatusComplained: {EventQueued, EventDelivered, EventComplained},
	}
	// An asynchronous bounce after a delivery, with and without a complaint.
	oob := []EventType{EventQueued, EventDelivered, EventBounced}
	both := []EventType{EventQueued, EventDelivered, EventBounced, EventComplained}

	check := func(t *rapid.T, events []EventType, want Status) {
		cur := StatusAccepted
		for _, e := range rapid.Permutation(events).Draw(t, "order") {
			cur = apply(cur, e)
		}
		if cur != want {
			t.Fatalf("history %v ended in %s, want %s", events, cur, want)
		}
	}
	rapid.Check(t, func(t *rapid.T) {
		for want, events := range histories {
			check(t, events, want)
		}
		check(t, oob, StatusBounced)
		check(t, both, StatusComplained)
	})
}

func splitWords(s string) []string {
	var out []string
	word := ""
	for _, r := range s {
		if r == ' ' {
			if word != "" {
				out = append(out, word)
			}
			word = ""
			continue
		}
		word += string(r)
	}
	if word != "" {
		out = append(out, word)
	}
	return out
}
