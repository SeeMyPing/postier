// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package domain

import "testing"

func TestSuppressionFor(t *testing.T) {
	cases := []struct {
		name     string
		event    EventType
		class    string
		feedback string
		want     SuppressionTrigger
	}{
		{"unknown mailbox", EventBounced, "InvalidRecipient", "", SuppressForProject},
		{"disabled mailbox", EventBounced, "InactiveMailbox", "", SuppressForProject},
		{"null MX", EventBounced, "BadDomain", "", SuppressForProject},
		{"mailbox full", EventBounced, "QuotaIssues", "", CountSoftBounce},
		{"expired", EventExpired, "MessageExpired", "", CountSoftBounce},
		{"complaint", EventComplained, "", "abuse", SuppressForTenant},
		{"complaint of unknown type", EventComplained, "", "", SuppressForTenant},
		{"not spam", EventComplained, "", "not-spam", NoSuppression},

		// Failures that are not the recipient's doing. The first is the real
		// reply Microsoft gave for a valid mailbox when the sending IP was
		// listed by Spamhaus: "550 5.7.1", classified AuthenticationFailed.
		{"sending IP blocked", EventBounced, "AuthenticationFailed", "", NoSuppression},
		{"policy", EventBounced, "PolicyRelated", "", NoSuppression},
		{"bad sender", EventBounced, "InvalidSender", "", NoSuppression},
		{"routing", EventBounced, "RoutingErrors", "", NoSuppression},
		{"unclassified", EventBounced, "", "", NoSuppression},
		{"uncategorized", EventBounced, "Uncategorized", "", NoSuppression},

		// A temporary reply about the mailbox may still be followed by a
		// delivery: only the final outcome counts.
		{"deferred, unknown mailbox", EventDeferred, "InvalidRecipient", "", NoSuppression},
		{"deferred, mailbox full", EventDeferred, "QuotaIssues", "", NoSuppression},
		{"delivered", EventDelivered, "", "", NoSuppression},
		{"cancelled", EventCanceled, "", "", NoSuppression},
		{"rejected at injection", EventRejected, "", "", NoSuppression},
	}
	for _, tc := range cases {
		if got := SuppressionFor(tc.event, tc.class, tc.feedback); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestSoftBounceClassesIsACopy(t *testing.T) {
	SoftBounceClasses()[0] = "tampered"
	if SoftBounceClasses()[0] == "tampered" {
		t.Fatal("SoftBounceClasses leaks its internal slice")
	}
}
