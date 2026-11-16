// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package domain

import "time"

// SuppressionReason says why an address is suppressed.
type SuppressionReason string

// The reasons an address is suppressed.
const (
	ReasonHardBounce  SuppressionReason = "hard_bounce"
	ReasonComplaint   SuppressionReason = "complaint"
	ReasonSoftBounces SuppressionReason = "soft_bounces"
	ReasonManual      SuppressionReason = "manual"
)

// Soft bounce policy: this many failed messages within the window suppress
// the address for the duration of the window.
const (
	SoftBounceThreshold = 3
	SoftBounceWindow    = 7 * 24 * time.Hour
)

// Bounce classes, as assigned by KumoMTA's classifier, that say something
// about the recipient. Every other class is about the sender, the content or
// the route.
//
// The distinction matters: on the first real send, Microsoft answered
// "550 5.7.1 … blocked using Spamhaus" for a perfectly valid mailbox. That is
// a 5xx, a permanent failure, and suppressing on it would have blacklisted a
// good address because of a bad sending IP.
var (
	hardBounceClasses = []string{"InvalidRecipient", "InactiveMailbox", "BadDomain"}
	softBounceClasses = []string{"QuotaIssues"}
)

// SoftBounceClasses lists the classes counted towards SoftBounceThreshold.
func SoftBounceClasses() []string { return append([]string(nil), softBounceClasses...) }

// SuppressionTrigger is what an event implies for its recipient's address.
type SuppressionTrigger int

// The possible consequences of an event.
const (
	// NoSuppression: the event says nothing against the address.
	NoSuppression SuppressionTrigger = iota
	// SuppressForProject: the address is dead. Permanent, for the project.
	SuppressForProject
	// SuppressForTenant: the recipient complained. Permanent, and for every
	// project of the tenant: they must not hear from it again.
	SuppressForTenant
	// CountSoftBounce: a temporary problem of the mailbox. The address is
	// suppressed for a while if it keeps happening.
	CountSoftBounce
)

// SuppressionFor decides what an event means for the address of its
// recipient. bounceClass is KumoMTA's classification, feedbackType the type
// of an ARF report ("abuse", "not-spam"…).
func SuppressionFor(event EventType, bounceClass, feedbackType string) SuppressionTrigger {
	switch event {
	case EventComplained:
		// The one report that is good news.
		if feedbackType == "not-spam" {
			return NoSuppression
		}
		return SuppressForTenant
	case EventBounced:
		// Only a final failure: the classifier also tags temporary replies
		// (451 4.1.1), which arrive as deferred events and may yet succeed.
		for _, c := range hardBounceClasses {
			if c == bounceClass {
				return SuppressForProject
			}
		}
		for _, c := range softBounceClasses {
			if c == bounceClass {
				return CountSoftBounce
			}
		}
	case EventExpired:
		// The class of an expiry is always MessageExpired. Whether the
		// mailbox was the cause is in the deferrals before it, which the
		// count looks at.
		return CountSoftBounce
	}
	return NoSuppression
}
