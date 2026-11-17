// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package dnscheck

import (
	"context"
	"fmt"
	"net/netip"
	"strings"

	"golang.org/x/net/publicsuffix"
)

// Result is the outcome of a check that could be carried out. Reason is a
// CamelCase word for a condition; Message is for a person.
type Result struct {
	OK      bool
	Reason  string
	Message string
}

func pass(reason, format string, args ...any) Result {
	return Result{OK: true, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

func fail(reason, format string, args ...any) Result {
	return Result{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// ChallengePrefix starts the value of the ownership record.
const ChallengePrefix = "pst-verify="

// ChallengeName is where the ownership record of a domain is published.
func ChallengeName(domain string) string { return "_postier-challenge." + domain }

// DKIMName is where the DKIM key of a domain is published.
func DKIMName(selector, domain string) string { return selector + "._domainkey." + domain }

// DKIMValue is the DKIM record for an RSA public key (base64 of its DER).
func DKIMValue(publicKey string) string { return "v=DKIM1; k=rsa; p=" + publicKey }

// CheckChallenge verifies that whoever declared the domain controls its DNS.
func CheckChallenge(ctx context.Context, r Resolver, domain, token string) (Result, error) {
	name := ChallengeName(domain)
	records, err := r.TXT(ctx, name)
	if err != nil {
		return Result{}, err
	}
	want := ChallengePrefix + token
	for _, rec := range records {
		if strings.TrimSpace(rec) == want {
			return pass("ChallengeFound", "%s is published", name), nil
		}
	}
	if len(records) == 0 {
		return fail("ChallengeMissing", "no TXT record at %s", name), nil
	}
	return fail("ChallengeMismatch", "the TXT record at %s does not hold the expected value", name), nil
}

// tags parses a "k=v; k=v" record (DKIM, DMARC).
func tags(record string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(record, ";") {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		out[strings.ToLower(strings.TrimSpace(k))] = strings.Join(strings.Fields(v), "")
	}
	return out
}

// CheckDKIM verifies that the published key is the one KumoMTA signs with.
// A different key would make every signature fail, which is worse than no
// signature at all.
func CheckDKIM(ctx context.Context, r Resolver, selector, domain, publicKey string) (Result, error) {
	name := DKIMName(selector, domain)
	records, err := r.TXT(ctx, name)
	if err != nil {
		return Result{}, err
	}
	if len(records) == 0 {
		return fail("KeyMissing", "no TXT record at %s", name), nil
	}
	if len(records) > 1 {
		return fail("MultipleRecords", "%d TXT records at %s: receivers treat that as an error", len(records), name), nil
	}
	t := tags(records[0])
	if v, ok := t["v"]; ok && v != "DKIM1" {
		return fail("NotDKIM", "the TXT record at %s is not a DKIM key", name), nil
	}
	switch t["p"] {
	case publicKey:
		return pass("KeyPublished", "%s holds the signing key", name), nil
	case "":
		return fail("KeyRevoked", "the DKIM record at %s has an empty key (revoked)", name), nil
	default:
		return fail("KeyMismatch", "the key at %s is not the one this domain signs with", name), nil
	}
}

// CheckReturnPath verifies the envelope sender domain: its MX must lead
// bounces back to the platform, and its SPF record must authorise every
// address mail is sent from. sources may be empty when the sending addresses
// are not pinned; SPF coverage then cannot be checked, only its presence.
func CheckReturnPath(ctx context.Context, r Resolver, returnPath, mxHost string, sources []netip.Addr) (Result, error) {
	mxs, err := r.MX(ctx, returnPath)
	if err != nil {
		return Result{}, err
	}
	want := normalizeHost(mxHost)
	found := false
	for _, mx := range mxs {
		found = found || mx == want
	}
	if !found {
		if len(mxs) == 0 {
			return fail("MXMissing", "%s has no MX record: bounces would be lost", returnPath), nil
		}
		return fail("MXMismatch", "the MX of %s is %s, not %s", returnPath, strings.Join(mxs, ", "), want), nil
	}

	spf, res, err := spfRecord(ctx, r, returnPath)
	if err != nil {
		return Result{}, err
	}
	if !res.OK {
		return res, nil
	}
	for _, addr := range sources {
		ev := spfEval{r: r}
		ok, err := ev.authorises(ctx, spf, returnPath, addr)
		if err != nil {
			return Result{}, err
		}
		if !ok {
			// The trap of a dual-stack host: mail leaves over an address
			// family the record forgot.
			return fail("SPFDoesNotCoverSource",
				"the SPF record of %s does not authorise the sending address %s", returnPath, addr), nil
		}
	}
	return pass("ReturnPathValid", "%s has its MX and an SPF record that covers the sending addresses", returnPath), nil
}

func spfRecord(ctx context.Context, r Resolver, domain string) (string, Result, error) {
	records, err := r.TXT(ctx, domain)
	if err != nil {
		return "", Result{}, err
	}
	var spf []string
	for _, rec := range records {
		if rec == "v=spf1" || strings.HasPrefix(strings.ToLower(rec), "v=spf1 ") {
			spf = append(spf, rec)
		}
	}
	switch len(spf) {
	case 0:
		return "", fail("SPFMissing", "%s has no SPF record", domain), nil
	case 1:
		return spf[0], Result{OK: true}, nil
	default:
		return "", fail("MultipleSPF", "%s has %d SPF records: receivers treat that as a permanent error", domain, len(spf)), nil
	}
}

// maxSPFLookups is the limit of RFC 7208 §4.6.4 on DNS-querying terms.
const maxSPFLookups = 10

type spfEval struct {
	r       Resolver
	lookups int
}

// authorises evaluates an SPF record for one address. Only a "pass" counts:
// a soft fail or a neutral result does not authorise.
func (e *spfEval) authorises(ctx context.Context, record, domain string, addr netip.Addr) (bool, error) {
	redirect := ""
	for _, term := range strings.Fields(record)[1:] {
		lower := strings.ToLower(term)
		if v, ok := strings.CutPrefix(lower, "redirect="); ok {
			redirect = v
			continue
		}
		qualifier := byte('+')
		if strings.ContainsRune("+-~?", rune(lower[0])) {
			qualifier, lower = lower[0], lower[1:]
		}
		matched, err := e.matches(ctx, lower, domain, addr)
		if err != nil {
			return false, err
		}
		if matched {
			return qualifier == '+', nil
		}
	}
	if redirect != "" {
		return e.include(ctx, redirect, addr)
	}
	return false, nil
}

func (e *spfEval) include(ctx context.Context, domain string, addr netip.Addr) (bool, error) {
	e.lookups++
	if e.lookups > maxSPFLookups {
		return false, nil
	}
	record, res, err := spfRecord(ctx, e.r, domain)
	if err != nil || !res.OK {
		return false, err
	}
	return e.authorises(ctx, record, domain, addr)
}

func (e *spfEval) matches(ctx context.Context, mech, domain string, addr netip.Addr) (bool, error) {
	name, arg, _ := strings.Cut(mech, ":")
	switch name {
	case "all":
		return true, nil
	case "ip4", "ip6":
		if (name == "ip4") != addr.Is4() {
			return false, nil
		}
		if p, err := netip.ParsePrefix(arg); err == nil {
			return p.Contains(addr), nil
		}
		a, err := netip.ParseAddr(arg)
		return err == nil && a == addr, nil
	case "include":
		return e.include(ctx, arg, addr)
	case "a", "mx":
		target := domain
		if arg != "" {
			target, _, _ = strings.Cut(arg, "/")
		}
		e.lookups++
		if e.lookups > maxSPFLookups {
			return false, nil
		}
		hosts := []string{target}
		if name == "mx" {
			var err error
			if hosts, err = e.r.MX(ctx, target); err != nil {
				return false, err
			}
		}
		for _, h := range hosts {
			addrs, err := e.r.Addrs(ctx, h)
			if err != nil {
				return false, err
			}
			for _, a := range addrs {
				if a == addr {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// CheckDMARC looks for a DMARC policy, on the domain or on its
// organisational domain, which is where receivers fall back to.
func CheckDMARC(ctx context.Context, r Resolver, domain string) (Result, error) {
	candidates := []string{domain}
	if org, err := publicsuffix.EffectiveTLDPlusOne(domain); err == nil && org != domain {
		candidates = append(candidates, org)
	}
	for _, d := range candidates {
		name := "_dmarc." + d
		records, err := r.TXT(ctx, name)
		if err != nil {
			return Result{}, err
		}
		for _, rec := range records {
			if t := tags(rec); t["v"] == "DMARC1" && t["p"] != "" {
				return pass("PolicyPublished", "%s publishes p=%s", name, t["p"]), nil
			}
		}
	}
	return fail("PolicyMissing",
		"no DMARC record at _dmarc.%s: Gmail and Yahoo require one from bulk senders", candidates[len(candidates)-1]), nil
}
