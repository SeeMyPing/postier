// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

package dnscheck

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/miekg/dns"
)

// zone is an in-memory Resolver.
type zone struct {
	txt   map[string][]string
	mx    map[string][]string
	addrs map[string][]netip.Addr
	err   error
}

func (z *zone) TXT(_ context.Context, name string) ([]string, error) { return z.txt[name], z.err }
func (z *zone) MX(_ context.Context, name string) ([]string, error)  { return z.mx[name], z.err }
func (z *zone) Addrs(_ context.Context, name string) ([]netip.Addr, error) {
	return z.addrs[name], z.err
}

var (
	v4 = netip.MustParseAddr("54.36.102.148")
	v6 = netip.MustParseAddr("2001:41d0:305:2100::37f5")
)

func TestCheckChallenge(t *testing.T) {
	ctx := context.Background()
	name := ChallengeName("acme.example")
	cases := map[string]struct {
		records []string
		ok      bool
		reason  string
	}{
		"published":      {[]string{"pst-verify=abc"}, true, "ChallengeFound"},
		"among others":   {[]string{"something else", " pst-verify=abc "}, true, "ChallengeFound"},
		"missing":        {nil, false, "ChallengeMissing"},
		"another tenant": {[]string{"pst-verify=xyz"}, false, "ChallengeMismatch"},
	}
	for label, tc := range cases {
		res, err := CheckChallenge(ctx, &zone{txt: map[string][]string{name: tc.records}}, "acme.example", "abc")
		if err != nil || res.OK != tc.ok || res.Reason != tc.reason {
			t.Errorf("%s: %+v, %v", label, res, err)
		}
	}
}

func TestCheckDKIM(t *testing.T) {
	ctx := context.Background()
	name := DKIMName("pst1", "acme.example")
	cases := map[string]struct {
		records []string
		reason  string
	}{
		"published":         {[]string{DKIMValue("MIIBkey")}, "KeyPublished"},
		"spaces and case":   {[]string{"V=DKIM1;K=rsa;  p=MIIB key"}, "KeyPublished"},
		"without version":   {[]string{"k=rsa; p=MIIBkey"}, "KeyPublished"},
		"missing":           {nil, "KeyMissing"},
		"another key":       {[]string{DKIMValue("OTHER")}, "KeyMismatch"},
		"revoked":           {[]string{"v=DKIM1; p="}, "KeyRevoked"},
		"two records":       {[]string{DKIMValue("MIIBkey"), DKIMValue("MIIBkey")}, "MultipleRecords"},
		"not a DKIM record": {[]string{"v=spf1 -all"}, "NotDKIM"},
	}
	for label, tc := range cases {
		res, err := CheckDKIM(ctx, &zone{txt: map[string][]string{name: tc.records}}, "pst1", "acme.example", "MIIBkey")
		if err != nil || res.Reason != tc.reason || res.OK != (tc.reason == "KeyPublished") {
			t.Errorf("%s: %+v, %v", label, res, err)
		}
	}
}

func TestCheckReturnPath(t *testing.T) {
	ctx := context.Background()
	const rp, mx = "bounce.acme.example", "mta.postier.example"
	base := func(spf ...string) *zone {
		return &zone{
			mx:  map[string][]string{rp: {mx}},
			txt: map[string][]string{rp: spf},
			addrs: map[string][]netip.Addr{
				"mta.postier.example": {v4},
			},
		}
	}
	both := []netip.Addr{v4, v6}

	cases := map[string]struct {
		z       *zone
		sources []netip.Addr
		reason  string
	}{
		"both families": {base("v=spf1 ip4:54.36.102.148 ip6:2001:41d0:305:2100::37f5 -all"), both, "ReturnPathValid"},
		"prefixes":      {base("v=spf1 ip4:54.36.102.0/24 ip6:2001:41d0:305:2100::/64 ~all"), both, "ReturnPathValid"},
		// What happened on the first real send: the record listed IPv4 only
		// and the host sent over IPv6.
		"ipv6 forgotten":         {base("v=spf1 ip4:54.36.102.148 -all"), both, "SPFDoesNotCoverSource"},
		"ipv4 only host":         {base("v=spf1 ip4:54.36.102.148 -all"), []netip.Addr{v4}, "ReturnPathValid"},
		"no sources":             {base("v=spf1 -all"), nil, "ReturnPathValid"},
		"spf missing":            {base(), both, "SPFMissing"},
		"two spf":                {base("v=spf1 ip4:54.36.102.148 -all", "v=spf1 -all"), both, "MultipleSPF"},
		"softfail is not a pass": {base("v=spf1 ~ip4:54.36.102.148 -all"), []netip.Addr{v4}, "SPFDoesNotCoverSource"},
		"fail before pass":       {base("v=spf1 -ip4:54.36.102.148 +all"), []netip.Addr{v4}, "SPFDoesNotCoverSource"},
		"a mechanism":            {base("v=spf1 a:mta.postier.example -all"), []netip.Addr{v4}, "ReturnPathValid"},
		"mx missing":             {&zone{txt: map[string][]string{rp: {"v=spf1 +all"}}}, both, "MXMissing"},
		"mx elsewhere": {&zone{
			mx: map[string][]string{rp: {"mail.other.example"}}, txt: map[string][]string{rp: {"v=spf1 +all"}},
		}, both, "MXMismatch"},
	}
	for label, tc := range cases {
		res, err := CheckReturnPath(ctx, tc.z, rp, "MTA.postier.example.", tc.sources)
		if err != nil || res.Reason != tc.reason || res.OK != (tc.reason == "ReturnPathValid") {
			t.Errorf("%s: %+v, %v", label, res, err)
		}
	}
}

func TestSPFIncludeAndRedirect(t *testing.T) {
	ctx := context.Background()
	z := &zone{
		mx: map[string][]string{"bounce.acme.example": {"mta.postier.example"}},
		txt: map[string][]string{
			"bounce.acme.example":  {"v=spf1 include:spf.postier.example -all"},
			"spf.postier.example":  {"v=spf1 redirect=_spf.postier.example"},
			"_spf.postier.example": {"v=spf1 ip4:54.36.102.148 ip6:2001:41d0:305:2100::37f5 -all"},
			"loop.example":         {"v=spf1 include:loop.example -all"},
		},
	}
	res, err := CheckReturnPath(ctx, z, "bounce.acme.example", "mta.postier.example", []netip.Addr{v4, v6})
	if err != nil || !res.OK {
		t.Errorf("include + redirect: %+v, %v", res, err)
	}
	// A record that includes itself ends, and authorises nothing.
	ev := spfEval{r: z}
	if ok, err := ev.authorises(ctx, z.txt["loop.example"][0], "loop.example", v4); ok || err != nil {
		t.Errorf("looping include: %v, %v", ok, err)
	}
}

func TestCheckDMARC(t *testing.T) {
	ctx := context.Background()
	z := &zone{txt: map[string][]string{"_dmarc.seemyping.xyz": {"v=DMARC1; p=none; rua=mailto:x@y.z"}}}
	// The policy of the organisational domain covers its subdomains.
	if res, err := CheckDMARC(ctx, z, "notifications.seemyping.xyz"); err != nil || !res.OK {
		t.Errorf("inherited policy: %+v, %v", res, err)
	}
	if res, err := CheckDMARC(ctx, z, "seemyping.xyz"); err != nil || !res.OK {
		t.Errorf("own policy: %+v, %v", res, err)
	}
	if res, err := CheckDMARC(ctx, z, "mail.other.example"); err != nil || res.OK || res.Reason != "PolicyMissing" {
		t.Errorf("no policy: %+v, %v", res, err)
	}
	z.txt["_dmarc.other.example"] = []string{"v=DMARC1; rua=mailto:x@y.z"} // no p=
	if res, _ := CheckDMARC(ctx, z, "other.example"); res.OK {
		t.Error("a record without a policy was accepted")
	}
}

// A lookup that fails says nothing: it must come back as an error, never as
// "the record is missing", or a resolver outage would unverify every domain.
func TestLookupFailureIsNotAVerdict(t *testing.T) {
	ctx := context.Background()
	z := &zone{err: errors.New("SERVFAIL")}
	if _, err := CheckChallenge(ctx, z, "a.example", "t"); err == nil {
		t.Error("CheckChallenge hid a lookup failure")
	}
	if _, err := CheckDKIM(ctx, z, "pst1", "a.example", "k"); err == nil {
		t.Error("CheckDKIM hid a lookup failure")
	}
	if _, err := CheckReturnPath(ctx, z, "b.a.example", "mx", nil); err == nil {
		t.Error("CheckReturnPath hid a lookup failure")
	}
	if _, err := CheckDMARC(ctx, z, "a.example"); err == nil {
		t.Error("CheckDMARC hid a lookup failure")
	}
}

// startDNS runs a DNS server on a local port. It plays both parts: the
// recursive resolver and the authoritative server of every zone.
func startDNS(t *testing.T, handler dns.HandlerFunc) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &dns.Server{PacketConn: pc, Handler: handler}
	go func() { _ = srv.ActivateAndServe() }()
	t.Cleanup(func() { _ = srv.Shutdown() })
	return pc.LocalAddr().String()
}

func TestAuthoritativeResolver(t *testing.T) {
	// Written by the server goroutine, read by the test.
	var authoritativeQueries, recursiveQueries atomic.Int64
	addr := startDNS(t, func(w dns.ResponseWriter, req *dns.Msg) {
		q := req.Question[0]
		m := new(dns.Msg)
		m.SetReply(req)
		if req.RecursionDesired {
			recursiveQueries.Add(1)
		} else {
			authoritativeQueries.Add(1)
		}
		rr := func(s string) {
			r, err := dns.NewRR(s)
			if err != nil {
				t.Error(err)
			}
			m.Answer = append(m.Answer, r)
		}
		name := strings.ToLower(q.Name)
		switch {
		case q.Qtype == dns.TypeNS && name == "acme.example.":
			rr("acme.example. 60 IN NS ns1.acme.example.")
		case q.Qtype == dns.TypeA && name == "ns1.acme.example.":
			rr("ns1.acme.example. 60 IN A 127.0.0.1")
		case q.Qtype == dns.TypeTXT && name == "pst1._domainkey.acme.example.":
			// A long key, split in two strings like registrars do.
			rr(`pst1._domainkey.acme.example. 60 IN TXT "v=DKIM1; k=rsa; p=AAAA" "BBBB"`)
		case q.Qtype == dns.TypeTXT && name == "alias.acme.example.":
			rr("alias.acme.example. 60 IN CNAME target.acme.example.")
		case q.Qtype == dns.TypeTXT && name == "target.acme.example.":
			rr(`target.acme.example. 60 IN TXT "through a cname"`)
		case q.Qtype == dns.TypeMX && name == "bounce.acme.example.":
			rr("bounce.acme.example. 60 IN MX 10 MTA.Postier.Example.")
		case name == "broken.acme.example.":
			m.Rcode = dns.RcodeServerFailure
		case name == "nothing.acme.example.":
			m.Rcode = dns.RcodeNameError
		}
		_ = w.WriteMsg(m)
	})
	_, port, _ := net.SplitHostPort(addr)

	r, err := NewResolver(addr, true)
	if err != nil {
		t.Fatal(err)
	}
	r.nsPort = port
	ctx := context.Background()

	txt, err := r.TXT(ctx, "pst1._domainkey.acme.example")
	if err != nil || len(txt) != 1 || txt[0] != "v=DKIM1; k=rsa; p=AAAABBBB" {
		t.Errorf("TXT = %q, %v", txt, err)
	}
	if authoritativeQueries.Load() == 0 {
		t.Error("the record was not asked to the zone's own name server")
	}
	if txt, err := r.TXT(ctx, "alias.acme.example"); err != nil || len(txt) != 1 || txt[0] != "through a cname" {
		t.Errorf("TXT through a CNAME = %q, %v", txt, err)
	}
	if mx, err := r.MX(ctx, "bounce.acme.example"); err != nil || len(mx) != 1 || mx[0] != "mta.postier.example" {
		t.Errorf("MX = %q, %v", mx, err)
	}
	// A name that does not exist is an empty answer; a failing server is an error.
	if txt, err := r.TXT(ctx, "nothing.acme.example"); err != nil || len(txt) != 0 {
		t.Errorf("NXDOMAIN: %q, %v", txt, err)
	}
	if _, err := r.TXT(ctx, "broken.acme.example"); err == nil {
		t.Error("SERVFAIL was reported as an empty answer")
	}

	// Without the authoritative mode, the recursive resolver answers.
	before := authoritativeQueries.Load()
	plain, _ := NewResolver(addr, false)
	if _, err := plain.TXT(ctx, "pst1._domainkey.acme.example"); err != nil {
		t.Fatal(err)
	}
	if authoritativeQueries.Load() != before || recursiveQueries.Load() == 0 {
		t.Error("the plain resolver did not use the recursive server")
	}
}
