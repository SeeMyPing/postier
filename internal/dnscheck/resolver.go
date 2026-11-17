// SPDX-FileCopyrightText: 2026 Nawer Kezal
// SPDX-License-Identifier: AGPL-3.0-only

// Package dnscheck verifies the DNS records of a sending domain.
package dnscheck

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/miekg/dns"
)

// Resolver looks records up. A name that does not exist, or has no record of
// the type, is an empty answer and no error: it is a fact about the zone. An
// error means the question could not be answered (timeout, SERVFAIL), and
// that nothing can be concluded.
type Resolver interface {
	TXT(ctx context.Context, name string) ([]string, error)
	MX(ctx context.Context, name string) ([]string, error)
	Addrs(ctx context.Context, name string) ([]netip.Addr, error)
}

// DNSResolver implements Resolver with plain DNS queries.
//
// With Authoritative set, each question is put to the name servers of the
// zone themselves rather than to a caching resolver: a record published a
// minute ago is seen at once, and one removed is not remembered for hours.
// On the first real send, a stale SPF record stayed eight hours in a public
// resolver's cache.
type DNSResolver struct {
	// Server is the recursive resolver, "host:port". It answers directly
	// when Authoritative is false, and locates the name servers otherwise.
	Server string
	// Authoritative asks the name servers of each zone directly.
	Authoritative bool
	// nsPort is the port of authoritative servers. 53, except in tests.
	nsPort string

	client *dns.Client
}

// NewResolver returns a resolver. An empty server means the first one of
// /etc/resolv.conf.
func NewResolver(server string, authoritative bool) (*DNSResolver, error) {
	if server == "" {
		conf, err := dns.ClientConfigFromFile("/etc/resolv.conf")
		if err != nil || len(conf.Servers) == 0 {
			return nil, fmt.Errorf("no DNS server configured: %w", err)
		}
		server = net.JoinHostPort(conf.Servers[0], conf.Port)
	}
	if _, _, err := net.SplitHostPort(server); err != nil {
		server = net.JoinHostPort(server, "53")
	}
	return &DNSResolver{
		Server: server, Authoritative: authoritative, nsPort: "53",
		client: &dns.Client{Timeout: 5 * time.Second},
	}, nil
}

const maxCNAMEDepth = 5

// exchange sends one question to one server, retrying over TCP when the
// answer is truncated.
func (r *DNSResolver) exchange(ctx context.Context, server, name string, qtype uint16, recurse bool) (*dns.Msg, error) {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(name), qtype)
	m.RecursionDesired = recurse
	m.SetEdns0(1232, false)
	resp, _, err := r.client.ExchangeContext(ctx, m, server)
	if err == nil && resp.Truncated {
		tcp := &dns.Client{Net: "tcp", Timeout: r.client.Timeout}
		resp, _, err = tcp.ExchangeContext(ctx, m, server)
	}
	if err != nil {
		return nil, err
	}
	switch resp.Rcode {
	case dns.RcodeSuccess, dns.RcodeNameError:
		return resp, nil
	default:
		return nil, fmt.Errorf("%s answered %s for %s", server, dns.RcodeToString[resp.Rcode], name)
	}
}

// nameServers returns the addresses of the name servers of the zone that
// holds name, found by walking up the name.
func (r *DNSResolver) nameServers(ctx context.Context, name string) ([]string, error) {
	labels := dns.SplitDomainName(name)
	for i := range labels {
		zone := strings.Join(labels[i:], ".")
		resp, err := r.exchange(ctx, r.Server, zone, dns.TypeNS, true)
		if err != nil {
			return nil, err
		}
		var servers []string
		for _, rr := range resp.Answer {
			ns, ok := rr.(*dns.NS)
			if !ok {
				continue
			}
			addrs, err := r.recursiveAddrs(ctx, ns.Ns)
			if err != nil {
				continue
			}
			for _, a := range addrs {
				servers = append(servers, net.JoinHostPort(a.String(), r.nsPort))
			}
		}
		if len(servers) > 0 {
			return servers, nil
		}
	}
	return nil, fmt.Errorf("no name server found for %s", name)
}

func (r *DNSResolver) recursiveAddrs(ctx context.Context, name string) ([]netip.Addr, error) {
	var out []netip.Addr
	var firstErr error
	for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
		resp, err := r.exchange(ctx, r.Server, name, qtype, true)
		if err != nil {
			firstErr = errors.Join(firstErr, err)
			continue
		}
		out = append(out, addrsOf(resp)...)
	}
	if len(out) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func addrsOf(resp *dns.Msg) []netip.Addr {
	var out []netip.Addr
	for _, rr := range resp.Answer {
		switch v := rr.(type) {
		case *dns.A:
			if a, ok := netip.AddrFromSlice(v.A.To4()); ok {
				out = append(out, a)
			}
		case *dns.AAAA:
			if a, ok := netip.AddrFromSlice(v.AAAA); ok {
				out = append(out, a)
			}
		}
	}
	return out
}

// query answers one question, from the zone's own servers when
// Authoritative is set, following CNAMEs across zones.
func (r *DNSResolver) query(ctx context.Context, name string, qtype uint16) ([]dns.RR, error) {
	for range maxCNAMEDepth {
		resp, err := r.queryOnce(ctx, name, qtype)
		if err != nil {
			return nil, err
		}
		var matching []dns.RR
		target := ""
		for _, rr := range resp.Answer {
			if rr.Header().Rrtype == qtype {
				matching = append(matching, rr)
			} else if c, ok := rr.(*dns.CNAME); ok {
				target = c.Target
			}
		}
		if len(matching) > 0 || target == "" {
			return matching, nil
		}
		name = target
	}
	return nil, fmt.Errorf("too many CNAMEs for %s", name)
}

func (r *DNSResolver) queryOnce(ctx context.Context, name string, qtype uint16) (*dns.Msg, error) {
	if !r.Authoritative {
		return r.exchange(ctx, r.Server, name, qtype, true)
	}
	servers, err := r.nameServers(ctx, name)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, server := range servers {
		resp, err := r.exchange(ctx, server, name, qtype, false)
		if err == nil {
			return resp, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// TXT implements Resolver. The strings of one record are joined, as DKIM
// and SPF require.
func (r *DNSResolver) TXT(ctx context.Context, name string) ([]string, error) {
	rrs, err := r.query(ctx, name, dns.TypeTXT)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rrs))
	for _, rr := range rrs {
		out = append(out, strings.Join(rr.(*dns.TXT).Txt, ""))
	}
	return out, nil
}

// MX implements Resolver. Hosts are lowercased, without the trailing dot.
func (r *DNSResolver) MX(ctx context.Context, name string) ([]string, error) {
	rrs, err := r.query(ctx, name, dns.TypeMX)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rrs))
	for _, rr := range rrs {
		out = append(out, normalizeHost(rr.(*dns.MX).Mx))
	}
	return out, nil
}

// Addrs implements Resolver.
func (r *DNSResolver) Addrs(ctx context.Context, name string) ([]netip.Addr, error) {
	var out []netip.Addr
	for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
		rrs, err := r.query(ctx, name, qtype)
		if err != nil {
			return nil, err
		}
		out = append(out, addrsOf(&dns.Msg{Answer: rrs})...)
	}
	return out, nil
}

func normalizeHost(h string) string {
	return strings.ToLower(strings.TrimSuffix(h, "."))
}
