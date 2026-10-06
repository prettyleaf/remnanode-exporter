package dict

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"time"
)

// IPSet holds the addresses of the panel's own nodes. The node refresh
// replaces it wholesale while the stream decoders read it, so it is swapped
// atomically instead of being locked.
type IPSet struct {
	addrs atomic.Pointer[map[netip.Addr]struct{}]
}

// Contains reports whether ip is one of the panel's own node addresses. An
// IPv4 address written IPv4-mapped (::ffff:1.2.3.4) matches its plain form.
func (s *IPSet) Contains(ip string) bool {
	if s == nil {
		return false
	}
	set := s.addrs.Load()
	if set == nil {
		return false
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil {
		return false
	}
	_, ok := (*set)[addr.Unmap()]
	return ok
}

// Len is the number of addresses in the set.
func (s *IPSet) Len() int {
	if set := s.addrs.Load(); set != nil {
		return len(*set)
	}
	return 0
}

func (s *IPSet) replace(addrs []netip.Addr) {
	set := make(map[netip.Addr]struct{}, len(addrs))
	for _, a := range addrs {
		set[a.Unmap()] = struct{}{}
	}
	s.addrs.Store(&set)
}

// nodeAddresses collects every address a node is known by: the entries of its
// ips list (Remnawave 3.3.0 and newer) and the address the panel reaches it
// on. That address is often a hostname, which is resolved here; a name that
// does not resolve is skipped rather than failing the refresh.
func nodeAddresses(ctx context.Context, nodes []apiNode, lookup func(context.Context, string) ([]string, error)) []netip.Addr {
	var out []netip.Addr
	add := func(raw string) bool {
		addr, err := netip.ParseAddr(strings.TrimSpace(raw))
		if err != nil {
			return false
		}
		out = append(out, addr)
		return true
	}

	for _, n := range nodes {
		for _, ip := range n.IPs {
			add(ip.IP)
		}
		host := strings.TrimSpace(n.Address)
		if host == "" || add(host) {
			continue
		}
		lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resolved, err := lookup(lctx, host)
		cancel()
		if err != nil {
			continue
		}
		for _, r := range resolved {
			add(r)
		}
	}
	return out
}

// lookupHost is the resolver nodeAddresses uses outside tests.
func lookupHost(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}
