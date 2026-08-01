package geoip

import (
	"io"
	"log/slog"
	"net/netip"
	"testing"
)

func TestPrefix(t *testing.T) {
	cases := map[string]string{
		"1.2.3.4":              "1.2.3.0/24",
		"203.0.113.255":        "203.0.113.0/24",
		"2a02:6ea0:c700::1":    "2a02:6ea0:c700::/48",
		"::ffff:1.2.3.4":       "1.2.3.0/24",
		"2001:db8:abcd:ef01::": "2001:db8:abcd::/48",
	}
	for in, want := range cases {
		got := Prefix(netip.MustParseAddr(in))
		if got != want {
			t.Errorf("Prefix(%s) = %s, want %s", in, got, want)
		}
	}
	if got := Prefix(netip.Addr{}); got != "" {
		t.Errorf("Prefix(invalid) = %q, want empty", got)
	}
}

func TestLookupWithoutDatabases(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := New("/nonexistent/city.mmdb", "/nonexistent/asn.mmdb", nil, log)
	defer r.Close()

	got := r.Lookup("8.8.8.8")
	if got.Prefix != "8.8.8.0/24" {
		t.Errorf("Prefix = %q, want 8.8.8.0/24", got.Prefix)
	}
	if got.Country != "" || got.ASN != 0 {
		t.Errorf("expected empty enrichment without databases, got %+v", got)
	}

	if got := r.Lookup("not-an-ip"); got != (Info{}) {
		t.Errorf("Lookup(garbage) = %+v, want zero value", got)
	}
}

func TestLookupSkipsPrivateAddresses(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := New("", "", nil, log)
	defer r.Close()

	got := r.Lookup("10.0.0.7")
	if got.Country != "" || got.ASN != 0 {
		t.Errorf("private address should not be enriched: %+v", got)
	}
	if got.Prefix != "10.0.0.0/24" {
		t.Errorf("Prefix = %q", got.Prefix)
	}
}

func TestIsHosting(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := New("", "", nil, log)
	defer r.Close()

	hosting := []string{"Hetzner Online GmbH", "OVH SAS", "DIGITALOCEAN-ASN", "Amazon.com, Inc."}
	for _, org := range hosting {
		if !r.isHosting(org) {
			t.Errorf("isHosting(%q) = false, want true", org)
		}
	}
	residential := []string{"MTS PJSC", "Deutsche Telekom AG", "Rostelecom", ""}
	for _, org := range residential {
		if r.isHosting(org) {
			t.Errorf("isHosting(%q) = true, want false", org)
		}
	}
}
