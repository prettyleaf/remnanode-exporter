// Package geoip enriches client addresses with MaxMind GeoLite2 City/ASN data.
//
// Both databases are optional: when a file is missing or unreadable the lookup
// degrades to empty fields instead of failing the pipeline. Files are reopened
// periodically so `geoipupdate` can rotate them under a running process.
package geoip

import (
	"context"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/maxminddb-golang"
)

// Info is the flattened enrichment attached to every address we store.
type Info struct {
	Country   string // ISO 3166-1 alpha-2, "" when unknown
	City      string
	ASN       uint32
	ASOrg     string
	IsHosting bool // ASOrg matches a datacenter/VPS keyword
	Prefix    string
}

type cityRecord struct {
	Country struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
	RegisteredCountry struct {
		ISOCode string `maxminddb:"iso_code"`
	} `maxminddb:"registered_country"`
	City struct {
		Names map[string]string `maxminddb:"names"`
	} `maxminddb:"city"`
}

type asnRecord struct {
	Number uint   `maxminddb:"autonomous_system_number"`
	Org    string `maxminddb:"autonomous_system_organization"`
}

// DefaultHostingKeywords flags ASNs that are almost never a residential user.
// A subscriber whose traffic originates from these is either reselling access
// or chaining the node through another VPS.
var DefaultHostingKeywords = []string{
	"hosting", "host", "datacenter", "data center", "dedicated", "server",
	"cloud", "vps", "colo", "ovh", "hetzner", "digitalocean", "linode",
	"vultr", "contabo", "scaleway", "leaseweb", "m247", "choopa", "amazon",
	"aws", "google llc", "microsoft", "azure", "oracle", "alibaba", "tencent",
	"packet", "equinix", "cogent", "netcup", "aeza", "vdsina", "timeweb",
	"selectel", "firstbyte", "ihor", "serverius", "worldstream", "clouvider",
}

// Resolver looks addresses up in the two MaxMind databases.
// The zero value is unusable; call New.
type Resolver struct {
	cityPath string
	asnPath  string
	keywords []string

	mu   sync.RWMutex
	city *maxminddb.Reader
	asn  *maxminddb.Reader

	log *slog.Logger
}

// New opens the databases. Missing paths are tolerated and simply disable the
// corresponding half of the enrichment.
func New(cityPath, asnPath string, hostingKeywords []string, log *slog.Logger) *Resolver {
	src := hostingKeywords
	if len(src) == 0 {
		src = DefaultHostingKeywords
	}
	kw := make([]string, len(src))
	for i, v := range src {
		kw[i] = strings.ToLower(v)
	}
	r := &Resolver{cityPath: cityPath, asnPath: asnPath, keywords: kw, log: log}
	r.reload()
	return r
}

// Watch reopens the databases every interval until ctx is cancelled.
func (r *Resolver) Watch(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.reload()
		}
	}
}

func (r *Resolver) reload() {
	city := openDB(r.cityPath, "city", r.log)
	asn := openDB(r.asnPath, "asn", r.log)

	r.mu.Lock()
	oldCity, oldASN := r.city, r.asn
	if city != nil {
		r.city = city
	}
	if asn != nil {
		r.asn = asn
	}
	r.mu.Unlock()

	// Only close the previous handles we actually replaced.
	if city != nil && oldCity != nil {
		_ = oldCity.Close()
	}
	if asn != nil && oldASN != nil {
		_ = oldASN.Close()
	}
}

func openDB(path, kind string, log *slog.Logger) *maxminddb.Reader {
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		log.Warn("geoip database unavailable", "kind", kind, "path", path, "err", err)
		return nil
	}
	db, err := maxminddb.Open(path)
	if err != nil {
		log.Warn("geoip database open failed", "kind", kind, "path", path, "err", err)
		return nil
	}
	return db
}

// Close releases both readers.
func (r *Resolver) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.city != nil {
		_ = r.city.Close()
		r.city = nil
	}
	if r.asn != nil {
		_ = r.asn.Close()
		r.asn = nil
	}
}

// Lookup enriches a textual address. Unparseable or private addresses come back
// with empty geo fields but still carry a Prefix so they group in dashboards.
func (r *Resolver) Lookup(raw string) Info {
	addr, err := netip.ParseAddr(strings.TrimSpace(raw))
	if err != nil {
		return Info{}
	}
	info := Info{Prefix: Prefix(addr)}
	if !addr.IsValid() || addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() {
		return info
	}
	ip := net.IP(addr.AsSlice())

	r.mu.RLock()
	city, asn := r.city, r.asn
	r.mu.RUnlock()

	if city != nil {
		var rec cityRecord
		if err := city.Lookup(ip, &rec); err == nil {
			info.Country = rec.Country.ISOCode
			if info.Country == "" {
				info.Country = rec.RegisteredCountry.ISOCode
			}
			if n, ok := rec.City.Names["en"]; ok {
				info.City = n
			}
		}
	}
	if asn != nil {
		var rec asnRecord
		if err := asn.Lookup(ip, &rec); err == nil {
			info.ASN = uint32(rec.Number)
			info.ASOrg = rec.Org
			info.IsHosting = r.isHosting(rec.Org)
		}
	}
	return info
}

func (r *Resolver) isHosting(org string) bool {
	if org == "" {
		return false
	}
	low := strings.ToLower(org)
	for _, kw := range r.keywords {
		if strings.Contains(low, kw) {
			return true
		}
	}
	return false
}

// Prefix collapses an address to the block a single subscriber realistically
// occupies: /24 for IPv4, /48 for IPv6. Counting distinct prefixes instead of
// distinct addresses avoids false "sharing" alarms from CGNAT rotation.
func Prefix(addr netip.Addr) string {
	if !addr.IsValid() {
		return ""
	}
	bits := 24
	if addr.Is6() && !addr.Is4In6() {
		bits = 48
	}
	if addr.Is4In6() {
		addr = addr.Unmap()
	}
	p, err := addr.Prefix(bits)
	if err != nil {
		return addr.String()
	}
	return p.String()
}
