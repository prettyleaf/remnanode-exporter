package main

import "testing"

// The Remnawave compose file exposes Valkey over a unix socket, so that form
// has to work as well as plain TCP.
func TestNewRedisAcceptsSocketAndTCP(t *testing.T) {
	cases := map[string]struct{ network, addr string }{
		"unix:///var/run/valkey/valkey.sock": {"unix", "/var/run/valkey/valkey.sock"},
		"redis://remnawave-redis:6379/0":     {"tcp", "remnawave-redis:6379"},
	}
	for url, want := range cases {
		c, err := newRedis(url)
		if err != nil {
			t.Fatalf("newRedis(%q): %v", url, err)
		}
		opts := c.Options()
		if opts.Network != want.network || opts.Addr != want.addr {
			t.Errorf("newRedis(%q) = %s/%s, want %s/%s", url, opts.Network, opts.Addr, want.network, want.addr)
		}
		_ = c.Close()
	}

	if _, err := newRedis("http://nope"); err == nil {
		t.Error("expected an error for an unsupported scheme")
	}
}
