// Package schema applies the ClickHouse DDL at startup.
//
// The DDL is embedded rather than mounted so that upgrading the exporter also
// upgrades the tables, including on a ClickHouse volume that was initialised
// before (docker-entrypoint-initdb.d only runs on a fresh data directory).
package schema

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"remnanode-exporter/internal/sink"
)

//go:embed sql/*.sql
var files embed.FS

// Apply runs every embedded statement in file-name order. All statements are
// IF NOT EXISTS, so this is safe to run on every start.
func Apply(ctx context.Context, w *sink.Writer, log *slog.Logger) error {
	entries, err := files.ReadDir("sql")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		raw, err := files.ReadFile("sql/" + name)
		if err != nil {
			return err
		}
		body := strings.ReplaceAll(string(raw), "{db}", w.DB())
		for i, stmt := range Split(body) {
			if err := w.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s statement %d: %w", name, i+1, err)
			}
		}
		log.Debug("applied schema file", "file", name)
	}
	return nil
}

// Split turns a SQL file into individual statements, dropping line comments and
// blank statements. Statements must not contain a literal semicolon.
func Split(body string) []string {
	var clean strings.Builder
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		clean.WriteString(line)
		clean.WriteString("\n")
	}
	var out []string
	for _, stmt := range strings.Split(clean.String(), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}
