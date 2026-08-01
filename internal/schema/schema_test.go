package schema

import (
	"strings"
	"testing"
)

func TestSplit(t *testing.T) {
	got := Split(`
-- a comment with a ; semicolon inside
CREATE TABLE a (x UInt8) ENGINE = Memory;

-- another comment
CREATE TABLE b (y UInt8) ENGINE = Memory;
`)
	if len(got) != 2 {
		t.Fatalf("got %d statements, want 2: %#v", len(got), got)
	}
	if !strings.HasPrefix(got[0], "CREATE TABLE a") || !strings.HasPrefix(got[1], "CREATE TABLE b") {
		t.Errorf("unexpected statements: %#v", got)
	}
}

// The embedded DDL must survive the naive splitter, so no statement may carry a
// literal semicolon and every {db} placeholder has to be substitutable.
func TestEmbeddedSQLIsWellFormed(t *testing.T) {
	entries, err := files.ReadDir("sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no embedded SQL files")
	}
	for _, e := range entries {
		raw, err := files.ReadFile("sql/" + e.Name())
		if err != nil {
			t.Fatal(err)
		}
		body := strings.ReplaceAll(string(raw), "{db}", "testdb")
		stmts := Split(body)
		if len(stmts) == 0 {
			t.Errorf("%s: no statements", e.Name())
		}
		for _, s := range stmts {
			if strings.Contains(s, "{db}") {
				t.Errorf("%s: unsubstituted placeholder in %.60s", e.Name(), s)
			}
			upper := strings.ToUpper(s)
			if !strings.HasPrefix(upper, "CREATE") {
				t.Errorf("%s: unexpected statement %.60s", e.Name(), s)
			}
			if strings.HasPrefix(upper, "CREATE TABLE") && !strings.Contains(upper, "IF NOT EXISTS") {
				t.Errorf("%s: CREATE TABLE must be idempotent: %.60s", e.Name(), s)
			}
		}
	}
}
