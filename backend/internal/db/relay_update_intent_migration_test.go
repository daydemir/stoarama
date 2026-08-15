package db

import (
	"os"
	"strings"
	"testing"
)

func TestRelayUpdateIntentMigrationPinsAndNarrowsAuthority(t *testing.T) {
	raw, err := os.ReadFile("../../../infra/sql/migrations/0142_relay_update_intents.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(raw)
	for _, required := range []string{
		"SET search_path=pg_catalog",
		"REVOKE ALL ON TABLE relay_update_intents FROM PUBLIC",
		"REVOKE ALL ON FUNCTION guard_relay_update_intent() FROM PUBLIC",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("0142 missing %q", required)
		}
	}
	for _, forbidden := range []string{"artifact_url", "command_argv", "command_env", "shell_command"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("0142 exposes generic update input %q", forbidden)
		}
	}
}
