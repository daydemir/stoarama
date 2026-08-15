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
		"ALTER FUNCTION %I.%s SET search_path=%I,pg_catalog,pg_temp",
		"BEFORE INSERT OR UPDATE OR DELETE ON relay_update_intents",
		"BEFORE TRUNCATE ON relay_update_intents",
		"REFERENCES nodes(id) ON DELETE RESTRICT",
		"REFERENCES node_tokens(id) ON DELETE RESTRICT",
		"REVOKE ALL ON TABLE %I.relay_update_intents FROM PUBLIC,%I,%I",
		"REVOKE ALL ON FUNCTION %I.%s FROM PUBLIC,%I,%I",
		"GRANT EXECUTE ON FUNCTION %I.relay_update_intent_create",
		"GRANT EXECUTE ON FUNCTION %I.relay_update_intent_heartbeat",
		"pg_advisory_xact_lock(hashtextextended('recording-worker-claim-v1',0))",
		"UPDATE nodes SET status='disabled'",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("0142 missing %q", required)
		}
	}
	for _, forbidden := range []string{"artifact_url", "command_argv", "command_env", "shell_command", "ON DELETE CASCADE"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("0142 exposes generic update input %q", forbidden)
		}
	}
}
