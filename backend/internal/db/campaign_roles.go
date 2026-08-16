package db

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
)

var postgresRoleName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

const campaignProductTableManifestSHA256 = "769af37338fc1a6775f1e7be93a55255fe21d8eebea9201e27f1ae4ddd6eabfb"

var campaignAuthorityTables = []string{
	"recording_campaign_authoritative_frame_witnesses",
	"recording_campaign_authority_decisions", "recording_campaign_admission_approvals",
	"recording_campaign_admission_reservations", "recording_campaign_admission_source_fence_events",
	"recording_campaign_admission_reservation_terminal_events",
	"recording_targeted_probe_orders", "recording_targeted_provider_attestations",
	"recording_targeted_probe_attempts", "recording_targeted_probe_evidence",
	"recording_targeted_probe_attempt_terminal_events",
	"recording_targeted_probe_scene_presentations", "recording_targeted_probe_scene_reviews", "recording_campaign_capacity_observations",
	"recording_campaign_baseline_scene_read_receipts", "recording_campaign_baseline_scene_presentations",
	"recording_campaign_capacity_reservations", "recording_campaign_storage_observations",
	"recording_campaign_storage_reservations", "recording_campaign_admission_results",
	"recording_campaign_admission_commits", "recording_campaign_admission_tx_authorizations",
}

var campaignRuntimeFunctions = []string{
	"recording_campaign_now()",
	"recording_campaign_authorize_account(text,uuid,bigint,bigint,bigint,text)",
	"recording_campaign_authorize_node(text,uuid,bigint,bigint,bigint,bigint,text)",
	"recording_campaign_create_approval(uuid,bigint,bigint,text,text,text,timestamp with time zone,jsonb,jsonb,text)",
	"recording_campaign_create_probe_order(uuid,uuid,bigint,bigint,bigint)",
	"recording_campaign_create_provider_attestation(bigint,bigint,bigint,text,text,text,text,text,text)",
	"recording_campaign_create_probe_attempt(uuid,uuid,uuid,uuid,bigint,bigint,bigint,bigint,uuid,bigint,text,text,bigint,text,text,timestamp with time zone,text,text,text,text,bigint,bigint)",
	"recording_campaign_lease_probe(bigint,bigint,bigint,text,bigint,bigint,text,text,text,text,text,text,uuid,uuid,text,text,text,text,bigint,bigint)",
	"recording_campaign_create_probe_evidence(uuid,uuid,bigint,bigint,text,double precision,bigint,integer,text,text,text,text,text,text,boolean,integer,integer,double precision,text,bigint,text,text,bigint,text,text,text,text,text,text,text,text,text,text,text,text,text)",
	"recording_campaign_submit_probe_evidence(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint,bigint,jsonb)",
	"recording_campaign_read_probe_attempt(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint)",
	"recording_campaign_prepare_authoritative_frame(bigint,text,bigint)",
	"recording_campaign_authorize_authoritative_frame(bigint,text,bigint,bigint,text)",
	"recording_campaign_read_probe_order_status(bigint,bigint,bigint,text,uuid)",
	"recording_campaign_seal_authoritative_frame(bigint,text,bigint,bigint,bigint,text,text)",
	"recording_campaign_assert_baseline_frame_authority(bigint,text,bigint,bigint)",
	"validate_recording_campaign_authoritative_frame_witness()",
	"recording_campaign_create_scene_presentation(uuid,bigint,uuid,uuid,bigint)",
	"recording_campaign_create_scene_review(uuid,bigint,uuid,uuid,uuid,bigint)",
	"recording_campaign_approve(uuid,bigint,bigint,bigint,text,text,text,text,timestamp with time zone,jsonb,jsonb,text)",
	"recording_campaign_expire_approval(uuid,uuid,bigint,bigint,bigint,text)",
	"recording_campaign_queue_probe(uuid,uuid,bigint,bigint,bigint,text,bigint)",
	"recording_campaign_present_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid)",
	"recording_campaign_review_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid,uuid)",
	"recording_campaign_read_probe_scene(bigint,bigint,bigint,text,uuid)",
	"recording_campaign_read_baseline_scene(uuid,bigint,bigint,bigint,text,text,bigint,bigint)",
	"recording_campaign_present_baseline_scene(uuid,uuid,bigint,bigint,bigint,text,text,bigint,bigint)",
	"recording_campaign_attest_baseline_scene(uuid,bigint,bigint,bigint,text,bigint,text)",
	"recording_campaign_create_capacity_observation(uuid,bigint,timestamp with time zone,timestamp with time zone,text,text,text,integer,integer,integer,integer,text,integer,integer,integer,integer,integer,text,text,text,text)",
	"recording_campaign_create_capacity_reservation(uuid,bigint,uuid,integer,integer)",
	"recording_campaign_forecast_peak_slots(bigint)",
	"recording_campaign_relay_failure_capacity(bigint)",
	"recording_campaign_create_storage_observation(uuid,bigint,bigint,timestamp with time zone,bigint,bigint,bigint,integer,integer,bigint,integer,bigint,bigint,bigint,boolean)",
	"recording_campaign_create_storage_reservation(uuid,bigint,uuid,bigint,timestamp with time zone)",
	"recording_campaign_create_admission_result(uuid,uuid,uuid,bigint,bigint,bigint,bigint,bigint,bigint,text,text,text)",
	"recording_campaign_create_admission_commit(uuid,bigint,bigint,bigint,jsonb)",
	"recording_campaign_admit(uuid,bigint,bigint,bigint,text,jsonb,jsonb,jsonb,jsonb)",
	"recording_campaign_replay(uuid,bigint,text)",
	"recording_campaign_replay_approval(bigint,uuid,text,text)",
	"transition_recording_campaign_track(bigint,text,text[],bigint,timestamp with time zone)",
}

var campaignRuntimeReadOnlyProductTables = []string{
	"recording_campaign_track_events",
}

// These product rows are read only by authority-owned SECURITY DEFINER
// procedures. The executor must not receive direct table access, but startup
// must prove the definer owner can resolve the exact protected frame bytes.
var campaignAuthorityReadOnlyProductTables = []string{
	"frames", "media_objects", "protected_campaign_recordings",
}

// Admission serializes against ordinary product writers by locking these
// product identities. PostgreSQL requires UPDATE privilege for FOR UPDATE and
// FOR SHARE, but the authority receives it only on each immutable key column.
var campaignAuthorityLockProductColumns = []string{
	"accounts:id", "account_sessions:id", "connections:id", "frames:id", "media_objects:id",
	"memberships:user_id", "node_tokens:id", "nodes:id", "recorder_droplets:id",
	"recording_scene_frame_evidence:id", "recording_worker_claim_heads:node_id",
	"stream_source_revisions:id", "streams:id", "users:id",
}

var campaignRuntimeDeniedProductSequences = []string{
	"recording_campaign_track_events_id_seq",
}

var campaignRuntimeProductFunctions = []string{
	"recording_surrender_source_snapshot(bigint)",
	"recording_surrender_destination_snapshot(bigint)",
	"recording_surrender_capture_config_snapshot(bigint,bigint,uuid)",
	"recording_surrender_token_can_access_lease(bigint,bigint,bigint,bigint)",
	"recording_surrender_reconcile_expired_upload_sessions()",
	"recording_surrender_expire_set_plans()",
	"recording_surrender_reclaim_expired()",
	"recording_surrender_request_sha(uuid,text,text,bigint,uuid,bigint,integer,bigint,integer)",
	"recording_surrender_relay_candidate_eligible(bigint,bigint)",
	"recording_surrender_relay_alternate(bigint,text)",
	"recording_worker_targeted_probe_occupancy(bigint)",
}

var campaignExecutorFunctions = []string{
	"recording_campaign_seal_authoritative_frame(bigint,text,bigint,bigint,bigint,text,text)",
	"recording_campaign_assert_baseline_frame_authority(bigint,text,bigint,bigint)",
	"recording_campaign_prepare_authoritative_frame(bigint,text,bigint)",
	"recording_campaign_authorize_authoritative_frame(bigint,text,bigint,bigint,text)",
	"recording_campaign_read_probe_order_status(bigint,bigint,bigint,text,uuid)",
	"recording_campaign_approve(uuid,bigint,bigint,bigint,text,text,text,text,timestamp with time zone,jsonb,jsonb,text)",
	"recording_campaign_expire_approval(uuid,uuid,bigint,bigint,bigint,text)",
	"recording_campaign_queue_probe(uuid,uuid,bigint,bigint,bigint,text,bigint)",
	"recording_campaign_present_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid)",
	"recording_campaign_review_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid,uuid)",
	"recording_campaign_read_probe_scene(bigint,bigint,bigint,text,uuid)",
	"recording_campaign_read_baseline_scene(uuid,bigint,bigint,bigint,text,text,bigint,bigint)",
	"recording_campaign_present_baseline_scene(uuid,uuid,bigint,bigint,bigint,text,text,bigint,bigint)",
	"recording_campaign_attest_baseline_scene(uuid,bigint,bigint,bigint,text,bigint,text)",
	"recording_campaign_lease_probe(bigint,bigint,bigint,text,bigint,bigint,text,text,text,text,text,text,uuid,uuid,text,text,text,text,bigint,bigint)",
	"recording_campaign_read_probe_attempt(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint)",
	"recording_campaign_submit_probe_evidence(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint,bigint,jsonb)",
	"recording_campaign_admit(uuid,bigint,bigint,bigint,text,jsonb,jsonb,jsonb,jsonb)",
	"recording_campaign_replay(uuid,bigint,text)",
	"recording_campaign_replay_approval(bigint,uuid,text,text)",
}

// BootstrapCampaignRoles creates only the NOLOGIN admission owner. The runtime
// LOGIN itself must be Render-managed so its credential can be rotated and
// distributed without ever entering SQL, logs, or repository state.
func BootstrapCampaignRoles(ctx context.Context, pool *pgxpool.Pool, runtimeRole, executorRole, authorityRole string) error {
	if !postgresRoleName.MatchString(runtimeRole) || !postgresRoleName.MatchString(executorRole) || !postgresRoleName.MatchString(authorityRole) || runtimeRole == authorityRole || executorRole == authorityRole || runtimeRole == executorRole {
		return fmt.Errorf("campaign database role names are invalid or not distinct")
	}
	var migrator string
	var super bool
	if err := pool.QueryRow(ctx, `SELECT current_user,r.rolsuper FROM pg_roles r WHERE r.rolname=current_user`).Scan(&migrator, &super); err != nil {
		return fmt.Errorf("inspect campaign migrator identity: %w", err)
	}
	if migrator == runtimeRole || migrator == executorRole || (!postgresRoleName.MatchString(migrator) && !super) {
		return fmt.Errorf("migration connection is not a distinct privileged login")
	}
	var loginsExact bool
	if err := pool.QueryRow(ctx, `SELECT count(*)=2 AND bool_and(rolcanlogin) FROM pg_roles WHERE rolname=ANY($1::text[])`, []string{runtimeRole, executorRole}).Scan(&loginsExact); err != nil || !loginsExact {
		return fmt.Errorf("Render-managed runtime and admission executor LOGINs are absent or not exact")
	}
	authority := pgxIdentifier(authorityRole)
	migratorID := pgxIdentifier(migrator)
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DO $roles$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='%s') THEN CREATE ROLE %s NOLOGIN NOINHERIT; END IF; END $roles$; GRANT %s TO %s WITH ADMIN OPTION`, authorityRole, authority, authority, migratorID)); err != nil {
		return fmt.Errorf("bootstrap campaign authority owner: %w", err)
	}
	var authorityOK, runtimeMember, executorMember bool
	var authorityMemberCount int
	if err := pool.QueryRow(ctx, `SELECT NOT rolcanlogin AND NOT rolinherit FROM pg_roles WHERE rolname=$1`, authorityRole).Scan(&authorityOK); err != nil || !authorityOK {
		return fmt.Errorf("campaign authority role is not exact NOLOGIN/NOINHERIT")
	}
	if err := pool.QueryRow(ctx, `SELECT pg_has_role($1,$2,'MEMBER')`, runtimeRole, authorityRole).Scan(&runtimeMember); err != nil || runtimeMember {
		return fmt.Errorf("runtime role must not be a member of campaign authority")
	}
	if err := pool.QueryRow(ctx, `SELECT pg_has_role($1,$2,'MEMBER')`, executorRole, authorityRole).Scan(&executorMember); err != nil || executorMember {
		return fmt.Errorf("admission executor role must not be a member of campaign authority")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_auth_members membership JOIN pg_roles role ON role.oid=membership.roleid JOIN pg_roles member ON member.oid=membership.member WHERE role.rolname=$1 AND member.rolname=$2`, authorityRole, migrator).Scan(&authorityMemberCount); err != nil || authorityMemberCount != 1 {
		return fmt.Errorf("campaign authority must have the exact migrator as its sole member")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_auth_members membership JOIN pg_roles role ON role.oid=membership.roleid WHERE role.rolname=$1`, authorityRole).Scan(&authorityMemberCount); err != nil || authorityMemberCount != 1 {
		return fmt.Errorf("campaign authority has an unreviewed additional member")
	}
	return nil
}

func pgxIdentifier(v string) string {
	// Callers validate through postgresRoleName, so identifier quoting remains
	// deterministic and cannot introduce SQL tokens.
	return `"` + v + `"`
}

// ValidateCampaignRuntimePrivileges is called before serving HTTP. It refuses
// owner/superuser/member drift and verifies the runtime cannot write an
// authority witness directly while retaining exact function execution.
func ValidateCampaignRuntimePrivileges(ctx context.Context, pool *pgxpool.Pool, runtimeRole, authorityRole string) error {
	if !postgresRoleName.MatchString(runtimeRole) || !postgresRoleName.MatchString(authorityRole) {
		return fmt.Errorf("campaign runtime role configuration is invalid")
	}
	var sessionUser, currentUser string
	var super, member, ownsObjects, schemaCreate, migratorLedgerDenied, migrationApplied bool
	var invalidTables, invalidProductTables, authoritySequences, invalidProductSequences, executableFunctions, missingProductFunctions, authorityMembers int
	var productManifestSHA256 string
	err := pool.QueryRow(ctx, `
		SELECT session_user,current_user,r.rolsuper,
		       pg_has_role(current_user,$2,'MEMBER'),
		       EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)),
		       has_schema_privilege(current_user,current_schema(),'CREATE'),
		       (SELECT count(*) FROM unnest($3::text[]) name
		          LEFT JOIN pg_namespace n ON n.nspname=current_schema()
		          LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=name
		         WHERE c.oid IS NULL OR pg_get_userbyid(c.relowner)<>$2 OR has_table_privilege(current_user,c.oid,'SELECT') OR
		               has_table_privilege(current_user,c.oid,'INSERT') OR has_table_privilege(current_user,c.oid,'UPDATE') OR
		               has_table_privilege(current_user,c.oid,'DELETE') OR has_table_privilege(current_user,c.oid,'TRUNCATE')),
		       (SELECT encode(sha256(convert_to(string_agg(c.relname,E'\n' ORDER BY c.relname)||E'\n','UTF8')),'hex')
		          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		         WHERE n.nspname=current_schema() AND c.relkind IN('r','p') AND c.relname<>'schema_migrations' AND NOT(c.relname=ANY($3::text[]))),
		       (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		         WHERE n.nspname=current_schema() AND c.relkind IN('r','p') AND c.relname<>'schema_migrations' AND NOT(c.relname=ANY($3::text[])) AND
		           NOT ((c.relname=ANY($6::text[]) AND has_table_privilege(current_user,c.oid,'SELECT') AND
		                 NOT has_table_privilege(current_user,c.oid,'INSERT') AND NOT has_table_privilege(current_user,c.oid,'UPDATE') AND
		                 NOT has_table_privilege(current_user,c.oid,'DELETE') AND NOT has_table_privilege(current_user,c.oid,'TRUNCATE')) OR
		                (NOT(c.relname=ANY($6::text[])) AND has_table_privilege(current_user,c.oid,'SELECT') AND
		                 has_table_privilege(current_user,c.oid,'INSERT') AND has_table_privilege(current_user,c.oid,'UPDATE') AND
		                 has_table_privilege(current_user,c.oid,'DELETE')))),
		       EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		         WHERE n.nspname=current_schema() AND c.relname='schema_migrations' AND c.relkind IN('r','p') AND
		           NOT has_table_privilege(current_user,c.oid,'SELECT') AND NOT has_table_privilege(current_user,c.oid,'INSERT') AND
		           NOT has_table_privilege(current_user,c.oid,'UPDATE') AND NOT has_table_privilege(current_user,c.oid,'DELETE') AND
		           NOT has_table_privilege(current_user,c.oid,'TRUNCATE')),
		       (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		         WHERE n.nspname=current_schema() AND c.relkind='S' AND pg_get_userbyid(c.relowner)=$2 AND
		           (has_sequence_privilege(current_user,c.oid,'USAGE') OR has_sequence_privilege(current_user,c.oid,'SELECT') OR has_sequence_privilege(current_user,c.oid,'UPDATE'))),
		       (SELECT count(*) FROM unnest($7::text[]) name
		          LEFT JOIN pg_namespace n ON n.nspname=current_schema()
		          LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=name AND c.relkind='S'
		         WHERE n.oid IS NULL OR has_sequence_privilege(current_user,c.oid,'USAGE') OR
		               has_sequence_privilege(current_user,c.oid,'SELECT') OR has_sequence_privilege(current_user,c.oid,'UPDATE')),
		       (SELECT count(*) FROM unnest($4::text[]) signature
		          LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',current_schema(),signature))
		         WHERE p.oid IS NOT NULL AND has_function_privilege(current_user,p.oid,'EXECUTE')),
		       (SELECT count(*) FROM unnest($5::text[]) signature
		          LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',current_schema(),signature))
		         WHERE p.oid IS NULL OR NOT has_function_privilege(current_user,p.oid,'EXECUTE')),
		       (SELECT count(*) FROM pg_auth_members membership JOIN pg_roles role ON role.oid=membership.roleid WHERE role.rolname=$2),
		       to_regprocedure(format('%I.recording_campaign_create_admission_commit(uuid,bigint,bigint,bigint,jsonb)',current_schema())) IS NOT NULL
		FROM pg_roles r WHERE r.rolname=current_user AND current_user=$1`, runtimeRole, authorityRole, campaignAuthorityTables, campaignRuntimeFunctions, campaignRuntimeProductFunctions, campaignRuntimeReadOnlyProductTables, campaignRuntimeDeniedProductSequences).Scan(&sessionUser, &currentUser, &super, &member, &ownsObjects, &schemaCreate, &invalidTables, &productManifestSHA256, &invalidProductTables, &migratorLedgerDenied, &authoritySequences, &invalidProductSequences, &executableFunctions, &missingProductFunctions, &authorityMembers, &migrationApplied)
	if err != nil {
		return fmt.Errorf("inspect campaign runtime privileges: %w", err)
	}
	if sessionUser != runtimeRole || currentUser != runtimeRole || super || member || ownsObjects || schemaCreate || invalidTables != 0 || productManifestSHA256 != campaignProductTableManifestSHA256 || invalidProductTables != 0 || !migratorLedgerDenied || authoritySequences != 0 || invalidProductSequences != 0 || executableFunctions != 0 || missingProductFunctions != 0 || authorityMembers != 1 || !migrationApplied {
		var invalidAuthorityNames []string
		_ = pool.QueryRow(ctx, `SELECT COALESCE(array_agg(name ORDER BY name),'{}'::text[]) FROM unnest($1::text[]) name LEFT JOIN pg_namespace n ON n.nspname=current_schema() LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=name WHERE c.oid IS NULL OR pg_get_userbyid(c.relowner)<>$2 OR has_table_privilege(current_user,c.oid,'SELECT') OR has_table_privilege(current_user,c.oid,'INSERT') OR has_table_privilege(current_user,c.oid,'UPDATE') OR has_table_privilege(current_user,c.oid,'DELETE') OR has_table_privilege(current_user,c.oid,'TRUNCATE')`, campaignAuthorityTables, authorityRole).Scan(&invalidAuthorityNames)
		return fmt.Errorf("campaign runtime database privilege boundary is not exact: session=%t current=%t super=%t member=%t owner=%t schema_create=%t authority_tables=%v product_manifest=%t product_tables=%d ledger_denied=%t authority_sequences=%d product_sequences=%d authority_exec=%d product_exec_missing=%d authority_members=%d migration=%t", sessionUser == runtimeRole, currentUser == runtimeRole, super, member, ownsObjects, schemaCreate, invalidAuthorityNames, productManifestSHA256 == campaignProductTableManifestSHA256, invalidProductTables, migratorLedgerDenied, authoritySequences, invalidProductSequences, executableFunctions, missingProductFunctions, authorityMembers, migrationApplied)
	}
	return nil
}

// ValidateCampaignExecutorPrivileges verifies that the API-only executor can
// invoke exactly the reviewed definer surface while having no table authority
// of its own. It must never be distributed to workers or background services.
func ValidateCampaignExecutorPrivileges(ctx context.Context, pool *pgxpool.Pool, executorRole, authorityRole string) error {
	if !postgresRoleName.MatchString(executorRole) || !postgresRoleName.MatchString(authorityRole) {
		return fmt.Errorf("campaign executor role configuration is invalid")
	}
	var sessionUser, currentUser string
	var super, member, ownsObjects, schemaCreate, migrationApplied bool
	var tablePrivileges, invalidFunctions, invalidAuthorityDependencies, invalidAuthorityLockDependencies int
	err := pool.QueryRow(ctx, `
		SELECT session_user,current_user,r.rolsuper,
		       pg_has_role(current_user,$2,'MEMBER'),
		       EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)),
		       has_schema_privilege(current_user,current_schema(),'CREATE'),
		       (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		         WHERE n.nspname=current_schema() AND c.relkind IN('r','p','v','m','S') AND
		           (has_table_privilege(current_user,c.oid,'SELECT') OR has_table_privilege(current_user,c.oid,'INSERT') OR
		            has_table_privilege(current_user,c.oid,'UPDATE') OR has_table_privilege(current_user,c.oid,'DELETE') OR
		            has_table_privilege(current_user,c.oid,'TRUNCATE'))),
		       (SELECT count(*) FROM unnest($3::text[]) signature
		          LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',current_schema(),signature))
		         WHERE p.oid IS NULL OR pg_get_userbyid(p.proowner)<>$2 OR NOT p.prosecdef OR
		               NOT COALESCE(p.proconfig,'{}'::text[]) @> ARRAY[format('search_path=%s, pg_catalog, pg_temp',current_schema())] OR
		               has_function_privilege(current_user,p.oid,'EXECUTE') IS DISTINCT FROM (signature=ANY($4::text[])) OR
		               EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) acl WHERE acl.grantee=0 AND acl.privilege_type='EXECUTE')),
		       (SELECT count(*) FROM unnest($5::text[]) name
		          LEFT JOIN pg_class c ON c.oid=to_regclass(format('%I.%I',current_schema(),name)) AND c.relkind IN('r','p','v','m')
		         WHERE c.oid IS NULL OR NOT has_table_privilege($2,c.oid,'SELECT') OR
		               has_table_privilege($2,c.oid,'INSERT') OR has_table_privilege($2,c.oid,'UPDATE') OR
		               has_table_privilege($2,c.oid,'DELETE') OR has_table_privilege($2,c.oid,'TRUNCATE')),
		       (SELECT count(*) FROM unnest($6::text[]) spec
		          LEFT JOIN pg_class c ON c.oid=to_regclass(format('%I.%I',current_schema(),split_part(spec,':',1))) AND c.relkind IN('r','p')
		         WHERE c.oid IS NULL OR NOT has_table_privilege($2,c.oid,'SELECT') OR
		               split_part(spec,':',2)='' OR NOT has_column_privilege($2,c.oid,split_part(spec,':',2),'UPDATE') OR
		               has_table_privilege($2,c.oid,'UPDATE') OR
		               EXISTS(SELECT 1 FROM pg_attribute attribute WHERE attribute.attrelid=c.oid AND attribute.attnum>0 AND
		                 NOT attribute.attisdropped AND attribute.attname<>split_part(spec,':',2) AND
		                 has_column_privilege($2,c.oid,attribute.attnum,'UPDATE'))),
		       to_regprocedure(format('%I.recording_campaign_create_admission_commit(uuid,bigint,bigint,bigint,jsonb)',current_schema())) IS NOT NULL
		FROM pg_roles r WHERE r.rolname=current_user AND current_user=$1`, executorRole, authorityRole, campaignRuntimeFunctions, campaignExecutorFunctions, campaignAuthorityReadOnlyProductTables, campaignAuthorityLockProductColumns).Scan(&sessionUser, &currentUser, &super, &member, &ownsObjects, &schemaCreate, &tablePrivileges, &invalidFunctions, &invalidAuthorityDependencies, &invalidAuthorityLockDependencies, &migrationApplied)
	if err != nil {
		return fmt.Errorf("inspect campaign executor privileges: %w", err)
	}
	if sessionUser != executorRole || currentUser != executorRole || super || member || ownsObjects || schemaCreate || tablePrivileges != 0 || invalidFunctions != 0 || invalidAuthorityDependencies != 0 || invalidAuthorityLockDependencies != 0 || !migrationApplied {
		return fmt.Errorf("campaign executor database privilege boundary is not exact")
	}
	var productFunctionExec int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM unnest($1::text[]) signature LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',current_schema(),signature)) WHERE p.oid IS NOT NULL AND has_function_privilege(current_user,p.oid,'EXECUTE')`, campaignRuntimeProductFunctions).Scan(&productFunctionExec); err != nil || productFunctionExec != 0 {
		return fmt.Errorf("campaign executor must not inherit recorder runtime function authority")
	}
	return nil
}
