package db

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"
)

var postgresRoleName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

const campaignProductTableManifestSHA256 = "67d9c41a1dd19dcf1939cd69a12eae47a148504da6610453094856eb383b8039"

var campaignAuthorityTables = []string{
	"recording_campaign_authority_decisions", "recording_campaign_admission_approvals",
	"recording_campaign_admission_reservations", "recording_campaign_admission_source_fence_events",
	"recording_targeted_probe_orders", "recording_targeted_provider_attestations",
	"recording_targeted_probe_attempts", "recording_targeted_probe_evidence",
	"recording_targeted_probe_scene_reviews", "recording_campaign_capacity_observations",
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
	"recording_campaign_create_provider_attestation(bigint,bigint,bigint,text,text,text,text)",
	"recording_campaign_create_probe_attempt(uuid,uuid,uuid,uuid,bigint,bigint,bigint,bigint,uuid,bigint,text,text,bigint,text,text,timestamp with time zone,text,text,text,text,bigint,bigint)",
	"recording_campaign_lease_probe(bigint,bigint,bigint,text,bigint,bigint,text,text,text,text,uuid,uuid,text,text,text,text,bigint,bigint)",
	"recording_campaign_create_probe_evidence(uuid,uuid,bigint,bigint,text,double precision,bigint,integer,text,text,text,text,text,text,boolean,integer,integer,double precision,text,bigint,text,text,bigint,text,text,text,text,text,text,text,text,text,text,text,text)",
	"recording_campaign_submit_probe_evidence(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint,bigint,jsonb)",
	"recording_campaign_create_scene_review(uuid,bigint,uuid,uuid,bigint)",
	"recording_campaign_approve(uuid,bigint,bigint,bigint,text,text,text,text,timestamp with time zone,jsonb,jsonb,text)",
	"recording_campaign_queue_probe(uuid,uuid,bigint,bigint,bigint,text,bigint)",
	"recording_campaign_review_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid)",
	"recording_campaign_create_capacity_observation(uuid,bigint,timestamp with time zone,text,integer,integer,integer,integer,text,integer,text,text,text)",
	"recording_campaign_create_capacity_reservation(uuid,bigint,uuid,integer,integer)",
	"recording_campaign_create_storage_observation(uuid,bigint,bigint,timestamp with time zone,bigint,bigint,bigint,integer,integer,bigint,integer,bigint,bigint,bigint,boolean)",
	"recording_campaign_create_storage_reservation(uuid,bigint,uuid,bigint,timestamp with time zone)",
	"recording_campaign_create_admission_result(uuid,uuid,uuid,bigint,bigint,bigint,bigint,bigint,bigint,text,text,text)",
	"recording_campaign_create_admission_commit(uuid,bigint,bigint,bigint,jsonb)",
	"recording_campaign_admit(uuid,bigint,bigint,bigint,text,jsonb,jsonb,jsonb)",
}

var campaignExecutorFunctions = []string{
	"recording_campaign_approve(uuid,bigint,bigint,bigint,text,text,text,text,timestamp with time zone,jsonb,jsonb,text)",
	"recording_campaign_queue_probe(uuid,uuid,bigint,bigint,bigint,text,bigint)",
	"recording_campaign_review_probe_scene(uuid,uuid,bigint,bigint,bigint,text,uuid)",
	"recording_campaign_lease_probe(bigint,bigint,bigint,text,bigint,bigint,text,text,text,text,uuid,uuid,text,text,text,text,bigint,bigint)",
	"recording_campaign_submit_probe_evidence(bigint,bigint,bigint,text,uuid,uuid,uuid,bigint,bigint,jsonb)",
	"recording_campaign_admit(uuid,bigint,bigint,bigint,text,jsonb,jsonb,jsonb)",
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
	if err := pool.QueryRow(ctx, `SELECT NOT rolcanlogin AND NOT rolinherit FROM pg_roles WHERE rolname=$1`, authorityRole).Scan(&authorityOK); err != nil || !authorityOK {
		return fmt.Errorf("campaign authority role is not exact NOLOGIN/NOINHERIT")
	}
	if err := pool.QueryRow(ctx, `SELECT pg_has_role($1,$2,'MEMBER')`, runtimeRole, authorityRole).Scan(&runtimeMember); err != nil || runtimeMember {
		return fmt.Errorf("runtime role must not be a member of campaign authority")
	}
	if err := pool.QueryRow(ctx, `SELECT pg_has_role($1,$2,'MEMBER')`, executorRole, authorityRole).Scan(&executorMember); err != nil || executorMember {
		return fmt.Errorf("admission executor role must not be a member of campaign authority")
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
	var super, member, ownsObjects, schemaCreate, migrationApplied bool
	var invalidTables, invalidProductTables, authoritySequences, executableFunctions int
	var productManifestSHA256 string
	err := pool.QueryRow(ctx, `
		SELECT session_user,current_user,r.rolsuper,
		       pg_has_role(current_user,$2,'MEMBER'),
		       EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)),
		       has_schema_privilege(current_user,current_schema(),'CREATE'),
		       (SELECT count(*) FROM unnest($3::text[]) name
		          LEFT JOIN pg_class c ON c.relname=name
		          LEFT JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname=current_schema()
		         WHERE c.oid IS NULL OR pg_get_userbyid(c.relowner)<>$2 OR
		               has_table_privilege(current_user,c.oid,'INSERT') OR has_table_privilege(current_user,c.oid,'UPDATE') OR
		               has_table_privilege(current_user,c.oid,'DELETE') OR has_table_privilege(current_user,c.oid,'TRUNCATE')),
		       (SELECT encode(sha256(convert_to(string_agg(c.relname,E'\n' ORDER BY c.relname)||E'\n','UTF8')),'hex')
		          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		         WHERE n.nspname=current_schema() AND c.relkind IN('r','p') AND NOT(c.relname=ANY($3::text[]))),
		       (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		         WHERE n.nspname=current_schema() AND c.relkind IN('r','p') AND NOT(c.relname=ANY($3::text[])) AND
		           NOT (has_table_privilege(current_user,c.oid,'SELECT') AND has_table_privilege(current_user,c.oid,'INSERT') AND
		                has_table_privilege(current_user,c.oid,'UPDATE') AND has_table_privilege(current_user,c.oid,'DELETE'))),
		       (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		         WHERE n.nspname=current_schema() AND c.relkind='S' AND pg_get_userbyid(c.relowner)=$2 AND
		           (has_sequence_privilege(current_user,c.oid,'USAGE') OR has_sequence_privilege(current_user,c.oid,'SELECT') OR has_sequence_privilege(current_user,c.oid,'UPDATE'))),
		       (SELECT count(*) FROM unnest($4::text[]) signature
		          LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',current_schema(),signature))
		         WHERE p.oid IS NOT NULL AND has_function_privilege(current_user,p.oid,'EXECUTE')),
		       to_regprocedure(format('%I.recording_campaign_create_admission_commit(uuid,bigint,bigint,bigint,jsonb)',current_schema())) IS NOT NULL
		FROM pg_roles r WHERE r.rolname=current_user`, runtimeRole, authorityRole, campaignAuthorityTables, campaignRuntimeFunctions).Scan(&sessionUser, &currentUser, &super, &member, &ownsObjects, &schemaCreate, &invalidTables, &productManifestSHA256, &invalidProductTables, &authoritySequences, &executableFunctions, &migrationApplied)
	if err != nil {
		return fmt.Errorf("inspect campaign runtime privileges: %w", err)
	}
	if sessionUser != runtimeRole || currentUser != runtimeRole || super || member || ownsObjects || schemaCreate || invalidTables != 0 || productManifestSHA256 != campaignProductTableManifestSHA256 || invalidProductTables != 0 || authoritySequences != 0 || executableFunctions != 0 || !migrationApplied {
		return fmt.Errorf("campaign runtime database privilege boundary is not exact")
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
	var tablePrivileges, invalidFunctions int
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
		       to_regprocedure(format('%I.recording_campaign_create_admission_commit(uuid,bigint,bigint,bigint,jsonb)',current_schema())) IS NOT NULL
		FROM pg_roles r WHERE r.rolname=current_user`, executorRole, authorityRole, campaignRuntimeFunctions, campaignExecutorFunctions).Scan(&sessionUser, &currentUser, &super, &member, &ownsObjects, &schemaCreate, &tablePrivileges, &invalidFunctions, &migrationApplied)
	if err != nil {
		return fmt.Errorf("inspect campaign executor privileges: %w", err)
	}
	if sessionUser != executorRole || currentUser != executorRole || super || member || ownsObjects || schemaCreate || tablePrivileges != 0 || invalidFunctions != 0 || !migrationApplied {
		return fmt.Errorf("campaign executor database privilege boundary is not exact")
	}
	return nil
}
