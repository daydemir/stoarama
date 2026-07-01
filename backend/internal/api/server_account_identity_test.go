package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/email"
)

// captureMailer is a no-op email.Sender for tests that exercise the invite path.
type captureMailer struct{ sent int }

func (m *captureMailer) Send(ctx context.Context, msg email.Message) (email.DeliveryReceipt, error) {
	m.sent++
	return email.DeliveryReceipt{Provider: "test", Status: "sent"}, nil
}

// sessionPrincipal loads the principal for a session token exactly as the auth
// middleware would, so handler tests run against a real resolved principal.
func sessionPrincipal(t *testing.T, s *Server, raw string) accountPrincipal {
	t.Helper()
	p, err := s.lookupAccountSession(context.Background(), raw)
	if err != nil {
		t.Fatalf("resolve principal: %v", err)
	}
	return p
}

// withPrincipal attaches a principal (and optional {id} route param) to a request.
func withPrincipal(req *http.Request, p accountPrincipal, idParam string) *http.Request {
	ctx := context.WithValue(req.Context(), accountPrincipalContextKey, p)
	if idParam != "" {
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("id", idParam)
		ctx = context.WithValue(ctx, chi.RouteCtxKey, rctx)
	}
	return req.WithContext(ctx)
}

// testIdentityServer spins up an isolated Postgres schema, applies the REAL
// migrations (so users/memberships/backfill behave exactly as in prod), and
// returns a *Server wired to that schema. Skips unless STOARAMA_TEST_DATABASE_URL
// is set, matching the existing DB-backed test convention.
func testIdentityServer(t *testing.T) (*Server, *pgxpool.Pool, func()) {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed identity resolution tests")
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schema := fmt.Sprintf("api_identity_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("parse db url: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("open test pool: %v", err)
	}

	migrationDir := findMigrationsDir(t)
	if err := db.MigrateUp(ctx, pool, migrationDir); err != nil {
		pool.Close()
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("apply migrations: %v", err)
	}

	s := &Server{pool: pool}
	cleanup := func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
	}
	return s, pool, cleanup
}

func findMigrationsDir(t *testing.T) string {
	t.Helper()
	// Tests run from internal/api; the migrations live at repo-root
	// infra/sql/migrations.
	candidates := []string{
		"../../../infra/sql/migrations",
		"../../infra/sql/migrations",
		"infra/sql/migrations",
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			abs, err := filepath.Abs(c)
			if err == nil {
				return abs
			}
			return c
		}
	}
	t.Fatalf("cannot locate infra/sql/migrations from %s", mustCwd(t))
	return ""
}

func mustCwd(t *testing.T) string {
	t.Helper()
	cwd, _ := os.Getwd()
	return cwd
}

// insertSession writes an account_sessions row with the given raw token, binding
// user_id + current_org_id (the new resolution keys). Returns nothing; the raw
// token is what lookupAccountSession is called with.
func insertSession(t *testing.T, pool *pgxpool.Pool, orgID, userID int64, rawToken string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO account_sessions (account_id, session_hash, expires_at, last_used_at, user_id, current_org_id)
		VALUES ($1, $2, now() + interval '1 day', now(), $3, $1)
	`, orgID, hashSecret(rawToken), userID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
}

// seedUserOrg creates a user + a personal org + owner membership, mirroring the
// self-signup shape. Returns (userID, orgID).
func seedUserOrg(t *testing.T, pool *pgxpool.Pool, email string, isOperator bool) (int64, int64) {
	t.Helper()
	ctx := context.Background()
	var userID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO users (email, name, is_operator) VALUES ($1, $2, $3) RETURNING id
	`, email, emailLocalPart(email), isOperator).Scan(&userID); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	var orgID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO accounts (email, name, role, status, is_personal)
		VALUES ($1, $2, 'member', 'active', true) RETURNING id
	`, email, emailLocalPart(email)).Scan(&orgID); err != nil {
		t.Fatalf("insert org: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO memberships (user_id, org_id, role, accepted_at)
		VALUES ($1, $2, 'owner', now())
	`, userID, orgID); err != nil {
		t.Fatalf("insert membership: %v", err)
	}
	return userID, orgID
}

// TestLookupSessionResolvesUserOrgMembership: a session bound to user + current
// org resolves to a principal with AccountID=org, the user's email, and the org
// role. An operator user gets Role=admin (drives requireAdminAuth).
func TestLookupSessionResolvesUserOrgMembership(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, orgID := seedUserOrg(t, pool, "resolver@example.com", true)
	raw := "raw-session-token-resolver"
	insertSession(t, pool, orgID, userID, raw)

	p, err := s.lookupAccountSession(context.Background(), raw)
	if err != nil {
		t.Fatalf("lookupAccountSession: %v", err)
	}
	if p.AccountID != orgID {
		t.Fatalf("AccountID=%d want %d (current org)", p.AccountID, orgID)
	}
	if p.UserID != userID {
		t.Fatalf("UserID=%d want %d", p.UserID, userID)
	}
	if p.Email != "resolver@example.com" {
		t.Fatalf("Email=%q want resolver@example.com", p.Email)
	}
	if p.MemberRole != "owner" {
		t.Fatalf("MemberRole=%q want owner", p.MemberRole)
	}
	if p.Role != accountRoleAdmin {
		t.Fatalf("Role=%q want admin (is_operator drives requireAdminAuth)", p.Role)
	}
}

// TestLookupSessionNonOperatorIsMemberRole: a non-operator user resolves with
// Role=member, so requireAdminAuth (Role!=admin) denies them.
func TestLookupSessionNonOperatorIsMemberRole(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, orgID := seedUserOrg(t, pool, "plain@example.com", false)
	raw := "raw-session-token-plain"
	insertSession(t, pool, orgID, userID, raw)

	p, err := s.lookupAccountSession(context.Background(), raw)
	if err != nil {
		t.Fatalf("lookupAccountSession: %v", err)
	}
	if p.Role != accountRoleMember {
		t.Fatalf("Role=%q want member", p.Role)
	}
}

// TestLookupSessionFailsFastOnMissingMembership: if the membership row for
// (user, current org) is gone, resolution returns ErrNoRows (401 upstream). This
// is the fail-fast that replaces the old COALESCE(role,'owner') fallback.
func TestLookupSessionFailsFastOnMissingMembership(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	userID, orgID := seedUserOrg(t, pool, "removed@example.com", false)
	raw := "raw-session-token-removed"
	insertSession(t, pool, orgID, userID, raw)

	if _, err := pool.Exec(context.Background(), `DELETE FROM memberships WHERE user_id=$1 AND org_id=$2`, userID, orgID); err != nil {
		t.Fatalf("delete membership: %v", err)
	}
	_, err := s.lookupAccountSession(context.Background(), raw)
	if err == nil {
		t.Fatalf("expected error after membership removed, got nil (fail-fast broken)")
	}
	if err != pgx.ErrNoRows {
		t.Fatalf("expected pgx.ErrNoRows, got %v", err)
	}
}

// TestMigrationBackfillResolvesLegacyState is the cutover-safety check: seed the
// PRE-0072 shape (accounts + account_members + a legacy account_sessions row with
// member_email), run only the backfill logic that 0072 performs, then confirm the
// session resolves through the new users/memberships path with no logout.
//
// We reproduce the exact prod shape: account 1 = a 2-member team (owner +
// invited member with NULL accepted_at), and a legacy session with NULL
// member_email that must resolve to the owner user.
func TestMigrationBackfillResolvesLegacyState(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()

	// The migration already ran (empty tables). Simulate the pre-0072 raw state by
	// inserting an accounts row + account_members rows + a legacy session with
	// member_email set, THEN re-run the same backfill SQL 0072 uses for the parts
	// that depend on those rows (users, memberships, session binding). This proves
	// the backfill maps a legacy session to (user, org) so it stays logged in.
	var orgID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO accounts (email, name, role, status) VALUES ('owner@team.example','owner','admin','active') RETURNING id
	`).Scan(&orgID); err != nil {
		t.Fatalf("insert team org: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO account_members (account_id, member_email, role, accepted_at) VALUES
		  ($1, 'owner@team.example', 'owner', now()),
		  ($1, 'invited@team.example', 'member', NULL)
	`, orgID); err != nil {
		t.Fatalf("insert members: %v", err)
	}
	// A legacy session: NULL member_email (must resolve to the account owner).
	rawLegacy := "raw-legacy-session"
	if _, err := pool.Exec(ctx, `
		INSERT INTO account_sessions (account_id, session_hash, expires_at, last_used_at)
		VALUES ($1, $2, now() + interval '1 day', now())
	`, orgID, hashSecret(rawLegacy)); err != nil {
		t.Fatalf("insert legacy session: %v", err)
	}

	// Re-run 0072's backfill for these newly-inserted legacy rows.
	if _, err := pool.Exec(ctx, `
		INSERT INTO users (email, name, is_operator)
		SELECT e, '', bool_or(is_admin) FROM (
		  SELECT lower(trim(email)) e, (role='admin') is_admin FROM accounts
		  UNION ALL SELECT lower(trim(member_email)), false FROM account_members
		) s
		WHERE e NOT IN (SELECT email FROM users)
		GROUP BY e
	`); err != nil {
		t.Fatalf("backfill users: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO memberships (user_id, org_id, role, invited_by, invited_at, accepted_at)
		SELECT u.id, am.account_id, am.role, iu.id, am.invited_at, am.accepted_at
		FROM account_members am
		JOIN users u ON u.email=lower(trim(am.member_email))
		LEFT JOIN accounts ib ON ib.id=am.invited_by
		LEFT JOIN users iu ON iu.email=lower(trim(ib.email))
		WHERE NOT EXISTS (SELECT 1 FROM memberships m WHERE m.user_id=u.id AND m.org_id=am.account_id)
	`); err != nil {
		t.Fatalf("backfill memberships: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE account_sessions rs SET current_org_id=rs.account_id, user_id=u.id
		FROM accounts a, users u
		WHERE a.id=rs.account_id AND u.email=lower(trim(COALESCE(rs.member_email, a.email))) AND rs.user_id IS NULL
	`); err != nil {
		t.Fatalf("backfill sessions: %v", err)
	}

	// The legacy session must still resolve: to the owner user + org, owner role.
	p, err := s.lookupAccountSession(ctx, rawLegacy)
	if err != nil {
		t.Fatalf("legacy session did not resolve (would be a logout): %v", err)
	}
	if p.AccountID != orgID || p.Email != "owner@team.example" || p.MemberRole != "owner" {
		t.Fatalf("legacy session resolved to org=%d email=%q role=%q want org=%d owner@team.example owner", p.AccountID, p.Email, p.MemberRole, orgID)
	}
	if p.Role != accountRoleAdmin {
		t.Fatalf("legacy owner Role=%q want admin (is_operator from role=admin)", p.Role)
	}

	// The invited member keeps accepted_at NULL (preserved), proving the backfill
	// does not silently accept an outstanding invite.
	var acceptedAtNull bool
	if err := pool.QueryRow(ctx, `
		SELECT accepted_at IS NULL FROM memberships m JOIN users u ON u.id=m.user_id
		WHERE m.org_id=$1 AND u.email='invited@team.example'
	`, orgID).Scan(&acceptedAtNull); err != nil {
		t.Fatalf("load invited membership: %v", err)
	}
	if !acceptedAtNull {
		t.Fatalf("invited member accepted_at should stay NULL after backfill")
	}
}

// addMembership adds an accepted membership of role in org for user.
func addMembership(t *testing.T, pool *pgxpool.Pool, userID, orgID int64, role string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO memberships (user_id, org_id, role, accepted_at) VALUES ($1,$2,$3,now())
	`, userID, orgID, role); err != nil {
		t.Fatalf("add membership: %v", err)
	}
}

// TestMultiOrgListAndSwitch: a user in two orgs can list both and switch the
// session's current org to the second, which then resolves as the new current org.
func TestMultiOrgListAndSwitch(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()

	userID, orgA := seedUserOrg(t, pool, "multi@example.com", false)
	// A second team org the user also belongs to.
	var orgB int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO accounts (email, name, role, status, is_personal)
		VALUES ('org-b@x.stoarama.internal','Team B','member','active',false) RETURNING id
	`).Scan(&orgB); err != nil {
		t.Fatalf("insert org B: %v", err)
	}
	addMembership(t, pool, userID, orgB, "member")

	raw := "raw-multi-session"
	insertSession(t, pool, orgA, userID, raw)
	p := sessionPrincipal(t, s, raw)

	// List returns both orgs, current flagged on A.
	listReq := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/orgs", nil), p, "")
	listRec := httptest.NewRecorder()
	s.handleAccountOrgsList(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("orgs list status=%d body=%s", listRec.Code, listRec.Body.String())
	}
	var listBody struct {
		Items []accountOrgItem `json:"items"`
	}
	if err := json.Unmarshal(listRec.Body.Bytes(), &listBody); err != nil {
		t.Fatalf("decode orgs list: %v", err)
	}
	if len(listBody.Items) != 2 {
		t.Fatalf("orgs list len=%d want 2", len(listBody.Items))
	}
	var currentOnA bool
	for _, it := range listBody.Items {
		if it.ID == orgA {
			currentOnA = it.Current
		}
	}
	if !currentOnA {
		t.Fatalf("org A should be flagged current")
	}

	// Switch to org B.
	switchReq := withPrincipal(httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/account/orgs/%d/switch", orgB), nil), p, fmt.Sprintf("%d", orgB))
	switchRec := httptest.NewRecorder()
	s.handleAccountOrgSwitch(switchRec, switchReq)
	if switchRec.Code != http.StatusOK {
		t.Fatalf("switch status=%d body=%s", switchRec.Code, switchRec.Body.String())
	}
	// The session now resolves org B as current.
	p2 := sessionPrincipal(t, s, raw)
	if p2.AccountID != orgB {
		t.Fatalf("after switch AccountID=%d want %d", p2.AccountID, orgB)
	}
	if p2.MemberRole != "member" {
		t.Fatalf("after switch MemberRole=%q want member (role in org B)", p2.MemberRole)
	}
}

// TestOrgSwitchDeniedForNonMember: switching into an org the user does not belong
// to is 403 and does not change the session's current org.
func TestOrgSwitchDeniedForNonMember(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	ctx := context.Background()

	userID, orgA := seedUserOrg(t, pool, "nonmember@example.com", false)
	var otherOrg int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO accounts (email, name, role, status, is_personal)
		VALUES ('other-org@x.stoarama.internal','Other','member','active',false) RETURNING id
	`).Scan(&otherOrg); err != nil {
		t.Fatalf("insert other org: %v", err)
	}
	raw := "raw-nonmember-session"
	insertSession(t, pool, orgA, userID, raw)
	p := sessionPrincipal(t, s, raw)

	req := withPrincipal(httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/account/orgs/%d/switch", otherOrg), nil), p, fmt.Sprintf("%d", otherOrg))
	rec := httptest.NewRecorder()
	s.handleAccountOrgSwitch(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("switch to non-member org status=%d want 403 body=%s", rec.Code, rec.Body.String())
	}
	if p2 := sessionPrincipal(t, s, raw); p2.AccountID != orgA {
		t.Fatalf("current org changed to %d after denied switch, want %d", p2.AccountID, orgA)
	}
}

// TestInvitePerOrg409OnlyForSameOrg: inviting an email already a member of THIS
// org is 409; the same email is invitable into a DIFFERENT org (multi-org).
func TestInvitePerOrg409OnlyForSameOrg(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	s.mailer = &captureMailer{}
	s.cfg.MagicLinkTTL = time.Hour

	ownerID, orgA := seedUserOrg(t, pool, "owner-a@example.com", false)
	raw := "raw-owner-a-session"
	insertSession(t, pool, orgA, ownerID, raw)
	pA := sessionPrincipal(t, s, raw)

	invite := func(p accountPrincipal, email string) int {
		body := fmt.Sprintf(`{"email":%q}`, email)
		req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/members", strings.NewReader(body)), p, "")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.handleAccountMembersInvite(rec, req)
		return rec.Code
	}

	// First invite into org A succeeds.
	if code := invite(pA, "guest@example.com"); code != http.StatusCreated {
		t.Fatalf("first invite status=%d want 201", code)
	}
	// Re-invite the same email into org A -> 409 (already a member of THIS org).
	if code := invite(pA, "guest@example.com"); code != http.StatusConflict {
		t.Fatalf("re-invite same org status=%d want 409", code)
	}

	// A second org, owned by a different owner; the same guest email is invitable.
	ownerBID, orgB := seedUserOrg(t, pool, "owner-b@example.com", false)
	rawB := "raw-owner-b-session"
	insertSession(t, pool, orgB, ownerBID, rawB)
	pB := sessionPrincipal(t, s, rawB)
	if code := invite(pB, "guest@example.com"); code != http.StatusCreated {
		t.Fatalf("invite same email into different org status=%d want 201 (multi-org)", code)
	}
	// The guest user now has two memberships.
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM memberships m JOIN users u ON u.id=m.user_id WHERE u.email='guest@example.com'
	`).Scan(&count); err != nil {
		t.Fatalf("count guest memberships: %v", err)
	}
	if count != 2 {
		t.Fatalf("guest memberships=%d want 2 (one per org)", count)
	}
}

// TestSelfSignupCreatesPersonalOrg: a brand-new email requesting a sign-in link
// gets a user + a personal org (is_personal=true) + an owner membership, and a
// magic link bound to that org.
func TestSelfSignupCreatesPersonalOrg(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	s.mailer = &captureMailer{}
	s.cfg.MagicLinkTTL = time.Hour
	s.authLinkLimiter = newAuthLinkLimiter()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/request-link", strings.NewReader(`{"email":"newbie@example.com"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleAccountAuthRequestLink(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("request-link status=%d body=%s", rec.Code, rec.Body.String())
	}

	ctx := context.Background()
	var (
		userID     int64
		orgID      int64
		isPersonal bool
		role       string
	)
	if err := pool.QueryRow(ctx, `
		SELECT u.id, o.id, o.is_personal, m.role
		FROM users u
		JOIN memberships m ON m.user_id=u.id
		JOIN accounts o ON o.id=m.org_id
		WHERE u.email='newbie@example.com'
	`).Scan(&userID, &orgID, &isPersonal, &role); err != nil {
		t.Fatalf("load new user org: %v", err)
	}
	if !isPersonal {
		t.Fatalf("self-signup org is_personal=false want true")
	}
	if role != "owner" {
		t.Fatalf("self-signup membership role=%q want owner", role)
	}
	// The magic link is bound to the user + org.
	var linkUser, linkOrg int64
	if err := pool.QueryRow(ctx, `
		SELECT user_id, target_org_id FROM account_magic_links WHERE account_id=$1 ORDER BY id DESC LIMIT 1
	`, orgID).Scan(&linkUser, &linkOrg); err != nil {
		t.Fatalf("load magic link: %v", err)
	}
	if linkUser != userID || linkOrg != orgID {
		t.Fatalf("magic link bound to user=%d org=%d want user=%d org=%d", linkUser, linkOrg, userID, orgID)
	}
}

// TestLastOwnerGuardOnRemove: an org owner cannot remove the sole remaining owner.
func TestLastOwnerGuardOnRemove(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	ownerID, orgID := seedUserOrg(t, pool, "sole-owner@example.com", false)
	raw := "raw-sole-owner"
	insertSession(t, pool, orgID, ownerID, raw)
	p := sessionPrincipal(t, s, raw)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/account/members/sole-owner@example.com", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("email", "sole-owner@example.com")
	req = req.WithContext(context.WithValue(context.WithValue(req.Context(), accountPrincipalContextKey, p), chi.RouteCtxKey, rctx))
	rec := httptest.NewRecorder()
	s.handleAccountMembersRemove(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("remove sole owner status=%d want 409 body=%s", rec.Code, rec.Body.String())
	}
}

// seedMemberSession adds a membership (given role) for a fresh user in orgID and
// returns a resolved session principal for that member. Used to exercise the
// billing/role gates from the perspective of a specific org role.
func seedMemberSession(t *testing.T, s *Server, pool *pgxpool.Pool, orgID int64, email, role, rawToken string) accountPrincipal {
	t.Helper()
	ctx := context.Background()
	var userID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO users (email, name) VALUES ($1, $2) RETURNING id
	`, email, emailLocalPart(email)).Scan(&userID); err != nil {
		t.Fatalf("insert member user: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO memberships (user_id, org_id, role, accepted_at)
		VALUES ($1, $2, $3, now())
	`, userID, orgID, role); err != nil {
		t.Fatalf("insert %s membership: %v", role, err)
	}
	insertSession(t, pool, orgID, userID, rawToken)
	return sessionPrincipal(t, s, rawToken)
}

// TestBillingWriteGateByRole: a plain member is 403 on the billing write
// endpoints (card + portal) and on the role-change endpoint, while an
// owner/billing_admin clears the gate (billing is nil in tests, so a cleared gate
// surfaces as 503, never 403). Billing READS stay open and are not gated here.
func TestBillingWriteGateByRole(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	_, orgID := seedUserOrg(t, pool, "gate-owner@example.com", false)
	memberP := seedMemberSession(t, s, pool, orgID, "gate-member@example.com", "member", "raw-gate-member")
	adminP := seedMemberSession(t, s, pool, orgID, "gate-billing-admin@example.com", "billing_admin", "raw-gate-admin")

	callCard := func(p accountPrincipal) int {
		req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/billing/card", strings.NewReader("{}")), p, "")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.handleAccountBillingCard(rec, req)
		return rec.Code
	}
	callPortal := func(p accountPrincipal) int {
		req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/account/billing/portal", strings.NewReader("{}")), p, "")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.handleAccountBillingPortal(rec, req)
		return rec.Code
	}

	// A plain member is forbidden on every billing write.
	if code := callCard(memberP); code != http.StatusForbidden {
		t.Fatalf("member POST card status=%d want 403", code)
	}
	if code := callPortal(memberP); code != http.StatusForbidden {
		t.Fatalf("member POST portal status=%d want 403", code)
	}
	// A billing_admin clears the gate; with billing disabled in tests that is 503.
	if code := callCard(adminP); code == http.StatusForbidden {
		t.Fatalf("billing_admin POST card was 403; gate must let billing_admin through")
	}
	if code := callPortal(adminP); code == http.StatusForbidden {
		t.Fatalf("billing_admin POST portal was 403; gate must let billing_admin through")
	}
}

// TestMemberRoleChangeOwnerOnly: a plain member cannot change roles (403); an
// owner can promote a member to billing_admin (persisted) and demote back.
func TestMemberRoleChangeOwnerOnly(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()

	ownerID, orgID := seedUserOrg(t, pool, "role-owner@example.com", false)
	ownerRaw := "raw-role-owner"
	insertSession(t, pool, orgID, ownerID, ownerRaw)
	ownerP := sessionPrincipal(t, s, ownerRaw)
	memberP := seedMemberSession(t, s, pool, orgID, "role-member@example.com", "member", "raw-role-member")

	setRole := func(p accountPrincipal, targetEmail, role string) (int, string) {
		body := fmt.Sprintf(`{"role":%q}`, role)
		req := httptest.NewRequest(http.MethodPatch, "/api/v1/account/members/"+targetEmail+"/role", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("email", targetEmail)
		req = req.WithContext(context.WithValue(context.WithValue(req.Context(), accountPrincipalContextKey, p), chi.RouteCtxKey, rctx))
		rec := httptest.NewRecorder()
		s.handleAccountMemberRoleSet(rec, req)
		return rec.Code, rec.Body.String()
	}

	// A plain member cannot change any role.
	if code, body := setRole(memberP, "role-member@example.com", "billing_admin"); code != http.StatusForbidden {
		t.Fatalf("member role change status=%d want 403 body=%s", code, body)
	}
	// The owner promotes the member to billing_admin, and it persists.
	if code, body := setRole(ownerP, "role-member@example.com", "billing_admin"); code != http.StatusOK {
		t.Fatalf("owner promote status=%d want 200 body=%s", code, body)
	}
	var role string
	if err := pool.QueryRow(context.Background(), `
		SELECT m.role FROM memberships m JOIN users u ON u.id=m.user_id
		WHERE m.org_id=$1 AND u.email='role-member@example.com'
	`, orgID).Scan(&role); err != nil {
		t.Fatalf("load role: %v", err)
	}
	if role != "billing_admin" {
		t.Fatalf("role after promote=%q want billing_admin", role)
	}
	// An unknown role is rejected, and the owner role is not assignable here.
	if code, _ := setRole(ownerP, "role-member@example.com", "owner"); code != http.StatusConflict {
		t.Fatalf("promote to owner status=%d want 409 (owner not assignable here)", code)
	}
	// Demoting the sole owner is refused so the org keeps an owner.
	if code, _ := setRole(ownerP, "role-owner@example.com", "member"); code != http.StatusConflict {
		t.Fatalf("demote sole owner status=%d want 409", code)
	}
}
