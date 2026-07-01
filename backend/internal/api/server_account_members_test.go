package api

import "testing"

func TestPrincipalIsOwnerDefaults(t *testing.T) {
	// Legacy sessions (empty MemberRole) and explicit owner are owners; member is not.
	cases := []struct {
		role string
		want bool
	}{
		{"", true},      // legacy session / no row -> owner (safe default)
		{"owner", true}, // explicit owner
		{"member", false},
	}
	for _, c := range cases {
		if got := principalIsOwner(accountPrincipal{MemberRole: c.role}); got != c.want {
			t.Fatalf("principalIsOwner(MemberRole=%q)=%v want %v", c.role, got, c.want)
		}
	}
}

func TestCanRemoveMemberLastOwnerGuard(t *testing.T) {
	cases := []struct {
		name       string
		targetRole string
		ownerCount int
		want       bool
	}{
		{"sole owner refused", "owner", 1, false},
		{"owner among many allowed", "owner", 2, true},
		{"member always allowed", "member", 1, true},
		{"member with many owners allowed", "member", 3, true},
	}
	for _, c := range cases {
		if got := canRemoveMember(c.targetRole, c.ownerCount); got != c.want {
			t.Fatalf("%s: canRemoveMember(%q,%d)=%v want %v", c.name, c.targetRole, c.ownerCount, got, c.want)
		}
	}
}

func TestValidateMemberRoleChange(t *testing.T) {
	cases := []struct {
		name       string
		targetRole string
		newRole    string
		ownerCount int
		wantOK     bool
	}{
		{"promote member to billing_admin", "member", "billing_admin", 1, true},
		{"demote billing_admin to member", "billing_admin", "member", 1, true},
		{"reject unknown role", "member", "admin", 1, false},
		{"reject promoting to owner", "member", "owner", 2, false},
		{"reject empty role", "member", "", 1, false},
		{"reject demoting sole owner", "owner", "member", 1, false},
		{"allow changing owner when others remain", "owner", "member", 2, true},
	}
	for _, c := range cases {
		got, reason := validateMemberRoleChange(c.targetRole, c.newRole, c.ownerCount)
		if got != c.wantOK {
			t.Fatalf("%s: validateMemberRoleChange(%q,%q,%d)=%v (%q) want %v", c.name, c.targetRole, c.newRole, c.ownerCount, got, reason, c.wantOK)
		}
		if !got && reason == "" {
			t.Fatalf("%s: rejection must carry a public reason", c.name)
		}
	}
}

func TestMembershipEmailNormalizationParity(t *testing.T) {
	// users.email must match accounts.email normalization (lowercased + trimmed)
	// so the email->user->memberships resolution does not silently miss.
	for _, in := range []string{"  Deniz@Example.COM ", "deniz@example.com", "\tDENIZ@EXAMPLE.COM\n"} {
		if got := normalizeAccountEmail(in); got != "deniz@example.com" {
			t.Fatalf("normalizeAccountEmail(%q)=%q want deniz@example.com", in, got)
		}
	}
}
