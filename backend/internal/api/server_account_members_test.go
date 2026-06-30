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

func TestMembershipEmailNormalizationParity(t *testing.T) {
	// account_members.member_email must match accounts.email normalization
	// (lowercased + trimmed) so email->account_id resolution does not silently miss.
	for _, in := range []string{"  Deniz@Example.COM ", "deniz@example.com", "\tDENIZ@EXAMPLE.COM\n"} {
		if got := normalizeAccountEmail(in); got != "deniz@example.com" {
			t.Fatalf("normalizeAccountEmail(%q)=%q want deniz@example.com", in, got)
		}
	}
}
