package api

import (
	"os"
	"strings"
	"testing"
)

// These checks make every stale-token and NAS telemetry seam name the same
// append-only authorization table. PostgreSQL lifecycle coverage exercises
// the predicate itself; this regression prevents a later endpoint refactor
// from silently dropping the defense-in-depth fence.
func TestJoinedGapOnlyAuthorizationCoversStaleOperationSurfaces(t *testing.T) {
	tests := []struct {
		file                string
		functions           []string
		batchIndexFunctions []string
	}{
		{
			file: "server_joined_worker_auth.go",
			functions: []string{
				"func (s *Server) joinedOperationWithinScope",
			},
			batchIndexFunctions: []string{"func (s *Server) joinedOperationWithinScope"},
		},
		{
			file: "server_recording_joined_worker.go",
			functions: []string{
				"func (s *Server) recordJoinedExpiredAttemptEvidence",
				"func (s *Server) handleJoinedFailure",
				"func (s *Server) handleJoinedHeartbeat",
				"func (s *Server) validateJoinedRootFinalizeIdentity",
			},
			batchIndexFunctions: []string{
				"func (s *Server) recordJoinedExpiredAttemptEvidence",
				"func (s *Server) handleJoinedFailure",
				"func (s *Server) handleJoinedHeartbeat",
				"func (s *Server) validateJoinedRootFinalizeIdentity",
			},
		},
		{
			file: "server_recording_joined.go",
			functions: []string{
				"func (s *Server) revalidateJoinedArtifactCapability",
				"func (s *Server) handleJoinedArtifactCapability",
			},
			batchIndexFunctions: []string{
				"func (s *Server) revalidateJoinedArtifactCapability",
				"func (s *Server) handleJoinedArtifactCapability",
			},
		},
		{
			file: "server_connections.go",
			functions: []string{
				"func (s *Server) handleAccountConnectionHeartbeat",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.file, func(t *testing.T) {
			raw, err := os.ReadFile(test.file)
			if err != nil {
				t.Fatal(err)
			}
			source := string(raw)
			for _, function := range test.functions {
				body := joinedFunctionSource(t, source, function)
				if !strings.Contains(body, "recording_joined_gap_only_scope_authorizations") ||
					!strings.Contains(body, "work_scope_identity_sha256") {
					t.Fatalf("%s lacks exact joined gap authorization predicate", function)
				}
			}
			for _, function := range test.batchIndexFunctions {
				body := joinedFunctionSource(t, source, function)
				if !strings.Contains(body, "recording_joined_batch_index_refs") ||
					!strings.Contains(body, "ref.reference_kind='hour_manifest'") {
					t.Fatalf("%s lacks joined batch-index gap authorization predicate", function)
				}
			}
		})
	}
}

func joinedFunctionSource(t *testing.T, source, function string) string {
	t.Helper()
	start := strings.Index(source, function)
	if start < 0 {
		t.Fatalf("missing function %q", function)
	}
	body := source[start:]
	if end := strings.Index(body[len(function):], "\nfunc "); end >= 0 {
		body = body[:len(function)+end]
	}
	return body
}
