package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func stitchReadbackSyntheticMessages() []string {
	const run = "11111111-1111-4111-8111-111111111111"
	plan, digest, pair := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	chunk := strings.Repeat("a1", 512)
	messages := []string{fmt.Sprintf("V85r proof-v1 %s %s 0/1 %s", plan, digest, chunk)}
	for _, kind := range []string{"task-v2", "producer-v2", "reader-v2", "pair-v2", "readback-v2"} {
		messages = append(messages, fmt.Sprintf("V85r %s %s %s %s %s %s 512 0/1 %s", kind, plan, digest, run, pair, run, chunk))
	}
	return messages
}

func TestNASStitchReadbackMetadataFramingSurvivesRedaction(t *testing.T) {
	for _, message := range stitchReadbackSyntheticMessages() {
		if len(message) > nasClientLogMaxLineBytes || redactNASClientLogLine(message) != message {
			t.Fatal("typed proof/readback chunk changed in diagnostic transport")
		}
	}
}

func TestNASStitchReadbackMetadataLostAckReplayPersistsRepeatedSequences(t *testing.T) {
	pool := cleanupTestPool(t)
	server := &Server{pool: pool}
	messages := stitchReadbackSyntheticMessages()
	lines := make([]map[string]any, len(messages))
	for n, message := range messages {
		lines[n] = map[string]any{"seq": 1, "at": "2026-11-15T00:00:00Z", "level": "INFO", "message": message}
	}
	for pass := range 2 {
		response := cleanupCall(t, server.handleAccountNASClientLogs, 47, 5, "", "", map[string]any{
			"client_version": nasCleanupR2CopyControlClientVersion,
			"boot_id":        "11111111-1111-4111-8111-111111111111",
			"lines":          lines,
		})
		var ack struct {
			OK    bool `json:"ok"`
			Lines int  `json:"stored_lines"`
		}
		if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &ack) != nil || !ack.OK || ack.Lines != len(lines) {
			t.Fatalf("metadata pass %d not durably acknowledged: %d %s", pass, response.Code, response.Body)
		}
	}
	request := httptest.NewRequest("GET", "/x?since=1h&grep=V85r&limit=128", nil)
	route := chi.NewRouteContext()
	route.URLParams.Add("id", "13")
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
	response := httptest.NewRecorder()
	server.handleAdminNASClientLogs(response, request)
	var result struct {
		Lines []struct {
			Message string `json:"message"`
		} `json:"lines"`
	}
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &result) != nil || len(result.Lines) != 2*len(messages) {
		t.Fatalf("metadata replay readback differs: %d %s", response.Code, response.Body)
	}
	for n, line := range result.Lines {
		if line.Message != messages[n%len(messages)] {
			t.Fatal("persisted metadata chunk changed or repeated sequence was discarded")
		}
	}
}
