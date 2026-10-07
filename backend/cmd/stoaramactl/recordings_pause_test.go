package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestRecordingPausePreflight(t *testing.T) {
	for _, tc := range []struct {
		name           string
		account        int64
		source, status string
		apply          bool
		wantError      string
	}{
		{"wrong account", 1, "https://www.youtube.com/watch?v=public", "active", true, "does not match"},
		{"non YouTube", 47, "https://example.org/live.m3u8", "active", true, "not bound"},
		{"completed", 47, "https://www.youtube.com/watch?v=public", "completed", true, "expected active or paused"},
		{"read only", 47, "https://www.youtube.com/watch?v=public", "active", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			posts := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer test-key" {
					t.Error("missing existing auth")
				}
				if r.Method != http.MethodGet {
					posts++
					t.Errorf("unexpected mutation %s", r.URL.Path)
				}
				switch r.URL.Path {
				case "/api/v1/account/me":
					json.NewEncoder(w).Encode(map[string]any{"account": map[string]any{"id": tc.account}})
				case "/api/v1/account/recordings":
					json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{
						{"id": 1, "status": "active", "stream_url": "https://youtu.be/public"},
						{"id": 2, "status": tc.status, "stream_url": tc.source},
					}})
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer s.Close()
			report, err := executeRecordingPause(context.Background(), s.Client(), recordingPauseOptions{47, []int64{1, 2}, tc.apply, true, s.URL, "test-key"})
			if tc.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error=%v", err)
			}
			if posts != 0 || len(report.PausedIDs) != 0 {
				t.Fatal("preflight/dry run mutated recordings")
			}
		})
	}
}

func TestRecordingPauseStopsOnForbidden(t *testing.T) {
	var posts []int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/account/me":
			json.NewEncoder(w).Encode(map[string]any{"account": map[string]any{"id": 47}})
		case "/api/v1/account/recordings":
			json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"id": 1, "status": "active", "stream_url": "https://youtu.be/public"}, {"id": 2, "status": "active", "stream_url": "https://youtu.be/public"}, {"id": 3, "status": "active", "stream_url": "https://youtu.be/public"}}})
		case "/api/v1/account/recordings/1/pause":
			posts = append(posts, 1)
			json.NewEncoder(w).Encode(map[string]any{"id": 1, "status": "paused"})
		case "/api/v1/account/recordings/2/pause":
			posts = append(posts, 2)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error":"restricted test-key"}`))
		default:
			t.Errorf("request after denial or unexpected path %s", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer s.Close()
	report, err := executeRecordingPause(context.Background(), s.Client(), recordingPauseOptions{47, []int64{1, 2, 3}, true, true, s.URL, "test-key"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 403") || strings.Contains(err.Error(), "test-key") {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(posts, []int64{1, 2}) || !reflect.DeepEqual(report.PausedIDs, []int64{1}) || report.Verified {
		t.Fatalf("posts=%v report=%+v", posts, report)
	}
}

func TestRecordingPauseApplyVerifiesAndRetainsOtherSources(t *testing.T) {
	paused := false
	listReads := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/account/me":
			json.NewEncoder(w).Encode(map[string]any{"account": map[string]any{"id": 47}})
		case "/api/v1/account/recordings":
			listReads++
			status := "active"
			var next any = "2026-10-08T06:00:00Z"
			if paused {
				status = "paused"
				next = nil
			}
			json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"id": 1, "status": status, "next_fire_at": next, "stream_url": "https://youtu.be/public"}, {"id": 2, "status": "paused", "next_fire_at": nil, "stream_url": "https://www.youtube.com/watch?v=public"}, {"id": 99, "status": "active", "stream_url": "https://example.org/live.m3u8"}}})
		case "/api/v1/account/recordings/1/pause":
			if r.Method != http.MethodPost {
				t.Error("pause must POST")
			}
			paused = true
			json.NewEncoder(w).Encode(map[string]any{"id": 1, "status": "paused"})
		default:
			t.Errorf("unexpected or non-selected mutation %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer s.Close()
	report, err := executeRecordingPause(context.Background(), s.Client(), recordingPauseOptions{47, []int64{1, 2}, true, true, s.URL, "test-key"})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Verified || !report.PreservesExistingMedia || listReads != 2 || !reflect.DeepEqual(report.PausedIDs, []int64{1}) || !reflect.DeepEqual(report.AlreadyPausedIDs, []int64{2}) {
		t.Fatalf("report=%+v reads=%d", report, listReads)
	}
}

func TestRecordingPauseRejectsAdmissionStillScheduled(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/account/me" {
			json.NewEncoder(w).Encode(map[string]any{"account": map[string]any{"id": 47}})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"items": []map[string]any{{"id": 1, "status": "paused", "next_fire_at": "2026-10-08T06:00:00Z", "stream_url": "https://youtu.be/public"}}})
	}))
	defer s.Close()
	report, err := executeRecordingPause(context.Background(), s.Client(), recordingPauseOptions{47, []int64{1}, true, true, s.URL, "test-key"})
	if err == nil || report.Verified {
		t.Fatal("scheduled admission incorrectly verified")
	}
}

func TestRecordingPauseIDsAndYouTubeHosts(t *testing.T) {
	for _, raw := range []string{"", "1,1", "1,no", "0", "1,"} {
		if _, err := parseRecordingPauseIDs(raw); err == nil {
			t.Errorf("accepted ids %q", raw)
		}
	}
	ids, err := parseRecordingPauseIDs("3, 1,2")
	if err != nil || !reflect.DeepEqual(ids, []int64{1, 2, 3}) {
		t.Fatal(ids, err)
	}
	for _, u := range []string{"https://youtube.com.evil.test/watch", "https://notyoutube.com/watch", "file://youtube.com/watch", "https://evil.test/?next=youtube.com"} {
		if isRecordingPauseYouTubeURL(u) {
			t.Errorf("accepted source %q", u)
		}
	}
}

func TestRecordingPauseNeverFollowsAuthRedirect(t *testing.T) {
	called := false
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(500) }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	defer origin.Close()
	_, err := executeRecordingPause(context.Background(), recordingPauseHTTPClient(), recordingPauseOptions{47, []int64{1}, true, true, origin.URL, "test-key"})
	if err == nil || !strings.Contains(err.Error(), "HTTP 302") || called {
		t.Fatalf("error=%v destination called=%v", err, called)
	}
}
