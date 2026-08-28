package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestRecordingJoinedBrowserRequiresAccountPrincipal(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		handler http.HandlerFunc
		params  map[string]string
	}{
		{name: "list", path: "/api/v1/account/recordings/7/joined", handler: (&Server{}).handleAccountRecordingJoinedList, params: map[string]string{"id": "7"}},
		{name: "folder", path: "/api/v1/account/recordings/7/joined/folder", handler: (&Server{}).handleAccountRecordingJoinedFolder, params: map[string]string{"id": "7"}},
		{name: "download", path: "/api/v1/account/recordings/7/joined/11/download", handler: (&Server{}).handleAccountRecordingJoinedDownload, params: map[string]string{"id": "7", "joinedId": "11"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			route := chi.NewRouteContext()
			for key, value := range test.params {
				route.URLParams.Add(key, value)
			}
			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
			recorder := httptest.NewRecorder()
			test.handler(recorder, req)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestParseJoinedByteRange(t *testing.T) {
	for _, test := range []struct {
		raw        string
		wantStart  int64
		wantEnd    int64
		wantAbsent bool
		wantError  bool
	}{
		{raw: "", wantAbsent: true},
		{raw: "bytes=0-9", wantStart: 0, wantEnd: 9},
		{raw: "bytes=90-", wantStart: 90, wantEnd: 99},
		{raw: "bytes=-10", wantStart: 90, wantEnd: 99},
		{raw: "bytes=95-200", wantStart: 95, wantEnd: 99},
		{raw: "bytes=100-", wantError: true},
		{raw: "bytes=1-2,4-5", wantError: true},
		{raw: "items=0-9", wantError: true},
	} {
		got, err := parseJoinedByteRange(test.raw, 100)
		if (err != nil) != test.wantError {
			t.Fatalf("range %q error=%v wantError=%v", test.raw, err, test.wantError)
		}
		if test.wantError {
			continue
		}
		if test.wantAbsent {
			if got != nil {
				t.Fatalf("range %q=%+v want absent", test.raw, got)
			}
			continue
		}
		if got == nil || got.start != test.wantStart || got.end != test.wantEnd {
			t.Fatalf("range %q=%+v want %d-%d", test.raw, got, test.wantStart, test.wantEnd)
		}
	}
}

func intPtr(value int) *int { return &value }

func TestJoinedProgressSortingKeepsUnavailableLastAndUsesIDTieBreaker(t *testing.T) {
	progress := map[int64]recordingJoinedProgress{
		1: {SourceDurationMS: 60_000, JoinedReadyMS: 0, Percent: intPtr(0)},
		2: {SourceDurationMS: 60_000, JoinedReadyMS: 30_000, Percent: intPtr(50)},
		3: {SourceDurationMS: 60_000, JoinedReadyMS: 60_000, Percent: intPtr(100)},
		4: {},
		5: {SourceDurationMS: 60_000, JoinedReadyMS: 30_000, Percent: intPtr(50)},
	}
	for _, test := range []struct {
		direction int
		want      []int64
	}{
		{direction: 1, want: []int64{1, 5, 2, 3, 4}},
		{direction: -1, want: []int64{3, 5, 2, 1, 4}},
	} {
		items := []map[string]any{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}, {"id": int64(4)}, {"id": int64(5)}}
		sortRecordingMapsByJoinedProgress(items, progress, test.direction)
		for i, want := range test.want {
			if got := items[i]["id"].(int64); got != want {
				t.Fatalf("direction=%d position=%d got=%d want=%d", test.direction, i, got, want)
			}
		}
	}
}
