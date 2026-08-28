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
