package api

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

type recordingBodyReadFailure struct{ err error }

func (b recordingBodyReadFailure) Read([]byte) (int, error) { return 0, b.err }
func (b recordingBodyReadFailure) Close() error             { return nil }

// Exercise both real delivery handlers before any database or storage mutation.
// A server read timeout must reach the existing recorder retry path as HTTP 408;
// invalid JSON must still fail permanently as HTTP 400.
func TestRecordingDeliveryBodyReadTimeoutIsRetryable(t *testing.T) {
	cipher, err := secretbox.NewFromBase64Key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{secrets: cipher}
	handlers := map[string]http.HandlerFunc{"upload-intents": s.handleRecordingUploadIntent, "clips/ingest": s.handleRecordingClipIngest}
	for name, handler := range handlers {
		t.Run(name, func(t *testing.T) {
			cases := []struct {
				name string
				body func() io.ReadCloser
				want int
			}{
				{"read timeout", func() io.ReadCloser {
					return recordingBodyReadFailure{&net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}}
				}, http.StatusRequestTimeout},
				{"malformed JSON", func() io.ReadCloser { return io.NopCloser(strings.NewReader("!")) }, http.StatusBadRequest},
				{"unknown field", func() io.ReadCloser { return io.NopCloser(strings.NewReader(`{"unexpected_field":1}`)) }, http.StatusBadRequest},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/"+name, nil)
					req.Body = tc.body()
					req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{AccountID: 47, NodeID: 93, NodeType: nodeTypeRelay}))
					res := httptest.NewRecorder()
					handler(res, req)
					if res.Code != tc.want {
						t.Fatalf("status=%d want=%d body=%s", res.Code, tc.want, res.Body.String())
					}
				})
			}
		})
	}
}
