package email

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResendSenderSendsIdempotencyKey(t *testing.T) {
	const want = "relay-connectivity-9-deadbeef"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Idempotency-Key"); got != want {
			t.Errorf("Idempotency-Key=%q want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"email-1"}`))
	}))
	defer server.Close()

	sender := resendSender{from: "alerts@example.com", apiKey: "test", baseURL: server.URL, httpClient: server.Client()}
	if _, err := sender.Send(context.Background(), Message{To: "deniz@example.com", Subject: "test", PlainText: "test", IdempotencyKey: want}); err != nil {
		t.Fatal(err)
	}
}
