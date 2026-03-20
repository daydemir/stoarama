package email

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type Message struct {
	To          string
	From        string
	ReplyTo     string
	Subject     string
	PlainText   string
	HTML        string
	MessageType string
}

type Sender interface {
	Send(ctx context.Context, msg Message) error
}

type Config struct {
	Provider  string
	From      string
	ReplyTo   string
	ResendKey string
}

func NewSender(cfg Config) (Sender, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "", "log":
		return logSender{}, nil
	case "resend":
		if strings.TrimSpace(cfg.From) == "" {
			return nil, fmt.Errorf("email from address is required for resend")
		}
		if strings.TrimSpace(cfg.ResendKey) == "" {
			return nil, fmt.Errorf("resend api key is required")
		}
		return &resendSender{
			from:       strings.TrimSpace(cfg.From),
			replyTo:    strings.TrimSpace(cfg.ReplyTo),
			apiKey:     strings.TrimSpace(cfg.ResendKey),
			baseURL:    "https://api.resend.com/emails",
			httpClient: &http.Client{Timeout: 15 * time.Second},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported email provider %q", cfg.Provider)
	}
}

type logSender struct{}

func (logSender) Send(_ context.Context, msg Message) error {
	log.Printf(
		"email provider=log type=%s to=%s subject=%q text=%q html_len=%d",
		strings.TrimSpace(msg.MessageType),
		strings.TrimSpace(msg.To),
		strings.TrimSpace(msg.Subject),
		strings.TrimSpace(msg.PlainText),
		len(strings.TrimSpace(msg.HTML)),
	)
	return nil
}

type resendSender struct {
	from       string
	replyTo    string
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

func (s *resendSender) Send(ctx context.Context, msg Message) error {
	from := strings.TrimSpace(msg.From)
	if from == "" {
		from = s.from
	}
	replyTo := strings.TrimSpace(msg.ReplyTo)
	if replyTo == "" {
		replyTo = s.replyTo
	}
	payload := map[string]any{
		"from":    from,
		"to":      []string{strings.TrimSpace(msg.To)},
		"subject": strings.TrimSpace(msg.Subject),
	}
	if text := strings.TrimSpace(msg.PlainText); text != "" {
		payload["text"] = text
	}
	if html := strings.TrimSpace(msg.HTML); html != "" {
		payload["html"] = html
	}
	if replyTo != "" {
		payload["reply_to"] = []string{replyTo}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal resend payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.baseURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build resend request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send resend request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("resend send failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}
