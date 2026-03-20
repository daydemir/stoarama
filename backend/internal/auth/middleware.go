package auth

import (
	"net/http"
	"strings"
)

func RequireAPIToken(token string, cookieName string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}

		got := strings.TrimSpace(r.Header.Get("Authorization"))
		const p = "Bearer "
		if strings.HasPrefix(got, p) && strings.TrimSpace(strings.TrimPrefix(got, p)) == token {
			next.ServeHTTP(w, r)
			return
		}

		if cookieName != "" {
			c, err := r.Cookie(cookieName)
			if err == nil && strings.TrimSpace(c.Value) == token {
				next.ServeHTTP(w, r)
				return
			}
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}
