package auth_test

import (
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/auth"
)

func TestToken_IsExpired(t *testing.T) {
	tests := []struct {
		name  string
		token *auth.Token
		want  bool
	}{
		{
			name:  "Token does not exist and is nil",
			token: nil,
			want:  true,
		},
		{
			name: "An empty access token is expired",
			token: &auth.Token{
				AccessToken: "",
				ExpiresAt:   time.Now().Add(1 * time.Hour),
			},
			want: true,
		},
		{
			name: "token already expired 10 minutes ago",
			token: &auth.Token{
				AccessToken: "mock_token",
				ExpiresAt:   time.Now().Add(-10 * time.Minute),
			},
			want: true,
		},
		{
			name: "token expires in 30s (inside 60s safety buffer)",
			token: &auth.Token{
				AccessToken: "mock_token",
				ExpiresAt:   time.Now().Add(30 * time.Second),
			},
			want: true,
		},
		{
			name: "token valid for 1 hour",
			token: &auth.Token{
				AccessToken: "mock_token",
				ExpiresAt:   time.Now().Add(1 * time.Hour),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.token.IsExpired()
			if got != tt.want {
				t.Errorf("IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToken_HasScope(t *testing.T) {
	tok := &auth.Token{
		AccessToken: "mock_token",
		Scopes:      []string{"clips:edit", "user:read:chat"},
	}
	if !tok.HasScope("clips:edit") {
		t.Errorf("expected token to have scope 'clips:edit'")
	}
	if !tok.HasScope("CLIPS:EDIT") { // Case insensitive
		t.Errorf("expected HasScope to be case-insensitive")
	}
	if tok.HasScope("channel:manage:broadcast") {
		t.Errorf("expected token NOT to have scope 'channel:manage:broadcast'")
	}
}
