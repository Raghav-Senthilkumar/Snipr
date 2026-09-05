package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Raghav-Senthilkumar/Snipr/internal/auth"
)

func TestClient_ExchangeCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth2/token" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			t.Errorf("expected POST method, got %s", r.Method)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("failed to parse form: %v", err)
		}
		if r.Form.Get("code") != "valid_code" {
			http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"access_token": "mock_access_token",
			"refresh_token": "mock_refresh_token",
			"expires_in": 3600,
			"scope": ["clips:edit", "user:read:chat"],
			"token_type": "bearer"
		}`))
	}))
	defer server.Close()

	cfg := auth.OAuthConfig{
		ClientID:     "test_client_id",
		ClientSecret: "test_secret",
		RedirectURI:  "http://localhost:13337/callback",
		Scopes:       []string{"clips:edit", "user:read:chat"},
	}

	client := auth.NewClient(cfg, nil)
	client.SetBaseURL(server.URL)

	tok, err := client.ExchangeCode(context.Background(), "valid_code")
	if err != nil {
		t.Fatalf("unexpected error exchanging code: %v", err)
	}

	if tok.AccessToken != "mock_access_token" {
		t.Errorf("got AccessToken %q, want 'mock_access_token'", tok.AccessToken)
	}
	if tok.RefreshToken != "mock_refresh_token" {
		t.Errorf("got RefreshToken %q, want 'mock_refresh_token'", tok.RefreshToken)
	}
	if !tok.HasScope("clips:edit") {
		t.Errorf("expected token to have scope 'clips:edit'")
	}
}

func TestClient_ValidateToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth2/validate" {
			http.NotFound(w, r)
			return
		}
		authHeader := r.Header.Get("Authorization")
		if authHeader != "OAuth valid_token" {
			http.Error(w, `{"status":401,"message":"invalid access token"}`, http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"client_id": "test_client_id",
			"login": "twitch_streamer",
			"scopes": ["clips:edit"],
			"user_id": "12345678",
			"expires_in": 3500
		}`))
	}))
	defer server.Close()

	client := auth.NewClient(auth.OAuthConfig{}, nil)
	client.SetBaseURL(server.URL)

	val, err := client.ValidateToken(context.Background(), "valid_token")
	if err != nil {
		t.Fatalf("unexpected error validating token: %v", err)
	}

	if val.Login != "twitch_streamer" {
		t.Errorf("got login %q, want 'twitch_streamer'", val.Login)
	}
	if val.UserID != "12345678" {
		t.Errorf("got user_id %q, want '12345678'", val.UserID)
	}
}
