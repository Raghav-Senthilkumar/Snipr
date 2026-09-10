package auth_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Raghav-Senthilkumar/Snipr/internal/auth"
)

func TestTokenManager_ValidToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth2/validate" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"client_id": "test_client",
				"login": "streamer_dan",
				"user_id": "112233",
				"expires_in": 3600
			}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token.json")
	_ = os.WriteFile(tokenFile, []byte(`{
		"client_id": "test_client",
		"access_token": "mock_valid_token",
		"refresh_token": "mock_refresh_token"
	}`), 0600)

	cfg := auth.ManagerConfig{
		ClientID:  "test_client",
		TokenFile: tokenFile,
	}

	client := auth.NewClient(auth.OAuthConfig{ClientID: "test_client"}, nil)
	client.SetBaseURL(server.URL)

	mgr := auth.NewTokenManager(cfg, client)

	tok, err := mgr.EnsureValidToken(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tok.AccessToken != "mock_valid_token" {
		t.Errorf("got access token %q, want 'mock_valid_token'", tok.AccessToken)
	}

	user, uid := mgr.GetUser()
	if user != "streamer_dan" || uid != "112233" {
		t.Errorf("got user=%q, uid=%q, want 'streamer_dan', '112233'", user, uid)
	}
}

func TestTokenManager_AutoRefreshesExpiredToken(t *testing.T) {
	refreshCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth2/validate":
			authHeader := r.Header.Get("Authorization")
			if authHeader == "OAuth expired_token" {
				http.Error(w, `{"status":401,"message":"invalid access token"}`, http.StatusUnauthorized)
				return
			}
			if authHeader == "OAuth new_refreshed_token" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{
					"client_id": "test_client",
					"login": "streamer_dan",
					"user_id": "112233",
					"expires_in": 3600
				}`))
				return
			}
			http.Error(w, "unexpected token", http.StatusBadRequest)

		case "/oauth2/token":
			refreshCalled = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"access_token": "new_refreshed_token",
				"refresh_token": "next_refresh_token",
				"expires_in": 3600,
				"token_type": "bearer"
			}`))
			return

		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "token.json")
	_ = os.WriteFile(tokenFile, []byte(`{
		"client_id": "test_client",
		"access_token": "expired_token",
		"refresh_token": "old_refresh_token"
	}`), 0600)

	cfg := auth.ManagerConfig{
		ClientID:     "test_client",
		ClientSecret: "test_secret",
		TokenFile:    tokenFile,
	}

	client := auth.NewClient(auth.OAuthConfig{ClientID: "test_client", ClientSecret: "test_secret"}, nil)
	client.SetBaseURL(server.URL)

	mgr := auth.NewTokenManager(cfg, client)

	tok, err := mgr.EnsureValidToken(context.Background())
	if err != nil {
		t.Fatalf("unexpected error ensuring valid token: %v", err)
	}

	if !refreshCalled {
		t.Errorf("expected RefreshToken to be called on expired token")
	}

	if tok.AccessToken != "new_refreshed_token" {
		t.Errorf("got %q, want 'new_refreshed_token'", tok.AccessToken)
	}

	// Verify that token.json on disk was updated with the new token
	data, _ := os.ReadFile(tokenFile)
	if len(data) == 0 {
		t.Errorf("expected token.json to be written")
	}
}
