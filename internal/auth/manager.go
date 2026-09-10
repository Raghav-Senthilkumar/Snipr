package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"
)

// StoredCredentials is the schema persisted in token.json.
type StoredCredentials struct {
	ClientID     string    `json:"client_id"`
	ClientSecret string    `json:"client_secret,omitempty"`
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	TokenType    string    `json:"token_type,omitempty"`
	ExpiresAt    time.Time `json:"expires_at,omitempty"`
	Username     string    `json:"username,omitempty"`
	UserID       string    `json:"user_id,omitempty"`
}

// ManagerConfig holds configuration for the automated TokenManager.
type ManagerConfig struct {
	ClientID     string
	ClientSecret string
	TokenFile    string   // path to token.json (default: "token.json")
	CallbackPort int      // local HTTP callback port (default: 13337)
	Scopes       []string // scopes to request (default: clips:edit, user:read:chat, chat:read)
}

// TokenManager ensures a fresh, validated Twitch user token is always available.
type TokenManager struct {
	cfg      ManagerConfig
	client   *Client
	mu       sync.Mutex
	token    *Token
	username string
	userID   string
}

// NewTokenManager creates a TokenManager.
func NewTokenManager(cfg ManagerConfig, httpClient *Client) *TokenManager {
	if cfg.TokenFile == "" {
		cfg.TokenFile = "token.json"
	}
	if cfg.CallbackPort <= 0 {
		cfg.CallbackPort = 13337
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{
			"clips:edit",
			"user:read:chat",
			"chat:read",
		}
	}

	if cfg.ClientID == "" {
		cfg.ClientID = os.Getenv("TWITCH_CLIENT_ID")
	}
	if cfg.ClientSecret == "" {
		cfg.ClientSecret = os.Getenv("TWITCH_CLIENT_SECRET")
	}

	tm := &TokenManager{
		cfg: cfg,
	}

	// Try loading initial token from disk or env
	_ = tm.loadFromDisk()

	redirectURI := fmt.Sprintf("http://localhost:%d/callback", tm.cfg.CallbackPort)
	oauthCfg := OAuthConfig{
		ClientID:     tm.cfg.ClientID,
		ClientSecret: tm.cfg.ClientSecret,
		RedirectURI:  redirectURI,
		Scopes:       tm.cfg.Scopes,
	}

	if httpClient != nil {
		tm.client = httpClient
		existing := httpClient.GetConfig()
		if existing.ClientID == "" {
			existing.ClientID = tm.cfg.ClientID
		}
		if existing.ClientSecret == "" {
			existing.ClientSecret = tm.cfg.ClientSecret
		}
		httpClient.SetConfig(existing)
	} else {
		tm.client = NewClient(oauthCfg, nil)
	}

	return tm
}

// GetClientID returns the current ClientID.
func (tm *TokenManager) GetClientID() string {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.cfg.ClientID
}

// IsConfigured returns true if ClientID and either a token or ClientSecret exist.
func (tm *TokenManager) IsConfigured() bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.cfg.ClientID != "" && (tm.token != nil || tm.cfg.ClientSecret != "")
}

// GetValidAccessToken returns a valid token string, auto-refreshing or re-authenticating if needed.
func (tm *TokenManager) GetValidAccessToken(ctx context.Context) (string, error) {
	tok, err := tm.EnsureValidToken(ctx)
	if err != nil {
		return "", err
	}
	return tok.AccessToken, nil
}

// EnsureValidToken checks token health, refreshes if expired, or triggers login if missing.
func (tm *TokenManager) EnsureValidToken(ctx context.Context) (*Token, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// 1. If we have a token, check if it's still valid with Twitch
	if tm.token != nil && tm.token.AccessToken != "" {
		val, err := tm.client.ValidateToken(ctx, tm.token.AccessToken)
		if err == nil {
			// Token is currently valid!
			tm.username = val.Login
			tm.userID = val.UserID

			// If it has less than 5 minutes left, proactively refresh in background
			if val.ExpiresIn < 300 && tm.token.RefreshToken != "" && tm.cfg.ClientSecret != "" {
				slog.Info("token expiring soon, proactively refreshing", "expires_in", val.ExpiresIn)
				if refreshed, err := tm.client.RefreshToken(ctx, tm.token.RefreshToken); err == nil {
					tm.token = refreshed
					_ = tm.saveToDisk()
				}
			}
			return tm.token, nil
		}

		slog.Warn("existing token failed validation, attempting refresh...", "error", err)

		// 2. Token failed validation (expired/revoked), attempt refresh using RefreshToken
		if tm.token.RefreshToken != "" && tm.cfg.ClientSecret != "" {
			refreshed, err := tm.client.RefreshToken(ctx, tm.token.RefreshToken)
			if err == nil {
				slog.Info("token successfully refreshed via twitch refresh_token")
				tm.token = refreshed

				// Validate the new token to get user info
				if v, err := tm.client.ValidateToken(ctx, refreshed.AccessToken); err == nil {
					tm.username = v.Login
					tm.userID = v.UserID
				}
				_ = tm.saveToDisk()
				return tm.token, nil
			}
			slog.Warn("token refresh failed", "error", err)
		}
	}

	// 3. No token exists or refresh failed: initiate browser login flow
	slog.Info("no valid twitch token found; initiating browser login flow...")
	tok, err := tm.loginViaBrowser(ctx)
	if err != nil {
		return nil, fmt.Errorf("browser login flow failed: %w", err)
	}

	tm.token = tok
	_ = tm.saveToDisk()

	return tm.token, nil
}

// GetUser returns the validated username and user ID.
func (tm *TokenManager) GetUser() (username, userID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.username, tm.userID
}

// loginViaBrowser starts the local callback server and opens the browser to authenticate.
func (tm *TokenManager) loginViaBrowser(ctx context.Context) (*Token, error) {
	if tm.cfg.ClientID == "" || tm.cfg.ClientSecret == "" {
		return nil, errors.New("cannot initiate browser login: TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET are required")
	}

	state, err := GenerateRandomState()
	if err != nil {
		return nil, fmt.Errorf("failed to generate state: %w", err)
	}

	authURL := tm.client.GetAuthURL(state)

	server := NewCallbackServer(tm.cfg.CallbackPort, state)
	if err := server.Start(); err != nil {
		return nil, fmt.Errorf("failed to start local callback listener on port %d: %w", tm.cfg.CallbackPort, err)
	}

	fmt.Println("\n========================================================")
	fmt.Println("🔑 TWITCH LOGIN REQUIRED FOR CLIPPING")
	fmt.Println("========================================================")
	fmt.Println("Opening browser for Twitch Authorization...")
	fmt.Printf("If browser does not open automatically, visit:\n%s\n", authURL)
	fmt.Println("========================================================")

	_ = openBrowser(authURL)

	code, err := server.WaitForCode(ctx)
	if err != nil {
		return nil, fmt.Errorf("timed out or failed waiting for browser authorization: %w", err)
	}

	slog.Info("authorization code received, exchanging for tokens...")
	tok, err := tm.client.ExchangeCode(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange code for tokens: %w", err)
	}

	// Validate to fetch user login and ID
	val, err := tm.client.ValidateToken(ctx, tok.AccessToken)
	if err == nil {
		tm.username = val.Login
		tm.userID = val.UserID
	}

	fmt.Printf("🎉 Successfully logged in as: %s!\n\n", tm.username)
	return tok, nil
}

func (tm *TokenManager) loadFromDisk() error {
	data, err := os.ReadFile(tm.cfg.TokenFile)
	if err != nil {
		return err
	}

	var stored StoredCredentials
	if err := json.Unmarshal(data, &stored); err != nil {
		return err
	}

	if tm.cfg.ClientID == "" && stored.ClientID != "" {
		tm.cfg.ClientID = stored.ClientID
	}
	if tm.cfg.ClientSecret == "" && stored.ClientSecret != "" {
		tm.cfg.ClientSecret = stored.ClientSecret
	}

	tm.username = stored.Username
	tm.userID = stored.UserID

	tm.token = &Token{
		AccessToken:  stored.AccessToken,
		RefreshToken: stored.RefreshToken,
		TokenType:    stored.TokenType,
		ExpiresAt:    stored.ExpiresAt,
	}

	return nil
}

func (tm *TokenManager) saveToDisk() error {
	if tm.token == nil {
		return nil
	}

	stored := StoredCredentials{
		ClientID:     tm.cfg.ClientID,
		ClientSecret: tm.cfg.ClientSecret,
		AccessToken:  tm.token.AccessToken,
		RefreshToken: tm.token.RefreshToken,
		TokenType:    tm.token.TokenType,
		ExpiresAt:    tm.token.ExpiresAt,
		Username:     tm.username,
		UserID:       tm.userID,
	}

	data, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(tm.cfg.TokenFile, data, 0600)
}

func openBrowser(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "darwin":
		cmd = "open"
		args = []string{url}
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start", url}
	default:
		cmd = "xdg-open"
		args = []string{url}
	}
	return exec.Command(cmd, args...).Start()
}
