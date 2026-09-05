package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/auth"
)

// openBrowser opens the specified URL in the default browser on macOS, Linux, or Windows.
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

func main() {
	// Configure default structured text logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	clientID := os.Getenv("TWITCH_CLIENT_ID")
	clientSecret := os.Getenv("TWITCH_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		slog.Error("missing required credentials", "error", "TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET environment variables must be set")
		os.Exit(1)
	}

	port := 13337
	redirectURI := fmt.Sprintf("http://localhost:%d/callback", port)

	cfg := auth.OAuthConfig{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURI:  redirectURI,
		Scopes: []string{
			"clips:edit",     // Needed to create clips
			"user:read:chat", // Needed to read chat via EventSub
		},
	}

	client := auth.NewClient(cfg, nil)

	state, err := auth.GenerateRandomState()
	if err != nil {
		slog.Error("failed to generate random state", "error", err)
		os.Exit(1)
	}

	authURL := client.GetAuthURL(state)

	server := auth.NewCallbackServer(port, state)
	if err := server.Start(); err != nil {
		slog.Error("failed to start local callback server", "port", port, "error", err)
		os.Exit(1)
	}

	slog.Info("starting twitch authorization flow", "port", port, "redirect_uri", redirectURI)
	slog.Info("if browser does not open automatically, visit authorization url", "url", authURL)

	if err := openBrowser(authURL); err != nil {
		slog.Warn("could not open browser automatically", "error", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	slog.Info("waiting for user authorization in browser...")
	code, err := server.WaitForCode(ctx)
	if err != nil {
		slog.Error("authorization failed or timed out", "error", err)
		os.Exit(1)
	}

	slog.Info("authorization code received, exchanging for tokens")

	token, err := client.ExchangeCode(ctx, code)
	if err != nil {
		slog.Error("failed to exchange authorization code for tokens", "error", err)
		os.Exit(1)
	}

	slog.Info("tokens acquired successfully",
		"token_type", token.TokenType,
		"expires_in_seconds", token.ExpiresIn,
		"scopes", token.Scopes,
	)

	// Validate the token to verify permissions and fetch user metadata
	validation, err := client.ValidateToken(ctx, token.AccessToken)
	if err != nil {
		slog.Error("failed to validate access token with twitch", "error", err)
		os.Exit(1)
	}

	slog.Info("user authenticated and validated",
		"username", validation.Login,
		"user_id", validation.UserID,
		"client_id", validation.ClientID,
		"scopes", validation.Scopes,
	)
}
