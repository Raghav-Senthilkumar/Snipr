package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultTwitchIDURL = "https://id.twitch.tv"
)

// Client handles Twitch OAuth2 API interactions.
type Client struct {
	config     OAuthConfig
	httpClient *http.Client
	baseURL    string // defaults to https://id.twitch.tv, customizable for tests
}

// NewClient creates a new Twitch OAuth client.
func NewClient(config OAuthConfig, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{
		config:     config,
		httpClient: httpClient,
		baseURL:    DefaultTwitchIDURL,
	}
}

// SetBaseURL overrides the base OAuth URL (primarily used in tests).
func (c *Client) SetBaseURL(url string) {
	c.baseURL = strings.TrimRight(url, "/")
}

// SetConfig updates the OAuth configuration.
func (c *Client) SetConfig(cfg OAuthConfig) {
	c.config = cfg
}

// GetConfig returns the OAuth configuration.
func (c *Client) GetConfig() OAuthConfig {
	return c.config
}

// GetAuthURL builds the Twitch authorization URL to open in the user's browser.
func (c *Client) GetAuthURL(state string) string {
	params := url.Values{}
	params.Set("client_id", c.config.ClientID)
	params.Set("redirect_uri", c.config.RedirectURI)
	params.Set("response_type", "code")
	params.Set("scope", strings.Join(c.config.Scopes, " "))
	params.Set("state", state)

	return fmt.Sprintf("%s/oauth2/authorize?%s", c.baseURL, params.Encode())
}

// ExchangeCode swaps the authorization code received from the redirect for tokens.
func (c *Client) ExchangeCode(ctx context.Context, code string) (*Token, error) {
	data := url.Values{}
	data.Set("client_id", c.config.ClientID)
	data.Set("client_secret", c.config.ClientSecret)
	data.Set("code", code)
	data.Set("grant_type", "authorization_code")
	data.Set("redirect_uri", c.config.RedirectURI)

	return c.requestToken(ctx, data)
}

// RefreshToken exchanges an existing refresh token for a fresh access token.
func (c *Client) RefreshToken(ctx context.Context, refreshToken string) (*Token, error) {
	data := url.Values{}
	data.Set("client_id", c.config.ClientID)
	data.Set("client_secret", c.config.ClientSecret)
	data.Set("grant_type", "refresh_token")
	data.Set("refresh_token", refreshToken)

	return c.requestToken(ctx, data)
}

// ValidateToken verifies that an access token is valid and returns its details.
func (c *Client) ValidateToken(ctx context.Context, accessToken string) (*TokenValidation, error) {
	reqURL := fmt.Sprintf("%s/oauth2/validate", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create validate request: %w", err)
	}

	req.Header.Set("Authorization", "OAuth "+accessToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("validate request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read validate response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token validation failed (status %d): %s", resp.StatusCode, string(body))
	}

	var validation TokenValidation
	if err := json.Unmarshal(body, &validation); err != nil {
		return nil, fmt.Errorf("failed to parse validation response: %w", err)
	}

	return &validation, nil
}

func (c *Client) requestToken(ctx context.Context, formData url.Values) (*Token, error) {
	tokenURL := fmt.Sprintf("%s/oauth2/token", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read token response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token request returned status %d: %s", resp.StatusCode, string(body))
	}

	type rawTokenResponse struct {
		AccessToken  string   `json:"access_token"`
		RefreshToken string   `json:"refresh_token"`
		TokenType    string   `json:"token_type"`
		ExpiresIn    int      `json:"expires_in"`
		Scope        []string `json:"scope"`
	}

	var raw rawTokenResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %w", err)
	}

	return &Token{
		AccessToken:  raw.AccessToken,
		RefreshToken: raw.RefreshToken,
		TokenType:    raw.TokenType,
		ExpiresIn:    raw.ExpiresIn,
		ExpiresAt:    time.Now().Add(time.Duration(raw.ExpiresIn) * time.Second),
		Scopes:       raw.Scope,
	}, nil
}
