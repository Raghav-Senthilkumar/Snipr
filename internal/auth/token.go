package auth

import (
	"strings"
	"time"
)

// Twitch OAuth2 Token Pair
type Token struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	TokenType    string    `json:"token_type"`
	ExpiresIn    int       `json:"expires_in"`
	ExpiresAt    time.Time `json:"expires_at"`
	Scopes       []string  `json:"scopes"`
}

// Token Validation response from Twitch's /oauth2/validate endpoint
type TokenValidation struct {
	ClientID  string   `json:"client_id"`
	Login     string   `json:"login"`
	UserID    string   `json:"user_id"`
	ExpiresIn int      `json:"expires_in"`
	Scopes    []string `json:"scopes"`
}

// IsExpired returns true if token is already expired or will expire in 60 seconds
func (t *Token) IsExpired() bool {
	if t == nil || t.AccessToken == "" {
		return true
	}
	// Include Buffer to prevent race conditions
	return time.Now().Add(60 * time.Second).After(t.ExpiresAt)
}

// Checks if token has been granted a specific permission scope
func (t *Token) HasScope(scope string) bool {
	if t == nil {
		return false
	}
	for _, s := range t.Scopes {
		if strings.EqualFold(s, scope) {
			return true
		}
	}
	return false
}

// OAuthConfig for Twitch Application Credentials
type OAuthConfig struct {
	ClientID     string
	ClientSecret string
	RedirectURI  string
	Scopes       []string
}
