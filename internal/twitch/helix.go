package twitch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	DefaultHelixURL = "https://api.twitch.tv/helix"
)

// ClipResponse is returned immediately when Twitch schedules a clip creation.
type ClipResponse struct {
	ID      string `json:"id"`
	EditURL string `json:"edit_url"`
}

// ClipDetails contains full metadata for a finalized clip.
type ClipDetails struct {
	ID              string  `json:"id"`
	URL             string  `json:"url"`
	EmbedURL        string  `json:"embed_url"`
	BroadcasterID   string  `json:"broadcaster_id"`
	BroadcasterName string  `json:"broadcaster_name"`
	CreatorID       string  `json:"creator_id"`
	CreatorName     string  `json:"creator_name"`
	Title           string  `json:"title"`
	ViewCount       int     `json:"view_count"`
	CreatedAt       string  `json:"created_at"`
	ThumbnailURL    string  `json:"thumbnail_url"`
	Duration        float64 `json:"duration"`
}

// TokenProvider dynamically provides a valid access token, auto-refreshing if needed.
type TokenProvider func(ctx context.Context) (string, error)

// HelixClient communicates with Twitch's Helix REST API.
type HelixClient struct {
	clientID      string
	userToken     string
	tokenProvider TokenProvider
	baseURL       string
	httpClient    *http.Client
	cacheMu       sync.RWMutex
	userCache     map[string]string // login -> user_id
}

// NewHelixClient creates a new client for Twitch Helix.
func NewHelixClient(clientID, userToken string, httpClient *http.Client) *HelixClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	cleanToken := strings.TrimPrefix(userToken, "oauth:")
	cleanToken = strings.TrimPrefix(cleanToken, "Bearer ")

	return &HelixClient{
		clientID:   clientID,
		userToken:  cleanToken,
		baseURL:    DefaultHelixURL,
		httpClient: httpClient,
		userCache:  make(map[string]string),
	}
}

// SetTokenProvider sets a callback that dynamically returns a fresh access token before API calls.
func (c *HelixClient) SetTokenProvider(provider TokenProvider) {
	c.tokenProvider = provider
}

// SetBaseURL overrides the base URL (useful for tests with httptest).
func (c *HelixClient) SetBaseURL(url string) {
	c.baseURL = strings.TrimRight(url, "/")
}

// IsConfigured returns true if the client has credentials to make Helix calls.
func (c *HelixClient) IsConfigured() bool {
	return c.clientID != "" && (c.userToken != "" || c.tokenProvider != nil)
}

// GetUserID looks up the numerical broadcaster ID for a given channel login.
func (c *HelixClient) GetUserID(ctx context.Context, login string) (string, error) {
	cleanLogin := strings.ToLower(strings.TrimPrefix(login, "#"))

	// 1. Check in-memory cache first
	c.cacheMu.RLock()
	if id, ok := c.userCache[cleanLogin]; ok {
		c.cacheMu.RUnlock()
		return id, nil
	}
	c.cacheMu.RUnlock()

	// 2. Call GET /helix/users?login=<channel>
	reqURL := fmt.Sprintf("%s/users?login=%s", c.baseURL, url.QueryEscape(cleanLogin))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create user lookup request: %w", err)
	}

	if err := c.setHeaders(ctx, req); err != nil {
		return "", fmt.Errorf("failed to prepare auth headers: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("user lookup request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read user lookup body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("user lookup returned status %d: %s", resp.StatusCode, string(body))
	}

	type userResponse struct {
		Data []struct {
			ID    string `json:"id"`
			Login string `json:"login"`
		} `json:"data"`
	}

	var res userResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return "", fmt.Errorf("failed to parse user response: %w", err)
	}

	if len(res.Data) == 0 {
		return "", fmt.Errorf("streamer %q not found on twitch", cleanLogin)
	}

	userID := res.Data[0].ID

	// Cache the result
	c.cacheMu.Lock()
	c.userCache[cleanLogin] = userID
	c.cacheMu.Unlock()

	return userID, nil
}

// CreateClip calls POST /helix/clips?broadcaster_id=<id> to generate a clip.
func (c *HelixClient) CreateClip(ctx context.Context, broadcasterID string) (*ClipResponse, error) {
	if !c.IsConfigured() {
		return nil, errors.New("helix client is missing client_id or user OAuth token")
	}

	reqURL := fmt.Sprintf("%s/clips?broadcaster_id=%s&has_delay=false", c.baseURL, url.QueryEscape(broadcasterID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create clip request: %w", err)
	}

	if err := c.setHeaders(ctx, req); err != nil {
		return nil, fmt.Errorf("failed to prepare auth headers: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create clip request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read create clip body: %w", err)
	}

	// 202 Accepted is Twitch's success code for clip creation
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("create clip failed (status %d): %s", resp.StatusCode, string(body))
	}

	type createResponse struct {
		Data []ClipResponse `json:"data"`
	}

	var res createResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("failed to parse clip response: %w", err)
	}

	if len(res.Data) == 0 {
		return nil, errors.New("twitch did not return any clip data")
	}

	return &res.Data[0], nil
}

// PollClipStatus polls GET /helix/clips?id=<clip_id> until Twitch finishes processing the video.
func (c *HelixClient) PollClipStatus(ctx context.Context, clipID string, maxAttempts int, interval time.Duration) (*ClipDetails, error) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if maxAttempts <= 0 {
		maxAttempts = 5
	}

	reqURL := fmt.Sprintf("%s/clips?id=%s", c.baseURL, url.QueryEscape(clipID))

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		if err := c.setHeaders(ctx, req); err != nil {
			continue
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			continue
		}

		type clipListResponse struct {
			Data []ClipDetails `json:"data"`
		}

		var list clipListResponse
		if err := json.Unmarshal(body, &list); err == nil && len(list.Data) > 0 {
			clip := list.Data[0]
			if clip.URL != "" {
				return &clip, nil
			}
		}
	}

	// Fallback if transcoding takes longer: construct standard Twitch clip URL
	return &ClipDetails{
		ID:  clipID,
		URL: fmt.Sprintf("https://clips.twitch.tv/%s", clipID),
	}, nil
}

func (c *HelixClient) setHeaders(ctx context.Context, req *http.Request) error {
	token, err := c.getToken(ctx)
	if err != nil {
		return err
	}
	req.Header.Set("Client-Id", c.clientID)
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

func (c *HelixClient) getToken(ctx context.Context) (string, error) {
	if c.tokenProvider != nil {
		tok, err := c.tokenProvider(ctx)
		if err != nil {
			return "", err
		}
		clean := strings.TrimPrefix(tok, "oauth:")
		clean = strings.TrimPrefix(clean, "Bearer ")
		c.userToken = clean
		return clean, nil
	}
	if c.userToken == "" {
		return "", errors.New("helix client is missing user OAuth token")
	}
	return c.userToken, nil
}
