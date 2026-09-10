package twitch_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/twitch"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newMockHTTPClient(fn roundTripFunc) *http.Client {
	return &http.Client{
		Transport: fn,
	}
}

func TestHelixClient_GetUserID(t *testing.T) {
	callCount := 0
	mockClient := newMockHTTPClient(func(req *http.Request) (*http.Response, error) {
		callCount++
		if req.Header.Get("Client-Id") != "mock_client_id" {
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       io.NopCloser(bytes.NewBufferString("unauthorized")),
			}, nil
		}
		if req.Header.Get("Authorization") != "Bearer mock_user_token" {
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       io.NopCloser(bytes.NewBufferString("missing token")),
			}, nil
		}
		if req.URL.Query().Get("login") != "tarik" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(`{"data":[]}`)),
			}, nil
		}

		respJSON := `{"data":[{"id":"141981764","login":"tarik"}]}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewBufferString(respJSON)),
		}, nil
	})

	client := twitch.NewHelixClient("mock_client_id", "mock_user_token", mockClient)

	// First call: calls the mock
	id, err := client.GetUserID(context.Background(), "tarik")
	if err != nil {
		t.Fatalf("unexpected error getting user id: %v", err)
	}
	if id != "141981764" {
		t.Errorf("got id %q, want '141981764'", id)
	}

	// Second call: should hit in-memory cache, callCount should stay 1
	id2, err := client.GetUserID(context.Background(), "tarik")
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	if id2 != id {
		t.Errorf("got %q, want %q", id2, id)
	}
	if callCount != 1 {
		t.Errorf("expected 1 API call due to caching, got %d", callCount)
	}
}

func TestHelixClient_CreateClip(t *testing.T) {
	mockClient := newMockHTTPClient(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", req.Method)
		}
		if req.URL.Query().Get("broadcaster_id") != "141981764" {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(bytes.NewBufferString("invalid broadcaster id")),
			}, nil
		}

		respJSON := `{"data":[{"id":"AwesomeClutchClip","edit_url":"https://clips.twitch.tv/AwesomeClutchClip/edit"}]}`
		return &http.Response{
			StatusCode: http.StatusAccepted, // Twitch returns 202 Accepted
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewBufferString(respJSON)),
		}, nil
	})

	client := twitch.NewHelixClient("mock_client_id", "mock_user_token", mockClient)

	res, err := client.CreateClip(context.Background(), "141981764")
	if err != nil {
		t.Fatalf("unexpected error creating clip: %v", err)
	}

	if res.ID != "AwesomeClutchClip" {
		t.Errorf("got clip ID %q, want 'AwesomeClutchClip'", res.ID)
	}
	if res.EditURL != "https://clips.twitch.tv/AwesomeClutchClip/edit" {
		t.Errorf("got edit URL %q", res.EditURL)
	}
}

func TestHelixClient_PollClipStatus(t *testing.T) {
	mockClient := newMockHTTPClient(func(req *http.Request) (*http.Response, error) {
		respJSON := `{
			"data": [{
				"id": "AwesomeClutchClip",
				"url": "https://clips.twitch.tv/AwesomeClutchClip",
				"title": "Tarik Insane 1v4 Clutch",
				"duration": 30.0
			}]
		}`
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewBufferString(respJSON)),
		}, nil
	})

	client := twitch.NewHelixClient("mock_client_id", "mock_user_token", mockClient)

	details, err := client.PollClipStatus(context.Background(), "AwesomeClutchClip", 2, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error polling clip: %v", err)
	}

	if details.URL != "https://clips.twitch.tv/AwesomeClutchClip" {
		t.Errorf("got URL %q, want 'https://clips.twitch.tv/AwesomeClutchClip'", details.URL)
	}
	if details.Title != "Tarik Insane 1v4 Clutch" {
		t.Errorf("got title %q, want 'Tarik Insane 1v4 Clutch'", details.Title)
	}
}
