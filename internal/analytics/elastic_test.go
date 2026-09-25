package analytics

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

// roundTripFunc allows using a simple function as an http.RoundTripper.
type roundTripFunc func(req *http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestNewElasticIndexer(t *testing.T) {
	cfg := Config{}
	indexer, err := NewElasticIndexer(cfg, nil)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	defer indexer.Close()

	if indexer.cfg.URL != DefaultElasticURL {
		t.Errorf("expected URL %s, got %s", DefaultElasticURL, indexer.cfg.URL)
	}
	if indexer.cfg.ChatIndex != DefaultChatIndex {
		t.Errorf("expected ChatIndex %s, got %s", DefaultChatIndex, indexer.cfg.ChatIndex)
	}
	if indexer.cfg.ClipsIndex != DefaultClipsIndex {
		t.Errorf("expected ClipsIndex %s, got %s", DefaultClipsIndex, indexer.cfg.ClipsIndex)
	}
	if indexer.cfg.BatchSize != DefaultBatchSize {
		t.Errorf("expected BatchSize %d, got %d", DefaultBatchSize, indexer.cfg.BatchSize)
	}
}

func TestElasticIndexer_Ping(t *testing.T) {
	t.Run("successful ping", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || (req.URL.Path != "/" && req.URL.Path != "") {
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Body:       io.NopCloser(strings.NewReader("not found")),
						Header:     make(http.Header),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"version":{"number":"8.13.4"}}`)),
					Header:     make(http.Header),
				}, nil
			}),
		}

		cfg := Config{URL: "http://elastic.mock"}
		indexer, err := NewElasticIndexer(cfg, mockClient)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer indexer.Close()

		if err := indexer.Ping(context.Background()); err != nil {
			t.Errorf("expected ping to succeed, got %v", err)
		}
	})

	t.Run("failed ping on error status", func(t *testing.T) {
		mockClient := &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Body:       io.NopCloser(strings.NewReader("node not ready")),
					Header:     make(http.Header),
				}, nil
			}),
		}

		cfg := Config{URL: "http://elastic.mock"}
		indexer, err := NewElasticIndexer(cfg, mockClient)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer indexer.Close()

		if err := indexer.Ping(context.Background()); err == nil {
			t.Errorf("expected ping error on 503 status, got nil")
		}
	})
}

func TestElasticIndexer_InitIndices(t *testing.T) {
	var createdIndices []string
	var mu sync.Mutex

	mockClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			mu.Lock()
			defer mu.Unlock()

			idx := strings.TrimPrefix(req.URL.Path, "/")
			switch req.Method {
			case http.MethodHead:
				// Pretend snipr-chat exists, snipr-clips does not
				if idx == "snipr-chat" {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(bytes.NewReader(nil)),
						Header:     make(http.Header),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Header:     make(http.Header),
				}, nil
			case http.MethodPut:
				createdIndices = append(createdIndices, idx)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"acknowledged":true}`)),
					Header:     make(http.Header),
				}, nil
			default:
				return &http.Response{
					StatusCode: http.StatusMethodNotAllowed,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Header:     make(http.Header),
				}, nil
			}
		}),
	}

	cfg := Config{
		URL:        "http://elastic.mock",
		ChatIndex:  "snipr-chat",
		ClipsIndex: "snipr-clips",
	}
	indexer, err := NewElasticIndexer(cfg, mockClient)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer indexer.Close()

	if err := indexer.InitIndices(context.Background()); err != nil {
		t.Fatalf("expected InitIndices to succeed, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(createdIndices) != 1 || createdIndices[0] != "snipr-clips" {
		t.Errorf("expected only snipr-clips to be created, got %v", createdIndices)
	}
}

func TestElasticIndexer_BulkBatchAndFlush(t *testing.T) {
	var receivedPayloads []string
	var mu sync.Mutex

	mockClient := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/_bulk" && req.Method == http.MethodPost {
				body, _ := io.ReadAll(req.Body)
				mu.Lock()
				receivedPayloads = append(receivedPayloads, string(body))
				mu.Unlock()
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"took":5,"errors":false,"items":[]}`)),
					Header:     make(http.Header),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewReader(nil)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	cfg := Config{
		URL:           "http://elastic.mock",
		ChatIndex:     "test-chat",
		ClipsIndex:    "test-clips",
		BatchSize:     3,
		FlushInterval: 40 * time.Millisecond,
	}

	indexer, err := NewElasticIndexer(cfg, mockClient)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Send 2 chat messages and 1 clip (total 3 items = BatchSize)
	indexer.IndexChat(models.ChatMessage{
		Channel:   "shroud",
		UserName:  "viewer1",
		Content:   "POGGERS",
		Emotes:    []string{"POGGERS"},
		Timestamp: time.Now(),
	})
	indexer.IndexChat(models.ChatMessage{
		Channel:   "shroud",
		UserName:  "viewer2",
		Content:   "LUL",
		Emotes:    []string{"LUL"},
		Timestamp: time.Now(),
	})
	indexer.IndexClip(ClipEventDoc{
		Channel:     "shroud",
		ClipID:      "CoolClip123",
		URL:         "https://clips.twitch.tv/CoolClip123",
		Title:       "Shroud INSANE flick",
		Prob:        0.91,
		Prediction:  1,
		MsgCount:    42,
		HypeScore:   5,
		UniqueUsers: 15,
		Reason:      "ml_hype_prediction",
		Timestamp:   time.Now(),
	})

	// Wait briefly for batch to flush
	time.Sleep(100 * time.Millisecond)

	indexer.Close()

	mu.Lock()
	defer mu.Unlock()

	if len(receivedPayloads) == 0 {
		t.Fatal("expected at least 1 bulk request received by test transport")
	}

	allPayloads := strings.Join(receivedPayloads, "")
	if !strings.Contains(allPayloads, `"test-chat"`) {
		t.Errorf("payload missing test-chat index")
	}
	if !strings.Contains(allPayloads, `"test-clips"`) {
		t.Errorf("payload missing test-clips index")
	}
	if !strings.Contains(allPayloads, "POGGERS") {
		t.Errorf("payload missing chat content POGGERS")
	}
	if !strings.Contains(allPayloads, "CoolClip123") {
		t.Errorf("payload missing clip ID CoolClip123")
	}

	stats := indexer.GetStats()
	if stats.ChatIndexed != 2 {
		t.Errorf("expected 2 chats indexed, got %d", stats.ChatIndexed)
	}
	if stats.ClipsIndexed != 1 {
		t.Errorf("expected 1 clip indexed, got %d", stats.ClipsIndexed)
	}
	if stats.BulkRequests == 0 {
		t.Errorf("expected > 0 bulk requests, got %d", stats.BulkRequests)
	}
	if stats.FailedDocs != 0 {
		t.Errorf("expected 0 failed docs, got %d", stats.FailedDocs)
	}
}
