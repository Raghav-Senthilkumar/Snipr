package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

const (
	DefaultElasticURL    = "http://localhost:9200"
	DefaultChatIndex     = "snipr-chat"
	DefaultClipsIndex    = "snipr-clips"
	DefaultBatchSize     = 100
	DefaultFlushInterval = 1 * time.Second
	DefaultQueueCapacity = 10000
)

// Config defines connection and buffering parameters for Elasticsearch.
type Config struct {
	URL           string        // Elasticsearch endpoint (e.g., http://localhost:9200)
	ChatIndex     string        // Target index for chat messages (default: snipr-chat)
	ClipsIndex    string        // Target index for auto-clips (default: snipr-clips)
	BatchSize     int           // Number of docs to accumulate before flushing (default: 100)
	FlushInterval time.Duration // Maximum duration before flushing partial batch (default: 1s)
	QueueCapacity int           // In-memory buffer size before dropping under extreme load (default: 10000)
}

// DefaultConfig provides recommended defaults for local and cloud usage.
func DefaultConfig() Config {
	return Config{
		URL:           DefaultElasticURL,
		ChatIndex:     DefaultChatIndex,
		ClipsIndex:    DefaultClipsIndex,
		BatchSize:     DefaultBatchSize,
		FlushInterval: DefaultFlushInterval,
		QueueCapacity: DefaultQueueCapacity,
	}
}

type bulkItem struct {
	index string
	doc   any
}

// Stats holds operational telemetry for the Elasticsearch indexer.
type Stats struct {
	ChatIndexed  uint64
	ClipsIndexed uint64
	FailedDocs   uint64
	BulkRequests uint64
}

// ElasticIndexer manages high-throughput bulk indexing into Elasticsearch.
type ElasticIndexer struct {
	cfg          Config
	client       *http.Client
	queue        chan bulkItem
	stopCh       chan struct{}
	wg           sync.WaitGroup
	chatIndexed  uint64
	clipsIndexed uint64
	failedDocs   uint64
	bulkRequests uint64
	closed       atomic.Bool
}

// NewElasticIndexer creates and starts the asynchronous bulk indexer.
func NewElasticIndexer(cfg Config, httpClient *http.Client) (*ElasticIndexer, error) {
	if cfg.URL == "" {
		cfg.URL = DefaultElasticURL
	}
	cfg.URL = strings.TrimRight(cfg.URL, "/")

	if cfg.ChatIndex == "" {
		cfg.ChatIndex = DefaultChatIndex
	}
	if cfg.ClipsIndex == "" {
		cfg.ClipsIndex = DefaultClipsIndex
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = DefaultQueueCapacity
	}

	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}

	indexer := &ElasticIndexer{
		cfg:    cfg,
		client: httpClient,
		queue:  make(chan bulkItem, cfg.QueueCapacity),
		stopCh: make(chan struct{}),
	}

	// Start background flusher
	indexer.wg.Add(1)
	go indexer.worker()

	return indexer, nil
}

// Ping checks if the Elasticsearch cluster is reachable.
func (e *ElasticIndexer) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.cfg.URL, nil)
	if err != nil {
		return err
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to reach elasticsearch at %s: %w", e.cfg.URL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch ping returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// InitIndices sets up explicit index mappings in Elasticsearch if they don't already exist.
func (e *ElasticIndexer) InitIndices(ctx context.Context) error {
	chatMapping := `{
		"mappings": {
			"properties": {
				"@timestamp": { "type": "date" },
				"event_type": { "type": "keyword" },
				"channel":    { "type": "keyword" },
				"user_id":    { "type": "keyword" },
				"user_name":  { "type": "keyword" },
				"content":    { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
				"emotes":     { "type": "keyword" },
				"bits":       { "type": "integer" },
				"is_hype":    { "type": "boolean" }
			}
		}
	}`

	clipsMapping := `{
		"mappings": {
			"properties": {
				"@timestamp":      { "type": "date" },
				"event_type":      { "type": "keyword" },
				"channel":         { "type": "keyword" },
				"clip_id":         { "type": "keyword" },
				"url":             { "type": "keyword" },
				"edit_url":        { "type": "keyword" },
				"title":           { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
				"z_score":         { "type": "float" },
				"velocity":        { "type": "float" },
				"hype_ratio":      { "type": "float" },
				"reason":          { "type": "keyword" },
				"unique_chatters": { "type": "integer" }
			}
		}
	}`

	if err := e.createIndexIfNotExists(ctx, e.cfg.ChatIndex, chatMapping); err != nil {
		return fmt.Errorf("failed to initialize chat index: %w", err)
	}

	if err := e.createIndexIfNotExists(ctx, e.cfg.ClipsIndex, clipsMapping); err != nil {
		return fmt.Errorf("failed to initialize clips index: %w", err)
	}

	return nil
}

func (e *ElasticIndexer) createIndexIfNotExists(ctx context.Context, indexName, mappingJSON string) error {
	reqURL := fmt.Sprintf("%s/%s", e.cfg.URL, indexName)

	// Check if index exists via HEAD request
	headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, reqURL, nil)
	if err != nil {
		return err
	}

	headResp, err := e.client.Do(headReq)
	if err == nil {
		headResp.Body.Close()
		if headResp.StatusCode == http.StatusOK {
			// Index already exists
			return nil
		}
	}

	// Create index with mapping
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewBufferString(mappingJSON))
	if err != nil {
		return err
	}
	putReq.Header.Set("Content-Type", "application/json")

	putResp, err := e.client.Do(putReq)
	if err != nil {
		return err
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("create index %s failed (%d): %s", indexName, putResp.StatusCode, string(body))
	}

	return nil
}

// IndexChat queues a ChatMessage for high-throughput batch indexing. Non-blocking.
func (e *ElasticIndexer) IndexChat(msg models.ChatMessage) {
	if e.closed.Load() {
		return
	}

	ts := msg.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	doc := ChatMessageDoc{
		Timestamp: ts,
		EventType: "chat",
		Channel:   msg.Channel,
		UserID:    msg.UserID,
		UserName:  msg.UserName,
		Content:   msg.Content,
		Emotes:    msg.Emotes,
		Bits:      msg.Bits,
		IsHype:    len(msg.Emotes) > 0 || msg.Bits > 0,
	}

	item := bulkItem{
		index: e.cfg.ChatIndex,
		doc:   doc,
	}

	select {
	case e.queue <- item:
	default:
		// Queue full under extreme load: drop message and record stat to avoid stalling NATS
		atomic.AddUint64(&e.failedDocs, 1)
		slog.Warn("elasticsearch queue full, dropped chat document")
	}
}

// IndexClip queues a ClipEventDoc for immediate indexing. Non-blocking.
func (e *ElasticIndexer) IndexClip(doc ClipEventDoc) {
	if e.closed.Load() {
		return
	}

	if doc.Timestamp.IsZero() {
		doc.Timestamp = time.Now().UTC()
	}
	doc.EventType = "clip"

	item := bulkItem{
		index: e.cfg.ClipsIndex,
		doc:   doc,
	}

	select {
	case e.queue <- item:
	default:
		atomic.AddUint64(&e.failedDocs, 1)
		slog.Warn("elasticsearch queue full, dropped clip document")
	}
}

// GetStats returns the current indexing throughput and telemetry counters.
func (e *ElasticIndexer) GetStats() Stats {
	return Stats{
		ChatIndexed:  atomic.LoadUint64(&e.chatIndexed),
		ClipsIndexed: atomic.LoadUint64(&e.clipsIndexed),
		FailedDocs:   atomic.LoadUint64(&e.failedDocs),
		BulkRequests: atomic.LoadUint64(&e.bulkRequests),
	}
}

// worker loops accumulating documents and periodically flushes to Elasticsearch.
func (e *ElasticIndexer) worker() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]bulkItem, 0, e.cfg.BatchSize)

	for {
		select {
		case <-e.stopCh:
			// Drain remaining items before stopping
			for {
				select {
				case item := <-e.queue:
					batch = append(batch, item)
					if len(batch) >= e.cfg.BatchSize {
						e.flushBatch(batch)
						batch = batch[:0]
					}
				default:
					if len(batch) > 0 {
						e.flushBatch(batch)
					}
					return
				}
			}

		case item := <-e.queue:
			batch = append(batch, item)
			if len(batch) >= e.cfg.BatchSize {
				e.flushBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				e.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (e *ElasticIndexer) flushBatch(batch []bulkItem) {
	if len(batch) == 0 {
		return
	}

	var buf bytes.Buffer
	chatCount := uint64(0)
	clipCount := uint64(0)

	for _, item := range batch {
		actionHeader := fmt.Sprintf(`{"index":{"_index":"%s"}}`+"\n", item.index)
		buf.WriteString(actionHeader)

		docJSON, err := json.Marshal(item.doc)
		if err != nil {
			atomic.AddUint64(&e.failedDocs, 1)
			continue
		}
		buf.Write(docJSON)
		buf.WriteString("\n")

		if item.index == e.cfg.ChatIndex {
			chatCount++
		} else {
			clipCount++
		}
	}

	if buf.Len() == 0 {
		return
	}

	reqURL := fmt.Sprintf("%s/_bulk", e.cfg.URL)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, reqURL, &buf)
	if err != nil {
		atomic.AddUint64(&e.failedDocs, uint64(len(batch)))
		return
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := e.client.Do(req)
	if err != nil {
		atomic.AddUint64(&e.failedDocs, uint64(len(batch)))
		slog.Warn("elasticsearch _bulk request failed", "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		atomic.AddUint64(&e.chatIndexed, chatCount)
		atomic.AddUint64(&e.clipsIndexed, clipCount)
		atomic.AddUint64(&e.bulkRequests, 1)
	} else {
		body, _ := io.ReadAll(resp.Body)
		atomic.AddUint64(&e.failedDocs, uint64(len(batch)))
		slog.Warn("elasticsearch _bulk returned error status", "status", resp.StatusCode, "body", string(body))
	}
}

// Close gracefully flushes all queued documents and waits for worker shutdown.
func (e *ElasticIndexer) Close() error {
	if e.closed.Swap(true) {
		return nil
	}
	close(e.stopCh)
	e.wg.Wait()
	return nil
}
