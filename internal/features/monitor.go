package features

import (
	"context"
	"sync"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/bus"
	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

const (
	// BatchInterval is the ValSparks hype feature window (24 seconds).
	BatchInterval = 24 * time.Second
	// PollInterval matches ValSparks Kafka POLL_TIMEOUT (500 ms).
	PollInterval = 500 * time.Millisecond
	// ConsumerGroup is the durable NATS name for the features consumer.
	ConsumerGroup = "twitch_feature_group"
)

// Monitor buffers chat per channel and publishes one FeatureVector per non-empty 24s window.
type Monitor struct {
	bus *bus.NATSBus
	mu  sync.Mutex
	// per-channel state
	buffers    map[string][]BufferedMessage
	batchStart map[string]time.Time
}

// NewMonitor creates a ValSparks-style feature window monitor.
func NewMonitor(eventBus *bus.NATSBus) *Monitor {
	return &Monitor{
		bus:        eventBus,
		buffers:    make(map[string][]BufferedMessage),
		batchStart: make(map[string]time.Time),
	}
}

// OnChat appends a normalized message into the channel's current 24s buffer.
func (m *Monitor) OnChat(msg models.ChatMessage) {
	now := msg.Timestamp
	if now.IsZero() {
		now = time.Now().UTC()
	}

	norm := SimpleNormalize(msg.Content)
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.batchStart[msg.Channel]; !ok {
		m.batchStart[msg.Channel] = now
	}
	m.buffers[msg.Channel] = append(m.buffers[msg.Channel], BufferedMessage{
		UserName: msg.UserName,
		NormText: norm,
		RawLen:   len(norm),
	})
}

// StartPollLoop ticks every 500ms and flushes windows that have reached 24s
// (mirrors HypeMonitorConsumer poll + BATCH_INTERVAL behavior).
func (m *Monitor) StartPollLoop(ctx context.Context) {
	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			m.flushReady(ctx, t.UTC())
		}
	}
}

func (m *Monitor) flushReady(ctx context.Context, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for channel, start := range m.batchStart {
		if now.Sub(start) < BatchInterval {
			continue
		}

		buf := m.buffers[channel]
		// Reset window even if empty (ValSparks: empty windows produce nothing).
		m.buffers[channel] = nil
		m.batchStart[channel] = now

		if len(buf) == 0 {
			continue
		}

		fv := ComputeWindowFeatures(channel, start, now, buf)
		_ = m.bus.PublishFeatures(ctx, fv)
	}
}
