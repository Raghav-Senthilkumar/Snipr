package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Config defines options for the embedded NATS JetStream server and stream.
type Config struct {
	Port       int           // -1 instructs NATS to find any free local port
	StreamName string        // Name of the JetStream stream (e.g., "CHAT_STREAM")
	MaxAge     time.Duration // Time-to-live retention for messages in memory
	MaxBytes   int64         // Maximum bytes allocated to the stream in RAM
}

// NATSBus wraps an embedded NATS server and JetStream client connection.
type NATSBus struct {
	server *server.Server
	nc     *nats.Conn
	js     nats.JetStreamContext
}

// NewNATSBus initializes an embedded NATS server and configures the JetStream chat stream.
func NewNATSBus(cfg Config) (*NATSBus, error) {
	// 1. Configure embedded NATS server options
	opts := &server.Options{
		Port:       cfg.Port,
		JetStream:  true,
		DontListen: false,
		NoLog:      true, // Suppress verbose internal NATS logs during tests
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats server: %w", err)
	}

	// 2. Start server in background
	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, errors.New("embedded nats server failed to become ready")
	}

	// 3. Connect client over in-memory local loopback
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("failed to connect nats client: %w", err)
	}

	// 4. Initialize JetStream context
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("failed to initialize jetstream: %w", err)
	}

	// 5. Create in-memory ring-buffer stream for chat messages
	streamCfg := &nats.StreamConfig{
		Name:       cfg.StreamName,
		Subjects:   []string{"chat.twitch.>"},
		Storage:    nats.MemoryStorage, // Keep in RAM for sub-millisecond latency
		MaxAge:     cfg.MaxAge,
		MaxBytes:   cfg.MaxBytes,
		Discard:    nats.DiscardOld, // Drop oldest messages when buffer fills
		Duplicates: 2 * time.Minute, // Deduplicate Twitch message UUIDs
	}

	if _, err := js.AddStream(streamCfg); err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("failed to create stream %s: %w", cfg.StreamName, err)
	}

	return &NATSBus{
		server: ns,
		nc:     nc,
		js:     js,
	}, nil
}

// PublishChat sends a ChatMessage to the subject: chat.twitch.<channel>
func (b *NATSBus) PublishChat(ctx context.Context, msg models.ChatMessage) error {
	subject := fmt.Sprintf("chat.twitch.%s", msg.Channel)

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal chat message: %w", err)
	}

	// Use Twitch's message ID as Nats-Msg-Id for automatic JetStream deduplication
	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": []string{msg.ID}},
	}

	_, err = b.js.PublishMsg(natsMsg, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("failed to publish to %s: %w", subject, err)
	}

	return nil
}

// SubscribeChat registers a handler for messages published to a channel (or all channels using "*").
func (b *NATSBus) SubscribeChat(channel string, handler func(models.ChatMessage)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("chat.twitch.%s", channel)

	sub, err := b.js.Subscribe(subject, func(m *nats.Msg) {
		var chatMsg models.ChatMessage
		if err := json.Unmarshal(m.Data, &chatMsg); err == nil {
			handler(chatMsg)
		}
		_ = m.Ack()
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to %s: %w", subject, err)
	}

	return sub, nil
}

// Close gracefully flushes client messages and shuts down the embedded server.
func (b *NATSBus) Close() {
	if b.nc != nil {
		_ = b.nc.Drain()
		b.nc.Close()
	}
	if b.server != nil {
		b.server.Shutdown()
		b.server.WaitForShutdown()
	}
}
