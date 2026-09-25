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

const (
	DefaultChatStream     = "SNIPR_CHAT"
	DefaultFeaturesStream = "SNIPR_FEATURES"
)

// Config defines options for the embedded NATS JetStream server and streams.
type Config struct {
	Port               int           // -1 = ephemeral free port
	ChatStreamName     string        // JetStream stream for IRC chat
	FeaturesStreamName string        // JetStream stream for 24s feature windows
	MaxAge             time.Duration // TTL retention in memory
	MaxBytes           int64         // Max RAM for each stream
}

// NATSBus wraps an embedded NATS server and JetStream client.
type NATSBus struct {
	server             *server.Server
	nc                 *nats.Conn
	js                 nats.JetStreamContext
	chatStreamName     string
	featuresStreamName string
}

// NewNATSBus starts an embedded NATS server and creates chat + features streams.
func NewNATSBus(cfg Config) (*NATSBus, error) {
	if cfg.ChatStreamName == "" {
		cfg.ChatStreamName = DefaultChatStream
	}
	if cfg.FeaturesStreamName == "" {
		cfg.FeaturesStreamName = DefaultFeaturesStream
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 5 * time.Minute
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 32 * 1024 * 1024
	}

	opts := &server.Options{
		Port:      cfg.Port,
		JetStream: true,
		NoLog:     true,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create nats server: %w", err)
	}
	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		return nil, errors.New("embedded nats server failed to become ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("connect nats client: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("init jetstream: %w", err)
	}

	chatCfg := &nats.StreamConfig{
		Name:       cfg.ChatStreamName,
		Subjects:   []string{"chat.twitch.>"},
		Storage:    nats.MemoryStorage,
		MaxAge:     cfg.MaxAge,
		MaxBytes:   cfg.MaxBytes,
		Discard:    nats.DiscardOld,
		Duplicates: 2 * time.Minute,
	}
	if _, err := js.AddStream(chatCfg); err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("create chat stream: %w", err)
	}

	featCfg := &nats.StreamConfig{
		Name:       cfg.FeaturesStreamName,
		Subjects:   []string{"features.twitch.>"},
		Storage:    nats.MemoryStorage,
		MaxAge:     cfg.MaxAge,
		MaxBytes:   cfg.MaxBytes,
		Discard:    nats.DiscardOld,
		Duplicates: 2 * time.Minute,
	}
	if _, err := js.AddStream(featCfg); err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("create features stream: %w", err)
	}

	return &NATSBus{
		server:             ns,
		nc:                 nc,
		js:                 js,
		chatStreamName:     cfg.ChatStreamName,
		featuresStreamName: cfg.FeaturesStreamName,
	}, nil
}

// PublishChat publishes to chat.twitch.<channel>.
func (b *NATSBus) PublishChat(ctx context.Context, msg models.ChatMessage) error {
	subject := fmt.Sprintf("chat.twitch.%s", msg.Channel)
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal chat: %w", err)
	}

	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": []string{msg.ID}},
	}
	if _, err := b.js.PublishMsg(natsMsg, nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}

// SubscribeChat registers a durable JetStream consumer on chat.twitch.<channel or *>.
// Each durable name is an independent parallel consumer (Kafka consumer-group equivalent).
func (b *NATSBus) SubscribeChat(channel, durable string, handler func(models.ChatMessage)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("chat.twitch.%s", channel)
	opts := []nats.SubOpt{
		nats.BindStream(b.chatStreamName),
		nats.ManualAck(),
		nats.AckExplicit(),
	}
	if durable != "" {
		opts = append(opts, nats.Durable(durable))
	}

	sub, err := b.js.Subscribe(subject, func(m *nats.Msg) {
		var chatMsg models.ChatMessage
		if err := json.Unmarshal(m.Data, &chatMsg); err == nil {
			handler(chatMsg)
		}
		_ = m.Ack()
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("subscribe chat %s (%s): %w", subject, durable, err)
	}
	return sub, nil
}

// PublishFeatures publishes a 24s feature window to features.twitch.<channel>.
func (b *NATSBus) PublishFeatures(ctx context.Context, fv models.FeatureVector) error {
	subject := fmt.Sprintf("features.twitch.%s", fv.Channel)
	data, err := json.Marshal(fv)
	if err != nil {
		return fmt.Errorf("marshal features: %w", err)
	}

	msgID := fmt.Sprintf("%s_%d", fv.Channel, fv.WindowEnd.UnixNano())
	natsMsg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": []string{msgID}},
	}
	if _, err := b.js.PublishMsg(natsMsg, nats.Context(ctx)); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}

// SubscribeFeatures registers a durable consumer on features.twitch.<channel or *>.
func (b *NATSBus) SubscribeFeatures(channel, durable string, handler func(models.FeatureVector)) (*nats.Subscription, error) {
	subject := fmt.Sprintf("features.twitch.%s", channel)
	opts := []nats.SubOpt{
		nats.BindStream(b.featuresStreamName),
		nats.ManualAck(),
		nats.AckExplicit(),
	}
	if durable != "" {
		opts = append(opts, nats.Durable(durable))
	}

	sub, err := b.js.Subscribe(subject, func(m *nats.Msg) {
		var fv models.FeatureVector
		if err := json.Unmarshal(m.Data, &fv); err == nil {
			handler(fv)
		}
		_ = m.Ack()
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("subscribe features %s (%s): %w", subject, durable, err)
	}
	return sub, nil
}

// Close flushes and shuts down the embedded server.
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
