package ingest

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/bus"
	"github.com/gorilla/websocket"
)

const DefaultTwitchIRCWS = "wss://irc-ws.chat.twitch.tv:443"

// ClientOptions configures the Twitch IRC WebSocket client.
type ClientOptions struct {
	OAuthToken string // Optional: if empty, connects as anonymous (justinfan)
	Username   string // Optional: required if OAuthToken is provided
	IRCURL     string // Optional: defaults to wss://irc-ws.chat.twitch.tv:443
}

// Client manages the live WebSocket connection to Twitch IRC.
type Client struct {
	bus      *bus.NATSBus
	options  ClientOptions
	conn     *websocket.Conn
	writeMu  sync.Mutex
	mu       sync.RWMutex
	channels map[string]bool
	stopCh   chan struct{}
	closed   bool
}

// NewClient creates a new Twitch IRC client connected to the event bus.
func NewClient(eventBus *bus.NATSBus, opts ClientOptions) *Client {
	if opts.IRCURL == "" {
		opts.IRCURL = DefaultTwitchIRCWS
	}
	return &Client{
		bus:      eventBus,
		options:  opts,
		channels: make(map[string]bool),
		stopCh:   make(chan struct{}),
	}
}

// Connect opens the WebSocket connection to Twitch and authenticates.
func (c *Client) Connect(ctx context.Context) error {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, _, err := dialer.DialContext(ctx, c.options.IRCURL, nil)
	if err != nil {
		return fmt.Errorf("failed to dial twitch irc websocket: %w", err)
	}
	c.conn = conn

	// 1. Request Twitch capabilities (tags and commands)
	if err := c.writeLine("CAP REQ :twitch.tv/tags twitch.tv/commands"); err != nil {
		c.conn.Close()
		return fmt.Errorf("failed to request capabilities: %w", err)
	}

	// 2. Authenticate (OAuth or Anonymous)
	if c.options.OAuthToken != "" && c.options.Username != "" {
		token := strings.TrimPrefix(c.options.OAuthToken, "oauth:")
		if err := c.writeLine(fmt.Sprintf("PASS oauth:%s", token)); err != nil {
			c.conn.Close()
			return err
		}
		if err := c.writeLine(fmt.Sprintf("NICK %s", strings.ToLower(c.options.Username))); err != nil {
			c.conn.Close()
			return err
		}
		slog.Info("connected to twitch irc as authenticated user", "user", c.options.Username)
	} else {
		// Anonymous connection via justinfan
		randNum, _ := rand.Int(rand.Reader, big.NewInt(89999))
		anonNick := fmt.Sprintf("justinfan%d", 10000+randNum.Int64())
		if err := c.writeLine("PASS SCHMOOPIIE"); err != nil {
			c.conn.Close()
			return err
		}
		if err := c.writeLine(fmt.Sprintf("NICK %s", anonNick)); err != nil {
			c.conn.Close()
			return err
		}
		slog.Info("connected to twitch irc as anonymous guest", "nick", anonNick)
	}

	return nil
}

// Start runs the message read loop until the context is cancelled or Close() is called.
func (c *Client) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			c.mu.RLock()
			isClosed := c.closed
			c.mu.RUnlock()
			if isClosed {
				return
			}
			slog.Warn("twitch irc read error", "error", err)
			return
		}

		rawLines := string(payload)
		lines := strings.Split(rawLines, "\r\n")

		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}

			result, err := ParseIRCLine(line)
			if err != nil {
				continue
			}

			// Respond to keepalive PING immediately
			if result.IsPing {
				pongMsg := fmt.Sprintf("PONG :%s", result.PingPayload)
				if err := c.writeLine(pongMsg); err != nil {
					slog.Warn("failed to send PONG", "error", err)
				}
				continue
			}

			// If a valid chat message was parsed, publish onto NATS JetStream
			if result.Message != nil && c.bus != nil {
				if err := c.bus.PublishChat(ctx, *result.Message); err != nil {
					slog.Warn("failed to publish chat message to bus", "error", err, "channel", result.Message.Channel)
				}
			}
		}
	}
}

// Join joins a Twitch channel chat room dynamically while running.
func (c *Client) Join(channel string) error {
	ch := strings.ToLower(strings.TrimPrefix(channel, "#"))
	if ch == "" {
		return fmt.Errorf("invalid channel name")
	}

	if err := c.writeLine(fmt.Sprintf("JOIN #%s", ch)); err != nil {
		return fmt.Errorf("failed to join #%s: %w", ch, err)
	}

	c.mu.Lock()
	c.channels[ch] = true
	c.mu.Unlock()

	slog.Info("joined twitch channel chat", "channel", ch)
	return nil
}

// Part leaves a Twitch channel chat room.
func (c *Client) Part(channel string) error {
	ch := strings.ToLower(strings.TrimPrefix(channel, "#"))
	if ch == "" {
		return fmt.Errorf("invalid channel name")
	}

	if err := c.writeLine(fmt.Sprintf("PART #%s", ch)); err != nil {
		return fmt.Errorf("failed to part #%s: %w", ch, err)
	}

	c.mu.Lock()
	delete(c.channels, ch)
	c.mu.Unlock()

	slog.Info("left twitch channel chat", "channel", ch)
	return nil
}

// GetChannels returns the list of currently joined channels.
func (c *Client) GetChannels() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	list := make([]string, 0, len(c.channels))
	for ch := range c.channels {
		list = append(list, ch)
	}
	return list
}

// Close shuts down the WebSocket connection cleanly.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.stopCh)
	c.mu.Unlock()

	if c.conn != nil {
		// Send normal closure frame
		_ = c.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"),
			time.Now().Add(time.Second),
		)
		return c.conn.Close()
	}
	return nil
}

func (c *Client) writeLine(line string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("websocket connection is nil")
	}

	return c.conn.WriteMessage(websocket.TextMessage, []byte(line+"\r\n"))
}
