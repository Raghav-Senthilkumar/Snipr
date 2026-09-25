package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/analytics"
	"github.com/Raghav-Senthilkumar/Snipr/internal/bus"
	"github.com/Raghav-Senthilkumar/Snipr/internal/features"
	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
	"github.com/Raghav-Senthilkumar/Snipr/internal/predict"
	"github.com/Raghav-Senthilkumar/Snipr/internal/store"
	"github.com/Raghav-Senthilkumar/Snipr/internal/twitch"
	"github.com/nats-io/nats.go"
)

// Durable consumer group names (ValSparks Kafka groups → NATS durables).
const (
	SQLiteGroup   = "sqlite_consumer_group"
	FeaturesGroup = features.ConsumerGroup
	ElasticGroup  = "elasticsearch_consumer_group"
	PredictGroup  = "predict_consumer_group"
)

// ClipCooldown matches ValSparks CLIP_COOLDOWN_SEC (30 seconds).
const ClipCooldown = 30 * time.Second

// Consumers holds the parallel NATS subscribers for the message pipeline.
type Consumers struct {
	subs []*nats.Subscription

	Store    *store.SQLiteStore
	Monitor  *features.Monitor
	Elastic  *analytics.ElasticIndexer
	Helix    *twitch.HelixClient
	Predictor predict.Predictor

	elasticOnline bool
	showChat      *atomic.Bool
	messageCount  *uint64

	mu        sync.Mutex
	cooldowns map[string]time.Time
	LastProb  sync.Map // channel -> float64 (for /status)
}

// Options wires dependencies into the parallel consumers.
type Options struct {
	Bus           *bus.NATSBus
	Store         *store.SQLiteStore
	Monitor       *features.Monitor
	Elastic       *analytics.ElasticIndexer
	ElasticOnline bool
	Helix         *twitch.HelixClient
	Predictor     predict.Predictor
	ShowChat      *atomic.Bool
	MessageCount  *uint64
}

// Start attaches all parallel consumers to NATS (chat + features).
func Start(ctx context.Context, opts Options) (*Consumers, error) {
	c := &Consumers{
		Store:         opts.Store,
		Monitor:       opts.Monitor,
		Elastic:       opts.Elastic,
		elasticOnline: opts.ElasticOnline,
		Helix:         opts.Helix,
		Predictor:     opts.Predictor,
		showChat:      opts.ShowChat,
		messageCount:  opts.MessageCount,
		cooldowns:     make(map[string]time.Time),
	}

	// 1) SQLite persist — batch of 30
	sub1, err := opts.Bus.SubscribeChat("*", SQLiteGroup, func(msg models.ChatMessage) {
		c.normalizeTS(&msg)
		if err := c.Store.Enqueue(msg); err != nil {
			fmt.Printf("WARN: [sqlite] enqueue error: %v\n", err)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("sqlite consumer: %w", err)
	}
	c.subs = append(c.subs, sub1)

	// 2) Features — 24s windows → features.twitch.<channel>
	sub2, err := opts.Bus.SubscribeChat("*", FeaturesGroup, func(msg models.ChatMessage) {
		c.normalizeTS(&msg)
		c.Monitor.OnChat(msg)
	})
	if err != nil {
		c.Stop()
		return nil, fmt.Errorf("features consumer: %w", err)
	}
	c.subs = append(c.subs, sub2)
	go c.Monitor.StartPollLoop(ctx)

	// 3) Elasticsearch — per message
	sub3, err := opts.Bus.SubscribeChat("*", ElasticGroup, func(msg models.ChatMessage) {
		c.normalizeTS(&msg)
		if c.messageCount != nil {
			atomic.AddUint64(c.messageCount, 1)
		}
		if c.elasticOnline && c.Elastic != nil {
			c.Elastic.IndexChat(msg)
		}
		if c.showChat != nil && c.showChat.Load() {
			fmt.Printf("[%s] %s: %s\n", msg.Channel, msg.UserName, msg.Content)
		}
	})
	if err != nil {
		c.Stop()
		return nil, fmt.Errorf("elastic consumer: %w", err)
	}
	c.subs = append(c.subs, sub3)

	// 4) Predict + clip — sink on features topic (ValSparks predict.py)
	sub4, err := opts.Bus.SubscribeFeatures("*", PredictGroup, func(fv models.FeatureVector) {
		c.onFeatures(ctx, fv)
	})
	if err != nil {
		c.Stop()
		return nil, fmt.Errorf("predict consumer: %w", err)
	}
	c.subs = append(c.subs, sub4)

	return c, nil
}

func (c *Consumers) normalizeTS(msg *models.ChatMessage) {
	now := time.Now().UTC()
	if msg.Timestamp.IsZero() || now.Sub(msg.Timestamp).Abs() > 10*time.Second {
		msg.Timestamp = now
	}
}

func (c *Consumers) onFeatures(ctx context.Context, fv models.FeatureVector) {
	if c.Predictor == nil || !c.Predictor.Ready() {
		fmt.Printf("[features] #%s msg_count=%.0f hype_score=%.0f (model not loaded — skipping predict)\n",
			fv.Channel, fv.MsgCount, fv.HypeScore)
		return
	}

	prob, err := c.Predictor.PredictProba(fv.Values())
	if err != nil {
		fmt.Printf("WARN: [predict] #%s error: %v\n", fv.Channel, err)
		return
	}
	c.LastProb.Store(fv.Channel, prob)
	prediction := predict.Decide(prob)

	fmt.Printf("[predict] #%s prob=%.3f prediction=%d (msg_count=%.0f hype_score=%.0f)\n",
		fv.Channel, prob, prediction, fv.MsgCount, fv.HypeScore)

	if prediction != 1 {
		return
	}

	now := time.Now().UTC()
	if !c.tryCooldown(fv.Channel, now) {
		fmt.Printf("[clip] #%s cooldown active (30s)\n", fv.Channel)
		return
	}

	c.createClip(ctx, fv, prob)
}

func (c *Consumers) tryCooldown(channel string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if until, ok := c.cooldowns[channel]; ok && now.Before(until) {
		return false
	}
	c.cooldowns[channel] = now.Add(ClipCooldown)
	return true
}

// IsInCooldown reports remaining cooldown for /status.
func (c *Consumers) IsInCooldown(channel string, now time.Time) (bool, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if until, ok := c.cooldowns[channel]; ok && now.Before(until) {
		return true, until.Sub(now)
	}
	return false, 0
}

func (c *Consumers) createClip(ctx context.Context, fv models.FeatureVector, prob float64) {
	if c.Helix == nil || !c.Helix.IsConfigured() {
		simURL := fmt.Sprintf("https://clips.twitch.tv/simulated_%s_%d", fv.Channel, time.Now().Unix())
		fmt.Printf("[clip] simulated #%s (no Helix token) prob=%.3f → %s\n", fv.Channel, prob, simURL)
		c.indexClip(fv, "sim_"+fv.Channel, simURL, "", prob)
		return
	}

	bID, err := c.Helix.GetUserID(ctx, fv.Channel)
	if err != nil {
		fmt.Printf("ERROR: [clip] resolve broadcaster #%s: %v\n", fv.Channel, err)
		return
	}
	clipResp, err := c.Helix.CreateClip(ctx, bID)
	if err != nil {
		fmt.Printf("ERROR: [clip] create #%s: %v\n", fv.Channel, err)
		return
	}
	clipURL := fmt.Sprintf("https://clips.twitch.tv/%s", clipResp.ID)
	fmt.Println("========================================================")
	fmt.Printf("[CLIP CREATED] #%s  prob=%.3f\n", fv.Channel, prob)
	fmt.Printf("   Watch: %s\n", clipURL)
	fmt.Printf("   Edit:  %s\n", clipResp.EditURL)
	fmt.Println("========================================================")
	c.indexClip(fv, clipResp.ID, clipURL, clipResp.EditURL, prob)
}

func (c *Consumers) indexClip(fv models.FeatureVector, clipID, url, editURL string, prob float64) {
	if !c.elasticOnline || c.Elastic == nil {
		return
	}
	c.Elastic.IndexClip(analytics.ClipEventDoc{
		Timestamp:   time.Now().UTC(),
		Channel:     fv.Channel,
		ClipID:      clipID,
		URL:         url,
		EditURL:     editURL,
		Title:       fmt.Sprintf("Auto-Clip on #%s (ml)", fv.Channel),
		Prob:        prob,
		Prediction:  1,
		MsgCount:    fv.MsgCount,
		HypeScore:   fv.HypeScore,
		UniqueUsers: int(fv.UniqueUsers),
		Reason:      "ml_hype_prediction",
	})
}

// Stop unsubscribes all consumers.
func (c *Consumers) Stop() {
	for _, s := range c.subs {
		if s != nil {
			_ = s.Unsubscribe()
		}
	}
	c.subs = nil
}
