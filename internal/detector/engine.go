package detector

import (
	"sync"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

// Config defines detection sensitivity parameters and timing thresholds.
type Config struct {
	RecentWindowSec           int           // Duration of the immediate evaluation window (e.g. 15s)
	BaselineWindowSec         int           // Duration of the rolling history baseline (e.g. 180s - 300s)
	ZScoreThreshold           float64       // Standard deviation multiplier (e.g. 2.5)
	HypeRatioThreshold        float64       // Minimum proportion of hype tokens (e.g. 0.35)
	MinChatters               int           // Minimum unique chatters to avoid single-user spam (e.g. 5)
	MinRecentMessages         int           // Minimum messages in recent window before triggering (e.g. 8)
	AbsoluteVelocityThreshold float64       // Immediate override rate (e.g. 20 msgs/sec)
	CooldownDuration          time.Duration // Time to wait after a clip trigger before allowing another (e.g. 60s)
	ClipCaptureDelay          time.Duration // Delay before calling Helix API so whole play is captured (e.g. 8s)
}

// DefaultConfig provides recommended production settings for Twitch detection.
func DefaultConfig() Config {
	return Config{
		RecentWindowSec:           15,
		BaselineWindowSec:         180,
		ZScoreThreshold:           2.5,
		HypeRatioThreshold:        0.35,
		MinChatters:               5,
		MinRecentMessages:         8,
		AbsoluteVelocityThreshold: 20.0,
		CooldownDuration:          60 * time.Second,
		ClipCaptureDelay:          8 * time.Second,
	}
}

// TriggerEvent contains metadata when a clip-worthy moment is identified.
type TriggerEvent struct {
	Channel           string
	TriggerTime       time.Time
	ScheduledClipTime time.Time // TriggerTime + ClipCaptureDelay
	Stats             WindowStats
	Reason            string
}

// Engine manages rolling windows and cooldown state machines across multiple channels.
type Engine struct {
	config    Config
	mu        sync.RWMutex
	windows   map[string]*RollingWindow
	cooldowns map[string]time.Time
}

// NewEngine initializes a multi-channel detection engine.
func NewEngine(cfg Config) *Engine {
	if cfg.RecentWindowSec <= 0 {
		cfg = DefaultConfig()
	}
	return &Engine{
		config:    cfg,
		windows:   make(map[string]*RollingWindow),
		cooldowns: make(map[string]time.Time),
	}
}

// ProcessMessage ingests a ChatMessage into its channel's rolling window.
func (e *Engine) ProcessMessage(msg models.ChatMessage) {
	window := e.getOrCreateWindow(msg.Channel)
	window.AddMessage(msg)
}

// Evaluate checks whether a channel currently meets the hype trigger criteria.
func (e *Engine) Evaluate(channel string, now time.Time) (*TriggerEvent, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Check if channel is currently in cooldown
	if cd, ok := e.cooldowns[channel]; ok {
		if now.Before(cd) {
			return nil, false
		}
		// Cooldown has expired
		delete(e.cooldowns, channel)
	}

	window, ok := e.windows[channel]
	if !ok {
		return nil, false
	}

	// 2. Compute metrics for this channel
	stats := window.ComputeStats(e.config.RecentWindowSec, e.config.BaselineWindowSec, now)

	// Guard: Need a minimal message count and diverse chatters
	if stats.RecentMessages < e.config.MinRecentMessages || stats.UniqueChatters < e.config.MinChatters {
		return nil, false
	}

	triggered := false
	reason := ""

	// Condition A: Absolute Velocity Override (for cold start or massive instant raids)
	if stats.RecentVelocity >= e.config.AbsoluteVelocityThreshold {
		triggered = true
		reason = "absolute_velocity_burst"
	}

	// Condition B: Statistical Z-Score Spike
	if stats.ZScore >= e.config.ZScoreThreshold {
		triggered = true
		reason = "statistical_zscore_spike"
	}

	// Condition C: High Bits Cheer Spike
	if stats.RecentBits >= 1000 {
		triggered = true
		reason = "bits_cheer_spike"
	}

	if !triggered {
		return nil, false
	}

	// 3. Enter Cooldown to prevent duplicate clips
	e.cooldowns[channel] = now.Add(e.config.CooldownDuration)

	event := &TriggerEvent{
		Channel:           channel,
		TriggerTime:       now,
		ScheduledClipTime: now.Add(e.config.ClipCaptureDelay),
		Stats:             stats,
		Reason:            reason,
	}

	return event, true
}

// GetStats returns current window metrics for debugging or UI monitoring.
func (e *Engine) GetStats(channel string, now time.Time) WindowStats {
	e.mu.RLock()
	window, ok := e.windows[channel]
	e.mu.RUnlock()

	if !ok {
		return WindowStats{}
	}
	return window.ComputeStats(e.config.RecentWindowSec, e.config.BaselineWindowSec, now)
}

// IsInCooldown checks if a channel is in post-clip cooldown.
func (e *Engine) IsInCooldown(channel string, now time.Time) (bool, time.Duration) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if cd, ok := e.cooldowns[channel]; ok {
		if now.Before(cd) {
			return true, cd.Sub(now)
		}
	}
	return false, 0
}

func (e *Engine) getOrCreateWindow(channel string) *RollingWindow {
	e.mu.Lock()
	defer e.mu.Unlock()

	window, ok := e.windows[channel]
	if !ok {
		window = NewRollingWindow(e.config.BaselineWindowSec + 60)
		e.windows[channel] = window
	}
	return window
}
