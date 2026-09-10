package detector

import (
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

// HypeKeywords contains common Twitch hype tokens and emotes.
var DefaultHypeTokens = map[string]struct{}{
	"kekw":         {},
	"lul":          {},
	"omegalul":     {},
	"lulw":         {},
	"pog":          {},
	"pogchamp":     {},
	"pogu":         {},
	"w":            {},
	"sheesh":       {},
	"insane":       {},
	"clip":         {},
	"clipit":       {},
	"holy":         {},
	"aimbot":       {},
	"clutch":       {},
	"omg":          {},
	"wtff":         {},
	"no way":       {},
	"vac":          {},
	"gg":           {},
	"f":            {},
	"l":            {},
	"diff":         {},
	"cooking":      {},
	"let him cook": {},
}

// Bucket stores pre-aggregated metrics for a single 1-second time slice.
type Bucket struct {
	SecondUnix     int64
	MessageCount   int
	HypeEmoteCount int
	Bits           int
	UniqueUsers    map[string]struct{}
}

func newBucket(sec int64) *Bucket {
	return &Bucket{
		SecondUnix:  sec,
		UniqueUsers: make(map[string]struct{}),
	}
}

// WindowStats holds the computed comparison metrics between recent and baseline windows.
type WindowStats struct {
	RecentMessages   int
	RecentVelocity   float64 // messages / sec
	BaselineMean     float64 // mean messages / sec
	BaselineStdDev   float64 // standard deviation of msgs / sec
	ZScore           float64
	HypeRatio        float64 // percentage of hype tokens
	ChatterDiversity float64 // ratio of unique chatters to messages
	UniqueChatters   int
	RecentBits       int
	BaselineSeconds  int // number of valid historical seconds observed
}

// RollingWindow maintains a 1-second-bucket circular ring buffer for a channel.
type RollingWindow struct {
	mu          sync.RWMutex
	sizeSeconds int
	buckets     []*Bucket
}

// NewRollingWindow creates a rolling window of the given second capacity (e.g. 300 for 5 minutes).
func NewRollingWindow(sizeSeconds int) *RollingWindow {
	if sizeSeconds <= 0 {
		sizeSeconds = 300 // default 5 minutes
	}
	return &RollingWindow{
		sizeSeconds: sizeSeconds,
		buckets:     make([]*Bucket, sizeSeconds),
	}
}

// AddMessage places an incoming ChatMessage into its corresponding 1-second bucket.
func (rw *RollingWindow) AddMessage(msg models.ChatMessage) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	sec := msg.Timestamp.Unix()
	idx := int(sec % int64(rw.sizeSeconds))
	if idx < 0 {
		idx = -idx
	}

	b := rw.buckets[idx]
	if b == nil || b.SecondUnix != sec {
		// New second slice: reset bucket
		b = newBucket(sec)
		rw.buckets[idx] = b
	}

	b.MessageCount++
	b.Bits += msg.Bits
	if msg.UserID != "" {
		b.UniqueUsers[msg.UserID] = struct{}{}
	}

	// Count hype tokens from parsed emotes and raw text content
	hypeCount := 0
	for _, emote := range msg.Emotes {
		if isHypeToken(emote) {
			hypeCount++
		}
	}

	words := strings.Fields(strings.ToLower(msg.Content))
	for _, word := range words {
		if isHypeToken(word) {
			hypeCount++
		}
	}

	b.HypeEmoteCount += hypeCount
}

// ComputeStats calculates the baseline vs recent window metrics for the given reference time.
func (rw *RollingWindow) ComputeStats(recentSec, baselineSec int, now time.Time) WindowStats {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	currentSec := now.Unix()
	stats := WindowStats{}

	recentUniqueMap := make(map[string]struct{})
	recentHypeCount := 0
	recentMsgCount := 0
	recentBits := 0

	// 1. Aggregate the Recent Window [currentSec - recentSec + 1 ... currentSec]
	for i := 0; i < recentSec; i++ {
		sec := currentSec - int64(i)
		idx := int(sec % int64(rw.sizeSeconds))
		if idx < 0 {
			idx = -idx
		}

		b := rw.buckets[idx]
		if b != nil && b.SecondUnix == sec {
			recentMsgCount += b.MessageCount
			recentHypeCount += b.HypeEmoteCount
			recentBits += b.Bits
			for uid := range b.UniqueUsers {
				recentUniqueMap[uid] = struct{}{}
			}
		}
	}

	stats.RecentMessages = recentMsgCount
	stats.RecentVelocity = float64(recentMsgCount) / float64(recentSec)
	stats.UniqueChatters = len(recentUniqueMap)
	stats.RecentBits = recentBits

	if recentMsgCount > 0 {
		stats.HypeRatio = float64(recentHypeCount) / float64(recentMsgCount)
		stats.ChatterDiversity = float64(len(recentUniqueMap)) / float64(recentMsgCount)
	}

	// 2. Aggregate the Baseline Window [currentSec - baselineSec ... currentSec - recentSec]
	// We exclude the recent window so the spike itself does not inflate the baseline!
	var baselineRates []float64
	for i := recentSec; i < baselineSec; i++ {
		sec := currentSec - int64(i)
		idx := int(sec % int64(rw.sizeSeconds))
		if idx < 0 {
			idx = -idx
		}

		b := rw.buckets[idx]
		if b != nil && b.SecondUnix == sec {
			baselineRates = append(baselineRates, float64(b.MessageCount))
		} else {
			// Second with 0 messages
			baselineRates = append(baselineRates, 0.0)
		}
	}

	stats.BaselineSeconds = len(baselineRates)
	if len(baselineRates) == 0 {
		return stats
	}

	// Compute Mean (μ)
	sum := 0.0
	for _, rate := range baselineRates {
		sum += rate
	}
	mean := sum / float64(len(baselineRates))
	stats.BaselineMean = mean

	// Compute Standard Deviation (σ)
	varianceSum := 0.0
	for _, rate := range baselineRates {
		diff := rate - mean
		varianceSum += diff * diff
	}
	variance := varianceSum / float64(len(baselineRates))
	stdDev := math.Sqrt(variance)

	// Clamp minimum standard deviation to 1.0 to avoid division by zero or extreme sensitivity
	clampedStdDev := math.Max(stdDev, 1.0)
	stats.BaselineStdDev = stdDev

	// Compute Z-score: (RecentVelocity - BaselineMean) / ClampedStdDev
	stats.ZScore = (stats.RecentVelocity - mean) / clampedStdDev

	return stats
}

func isHypeToken(token string) bool {
	clean := strings.ToLower(token)
	_, ok := DefaultHypeTokens[clean]
	return ok
}
