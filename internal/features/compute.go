package features

import (
	"math"
	"strings"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

// BufferedMessage is a chat line held inside a 24s feature window.
type BufferedMessage struct {
	UserName string
	NormText string
	RawLen   int
}

// ComputeWindowFeatures builds the ValSparks 8-feature vector from a non-empty buffer.
func ComputeWindowFeatures(channel string, start, end time.Time, msgs []BufferedMessage) models.FeatureVector {
	msgCount := float64(len(msgs))
	users := make(map[string]struct{})
	freq := make(map[string]int)
	totalLen := 0
	var joined strings.Builder

	for _, m := range msgs {
		users[m.UserName] = struct{}{}
		freq[m.NormText]++
		totalLen += m.RawLen
		joined.WriteString(m.NormText)
		joined.WriteByte(' ')
	}

	uniqueNorm := float64(len(freq))
	maxRepeat := 0
	for _, c := range freq {
		if c > maxRepeat {
			maxRepeat = c
		}
	}

	avgLen := 0.0
	if msgCount > 0 {
		avgLen = float64(totalLen) / msgCount
	}

	repeatRatio := 0.0
	if msgCount > 0 {
		repeatRatio = (msgCount - uniqueNorm) / msgCount
	}

	return models.FeatureVector{
		Channel:        channel,
		WindowStart:    start,
		WindowEnd:      end,
		MsgCount:       msgCount,
		UniqueUsers:    float64(len(users)),
		AvgMsgLen:      avgLen,
		MaxRepeatCount: float64(maxRepeat),
		UniqueNormMsgs: uniqueNorm,
		EntropyRaw:     shannonEntropy(freq, int(msgCount)),
		HypeScore:      HypeScore(joined.String()),
		RepeatRatio:    repeatRatio,
	}
}

func shannonEntropy(freq map[string]int, total int) float64 {
	if total <= 0 {
		return 0
	}
	h := 0.0
	n := float64(total)
	for _, c := range freq {
		p := float64(c) / n
		if p > 0 {
			h -= p * math.Log2(p)
		}
	}
	return h
}
