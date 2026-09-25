package features_test

import (
	"math"
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/features"
)

func TestSimpleNormalize_CapsRunsAtTwo(t *testing.T) {
	got := features.SimpleNormalize("  WOOOOOOW   nooooo  ")
	want := "woow noo"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestHypeScore_FuzzyJaccard(t *testing.T) {
	// Exact hits still count once each
	text := "omg wtf holy wow"
	score := features.HypeScore(text)
	if score != 4 {
		t.Fatalf("exact score=%v want 4", score)
	}

	// Typos / elongations via Jaccard (omgg ~ omg, www listed, wtff ~ wtf)
	fuzzy := features.HypeScore("omgg that was www wtff")
	if fuzzy < 2 {
		t.Fatalf("fuzzy score=%v want >= 2", fuzzy)
	}

	// Unrelated chat should not inflate
	quiet := features.HypeScore("nice crosshair placement today")
	if quiet != 0 {
		t.Fatalf("quiet score=%v want 0", quiet)
	}
}

func TestBigramJaccard_WWW(t *testing.T) {
	// "www" vs normalized "ww" (from WWWW) should be a strong match
	j := features.HypeScore(features.SimpleNormalize("WWWW LETS GO"))
	if j < 1 {
		t.Fatalf("expected www/wow-family hit from WWWW, score=%v", j)
	}
}

func TestComputeWindowFeatures_EightFields(t *testing.T) {
	now := time.Now().UTC()
	msgs := []features.BufferedMessage{
		{UserName: "a", NormText: "pog", RawLen: 3},
		{UserName: "b", NormText: "pog", RawLen: 3},
		{UserName: "a", NormText: "omg insane", RawLen: 10},
	}
	fv := features.ComputeWindowFeatures("tarik", now.Add(-24*time.Second), now, msgs)

	if fv.MsgCount != 3 {
		t.Fatalf("msg_count=%v", fv.MsgCount)
	}
	if fv.UniqueUsers != 2 {
		t.Fatalf("unique_users=%v", fv.UniqueUsers)
	}
	if fv.UniqueNormMsgs != 2 {
		t.Fatalf("unique_norm_msgs=%v", fv.UniqueNormMsgs)
	}
	if fv.MaxRepeatCount != 2 {
		t.Fatalf("max_repeat=%v", fv.MaxRepeatCount)
	}
	if math.Abs(fv.RepeatRatio-1.0/3.0) > 1e-9 {
		t.Fatalf("repeat_ratio=%v", fv.RepeatRatio)
	}
	if fv.HypeScore < 1 {
		t.Fatalf("expected hype_score >= 1, got %v", fv.HypeScore)
	}
	vals := fv.Values()
	if len(vals) != 8 {
		t.Fatalf("values len=%d", len(vals))
	}
}
