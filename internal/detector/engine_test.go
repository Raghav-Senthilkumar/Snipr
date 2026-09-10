package detector_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/detector"
	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

func TestEngine_NormalChat_NoTrigger(t *testing.T) {
	eng := detector.NewEngine(detector.DefaultConfig())
	now := time.Now().UTC()

	// Feed 100 seconds of normal chat (2 msgs/sec, random conversation)
	for s := 100; s >= 0; s-- {
		secTime := now.Add(-time.Duration(s) * time.Second)
		eng.ProcessMessage(models.ChatMessage{
			ID:        fmt.Sprintf("msg_%d_1", s),
			Channel:   "tarik",
			UserID:    fmt.Sprintf("user_%d", s%20),
			Content:   "just talking about valorant crosshair placement",
			Timestamp: secTime,
		})
		eng.ProcessMessage(models.ChatMessage{
			ID:        fmt.Sprintf("msg_%d_2", s),
			Channel:   "tarik",
			UserID:    fmt.Sprintf("user_%d", (s+1)%20),
			Content:   "nice weather outside today",
			Timestamp: secTime,
		})
	}

	event, triggered := eng.Evaluate("tarik", now)
	if triggered {
		t.Fatalf("expected no trigger on normal chat, got event: %+v", event)
	}
}

func TestEngine_HypeBurst_TriggersClip(t *testing.T) {
	eng := detector.NewEngine(detector.DefaultConfig())
	now := time.Now().UTC()

	// 1. Establish normal baseline for 100 seconds (1 msg/sec)
	for s := 100; s > 15; s-- {
		secTime := now.Add(-time.Duration(s) * time.Second)
		eng.ProcessMessage(models.ChatMessage{
			ID:        fmt.Sprintf("base_%d", s),
			Channel:   "tarik",
			UserID:    fmt.Sprintf("user_%d", s%20),
			Content:   "normal chat message",
			Timestamp: secTime,
		})
	}

	// 2. Inject massive 15-second hype spike: 15 diverse chatters spamming KEKW
	for s := 15; s >= 0; s-- {
		secTime := now.Add(-time.Duration(s) * time.Second)
		for u := 0; u < 10; u++ {
			eng.ProcessMessage(models.ChatMessage{
				ID:        fmt.Sprintf("hype_%d_%d", s, u),
				Channel:   "tarik",
				UserID:    fmt.Sprintf("hype_user_%d", u),
				Content:   "KEKW THAT FLICK WAS INSANE KEKW",
				Emotes:    []string{"KEKW", "KEKW"},
				Timestamp: secTime,
			})
		}
	}

	event, triggered := eng.Evaluate("tarik", now)
	if !triggered {
		stats := eng.GetStats("tarik", now)
		t.Fatalf("expected hype trigger, but did not trigger. Stats: %+v", stats)
	}

	if event.Channel != "tarik" {
		t.Errorf("got channel %q, want 'tarik'", event.Channel)
	}
	if event.Stats.ZScore < 2.5 {
		t.Errorf("expected Z-score >= 2.5, got %f", event.Stats.ZScore)
	}
	if event.Stats.HypeRatio < 0.35 {
		t.Errorf("expected HypeRatio >= 0.35, got %f", event.Stats.HypeRatio)
	}
}

func TestEngine_SpamBot_AntiBotFilter(t *testing.T) {
	eng := detector.NewEngine(detector.DefaultConfig())
	now := time.Now().UTC()

	// 1 single user spamming 100 messages in recent window
	for s := 10; s >= 0; s-- {
		secTime := now.Add(-time.Duration(s) * time.Second)
		for m := 0; m < 10; m++ {
			eng.ProcessMessage(models.ChatMessage{
				ID:        fmt.Sprintf("spam_%d_%d", s, m),
				Channel:   "shroud",
				UserID:    "single_spambot_user_id", // Same user!
				Content:   "KEKW KEKW KEKW",
				Emotes:    []string{"KEKW"},
				Timestamp: secTime,
			})
		}
	}

	_, triggered := eng.Evaluate("shroud", now)
	if triggered {
		t.Fatal("expected anti-bot filter to reject single-user spam burst, but it triggered!")
	}
}

func TestEngine_CooldownEnforcement(t *testing.T) {
	cfg := detector.DefaultConfig()
	cfg.CooldownDuration = 60 * time.Second
	eng := detector.NewEngine(cfg)
	now := time.Now().UTC()

	// Inject hype spike to trigger
	for s := 10; s >= 0; s-- {
		secTime := now.Add(-time.Duration(s) * time.Second)
		for u := 0; u < 10; u++ {
			eng.ProcessMessage(models.ChatMessage{
				ID:        fmt.Sprintf("hype_%d_%d", s, u),
				Channel:   "tarik",
				UserID:    fmt.Sprintf("user_%d", u),
				Content:   "POG POG INSANE",
				Emotes:    []string{"POG"},
				Timestamp: secTime,
			})
		}
	}

	// First evaluation: must trigger
	_, triggered := eng.Evaluate("tarik", now)
	if !triggered {
		t.Fatal("expected first evaluation to trigger")
	}

	// Immediate second evaluation (5 seconds later): must NOT trigger due to cooldown
	secondEvalTime := now.Add(5 * time.Second)
	_, secondTriggered := eng.Evaluate("tarik", secondEvalTime)
	if secondTriggered {
		t.Fatal("expected cooldown to block second trigger, but it triggered again!")
	}

	// Third evaluation (65 seconds later): cooldown expired
	thirdEvalTime := now.Add(65 * time.Second)
	inCooldown, _ := eng.IsInCooldown("tarik", thirdEvalTime)
	if inCooldown {
		t.Errorf("expected cooldown to expire after 65 seconds")
	}
}
