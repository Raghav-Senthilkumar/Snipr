package ingest_test

import (
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/ingest"
)

func TestParseIRCLine_PRIVMSG(t *testing.T) {
	raw := `@badge-info=;badges=moderator/1;color=#1E90FF;display-name=ModGuy;emotes=25:0-4,15-19;id=uuid-1234;room-id=12345;tmi-sent-ts=1700000000000;user-id=9999 :modguy!modguy@modguy.tmi.twitch.tv PRIVMSG #tarik :Kappa that was Kappa insane!`

	parsed, err := ingest.ParseIRCLine(raw)
	if err != nil {
		t.Fatalf("unexpected error parsing PRIVMSG: %v", err)
	}

	if parsed.Message == nil {
		t.Fatal("expected ChatMessage, got nil")
	}

	if parsed.Message.ID != "uuid-1234" {
		t.Errorf("got ID %q, want 'uuid-1234'", parsed.Message.ID)
	}
	if parsed.Message.Channel != "tarik" {
		t.Errorf("got channel %q, want 'tarik'", parsed.Message.Channel)
	}
	if parsed.Message.UserName != "ModGuy" {
		t.Errorf("got username %q, want 'ModGuy'", parsed.Message.UserName)
	}
	if parsed.Message.UserID != "9999" {
		t.Errorf("got user_id %q, want '9999'", parsed.Message.UserID)
	}
	if parsed.Message.Content != "Kappa that was Kappa insane!" {
		t.Errorf("got content %q, want 'Kappa that was Kappa insane!'", parsed.Message.Content)
	}
	if len(parsed.Message.Emotes) != 2 || parsed.Message.Emotes[0] != "Kappa" || parsed.Message.Emotes[1] != "Kappa" {
		t.Errorf("got emotes %v, want ['Kappa', 'Kappa']", parsed.Message.Emotes)
	}

	expectedTime := time.UnixMilli(1700000000000).UTC()
	if !parsed.Message.Timestamp.Equal(expectedTime) {
		t.Errorf("got timestamp %v, want %v", parsed.Message.Timestamp, expectedTime)
	}
}

func TestParseIRCLine_Bits(t *testing.T) {
	raw := `@bits=500;display-name=DonorGuy;id=uuid-bits;tmi-sent-ts=1700000000000;user-id=8888 :donor!donor@donor.tmi.twitch.tv PRIVMSG #shroud :cheer500 clutch!`

	parsed, err := ingest.ParseIRCLine(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Message.Bits != 500 {
		t.Errorf("got bits %d, want 500", parsed.Message.Bits)
	}
}

func TestParseIRCLine_PING(t *testing.T) {
	raw := "PING :tmi.twitch.tv"

	parsed, err := ingest.ParseIRCLine(raw)
	if err != nil {
		t.Fatalf("unexpected error parsing PING: %v", err)
	}

	if !parsed.IsPing {
		t.Errorf("expected IsPing to be true")
	}
	if parsed.PingPayload != "tmi.twitch.tv" {
		t.Errorf("got PingPayload %q, want 'tmi.twitch.tv'", parsed.PingPayload)
	}
}

func TestParseIRCLine_IgnoredCommands(t *testing.T) {
	raw := ":tmi.twitch.tv 001 user :Welcome to Twitch!"
	parsed, err := ingest.ParseIRCLine(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.Message != nil {
		t.Errorf("expected nil message for 001 welcome, got %+v", parsed.Message)
	}
}
