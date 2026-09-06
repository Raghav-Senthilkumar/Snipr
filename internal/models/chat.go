package models

import (
	"time"
)

// ChatMessage represents a normalized Twitch chat event on the message bus.
type ChatMessage struct {
	ID        string    `json:"id"`        // Twitch message UUID
	Channel   string    `json:"channel"`   // Streamer channel (e.g. "tarik")
	UserID    string    `json:"user_id"`   // Unique chatter ID
	UserName  string    `json:"user_name"` // Chatter display name
	Content   string    `json:"content"`   // Raw text message
	Emotes    []string  `json:"emotes"`    // Extracted emote codes (e.g. "KEKW", "LUL")
	Bits      int       `json:"bits"`      // Cheer bits if any (0 if none)
	Timestamp time.Time `json:"timestamp"` // Twitch server timestamp
}
