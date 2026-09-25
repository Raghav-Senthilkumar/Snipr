package analytics

import (
	"time"
)

// ChatMessageDoc represents a single Twitch chat message document in Elasticsearch.
type ChatMessageDoc struct {
	Timestamp time.Time `json:"@timestamp"`
	EventType string    `json:"event_type"`
	Channel   string    `json:"channel"`
	UserID    string    `json:"user_id"`
	UserName  string    `json:"user_name"`
	Content   string    `json:"content"`
	Emotes    []string  `json:"emotes"`
	Bits      int       `json:"bits"`
	IsHype    bool      `json:"is_hype"`
}

// ClipEventDoc represents an auto-clipped highlight moment document in Elasticsearch.
type ClipEventDoc struct {
	Timestamp   time.Time `json:"@timestamp"`
	EventType   string    `json:"event_type"`
	Channel     string    `json:"channel"`
	ClipID      string    `json:"clip_id"`
	URL         string    `json:"url"`
	EditURL     string    `json:"edit_url,omitempty"`
	Title       string    `json:"title"`
	Prob        float64   `json:"prob"`
	Prediction  int       `json:"prediction"`
	MsgCount    float64   `json:"msg_count"`
	HypeScore   float64   `json:"hype_score"`
	UniqueUsers int       `json:"unique_users"`
	Reason      string    `json:"reason"`
}
