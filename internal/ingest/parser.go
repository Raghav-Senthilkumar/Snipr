package ingest

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

// ParseResult holds the parsed ChatMessage or PING control notice.
type ParseResult struct {
	Message     *models.ChatMessage
	IsPing      bool
	PingPayload string
}

// ParseIRCLine parses a single raw IRC wire message line from Twitch.
func ParseIRCLine(raw string) (*ParseResult, error) {
	line := strings.TrimRight(raw, "\r\n")
	if line == "" {
		return nil, errors.New("empty IRC line")
	}

	// 1. Check for PING keepalive
	if strings.HasPrefix(line, "PING") {
		parts := strings.SplitN(line, ":", 2)
		payload := "tmi.twitch.tv"
		if len(parts) == 2 {
			payload = parts[1]
		}
		return &ParseResult{
			IsPing:      true,
			PingPayload: payload,
		}, nil
	}

	tags := make(map[string]string)
	rest := line

	// 2. Parse Tags if present (starts with '@')
	if strings.HasPrefix(rest, "@") {
		parts := strings.SplitN(rest, " ", 2)
		if len(parts) < 2 {
			return nil, errors.New("malformed tag prefix")
		}
		tagStr := parts[0][1:] // strip '@'
		rest = parts[1]

		for _, pair := range strings.Split(tagStr, ";") {
			kv := strings.SplitN(pair, "=", 2)
			if len(kv) == 2 {
				tags[kv[0]] = unescapeTagValue(kv[1])
			} else {
				tags[kv[0]] = ""
			}
		}
	}

	// 3. Skip prefix if present (starts with ':')
	var prefixNick string
	if strings.HasPrefix(rest, ":") {
		parts := strings.SplitN(rest, " ", 2)
		if len(parts) < 2 {
			return nil, errors.New("malformed prefix")
		}
		prefix := parts[0][1:]
		rest = parts[1]

		// Extract nick from nick!user@host
		if idx := strings.Index(prefix, "!"); idx != -1 {
			prefixNick = prefix[:idx]
		} else {
			prefixNick = prefix
		}
	}

	// 4. Parse Command and Trailing text
	// We specifically look for PRIVMSG #channel :content
	commandParts := strings.SplitN(rest, " :", 2)
	header := commandParts[0]
	content := ""
	if len(commandParts) == 2 {
		content = commandParts[1]
	}

	headerFields := strings.Fields(header)
	if len(headerFields) < 2 || headerFields[0] != "PRIVMSG" {
		// Non-chat event (e.g., JOIN, PART, 001, etc.)
		return &ParseResult{}, nil
	}

	// Channel is second field: "#tarik" -> "tarik"
	channel := strings.TrimPrefix(headerFields[1], "#")
	channel = strings.ToLower(channel)

	// 5. Extract fields from tags
	msgID := tags["id"]
	userID := tags["user-id"]
	userName := tags["display-name"]
	if userName == "" {
		userName = prefixNick
	}

	// Timestamp
	timestamp := time.Now().UTC()
	if tsStr, ok := tags["tmi-sent-ts"]; ok {
		if ms, err := strconv.ParseInt(tsStr, 10, 64); err == nil {
			timestamp = time.UnixMilli(ms).UTC()
		}
	}

	// Bits
	bits := 0
	if bitsStr, ok := tags["bits"]; ok {
		if b, err := strconv.Atoi(bitsStr); err == nil {
			bits = b
		}
	}

	// Emotes
	emotes := parseEmotes(tags["emotes"], content)

	chatMsg := &models.ChatMessage{
		ID:        msgID,
		Channel:   channel,
		UserID:    userID,
		UserName:  userName,
		Content:   content,
		Emotes:    emotes,
		Bits:      bits,
		Timestamp: timestamp,
	}

	return &ParseResult{
		Message: chatMsg,
	}, nil
}

// parseEmotes extracts emote names from message content based on Twitch's emote tag.
// Twitch format: <emote_id>:<start>-<end>,<start>-<end>/<emote_id>:<start>-<end>
// Character positions are rune/character indices, not byte offsets.
func parseEmotes(emoteTag, content string) []string {
	if emoteTag == "" || content == "" {
		return nil
	}

	runes := []rune(content)
	totalRunes := len(runes)
	var emotes []string

	// Split by '/' for different emote IDs
	for _, emoteGroup := range strings.Split(emoteTag, "/") {
		parts := strings.SplitN(emoteGroup, ":", 2)
		if len(parts) != 2 {
			continue
		}

		// Split by ',' for multiple instances of this emote
		ranges := strings.Split(parts[1], ",")
		for _, r := range ranges {
			indices := strings.SplitN(r, "-", 2)
			if len(indices) != 2 {
				continue
			}

			start, err1 := strconv.Atoi(indices[0])
			end, err2 := strconv.Atoi(indices[1])
			if err1 != nil || err2 != nil {
				continue
			}

			if start >= 0 && end >= start && end < totalRunes {
				emoteStr := string(runes[start : end+1])
				emotes = append(emotes, emoteStr)
			}
		}
	}

	return emotes
}

func unescapeTagValue(val string) string {
	if !strings.Contains(val, `\`) {
		return val
	}
	r := strings.NewReplacer(
		`\:`, ";",
		`\s`, " ",
		`\\`, `\`,
		`\r`, "\r",
		`\n`, "\n",
	)
	return r.Replace(val)
}
