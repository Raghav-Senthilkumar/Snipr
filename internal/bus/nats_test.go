package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/bus"
	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

func TestNATSBus_PublishAndSubscribe(t *testing.T) {
	eventBus, err := bus.NewNATSBus(bus.Config{
		Port:       -1,
		StreamName: "CHAT_STREAM",
		MaxAge:     5 * time.Minute,
		MaxBytes:   16 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to create embedded NATS Bus: %v", err)
	}
	defer eventBus.Close()

	testMsg := models.ChatMessage{
		ID:        "msg_uuid_12345",
		Channel:   "tarik",
		UserID:    "user_999",
		UserName:  "HypeFan",
		Content:   "KEKW WHAT A PLAY KEKW",
		Emotes:    []string{"KEKW", "KEKW"},
		Bits:      100,
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
	}

	receivedChannel := make(chan models.ChatMessage, 1)

	sub, err := eventBus.SubscribeChat("tarik", func(msg models.ChatMessage) {
		receivedChannel <- msg
	})
	if err != nil {
		t.Fatalf("Failed to subscribe to chat: %v", err)
	}
	defer sub.Unsubscribe()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := eventBus.PublishChat(ctx, testMsg); err != nil {
		t.Fatalf("failed to publish chat message: %v", err)
	}

	select {
	case received := <-receivedChannel:
		if received.ID != testMsg.ID {
			t.Errorf("got ID %q, want %q", received.ID, testMsg.ID)
		}
		if received.Channel != "tarik" {
			t.Errorf("got channel %q, want 'tarik'", received.Channel)
		}
		if received.Content != testMsg.Content {
			t.Errorf("got content %q, want %q", received.Content, testMsg.Content)
		}
		if len(received.Emotes) != 2 || received.Emotes[0] != "KEKW" {
			t.Errorf("got emotes %v, want ['KEKW', 'KEKW']", received.Emotes)
		}
		if received.Bits != 100 {
			t.Errorf("got bits %d, want 100", received.Bits)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message to be delivered via NATS JetStream")
	}
}
