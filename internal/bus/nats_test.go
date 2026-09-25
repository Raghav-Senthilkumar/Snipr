package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/bus"
	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
)

func TestNATSBus_PublishAndSubscribe(t *testing.T) {
	eventBus, err := bus.NewNATSBus(bus.Config{Port: -1})
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

	sub, err := eventBus.SubscribeChat("tarik", "test_chat_group", func(msg models.ChatMessage) {
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
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message to be delivered via NATS JetStream")
	}
}

func TestNATSBus_ParallelConsumersAndFeatures(t *testing.T) {
	eventBus, err := bus.NewNATSBus(bus.Config{Port: -1})
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	defer eventBus.Close()

	chatA := make(chan models.ChatMessage, 1)
	chatB := make(chan models.ChatMessage, 1)
	featCh := make(chan models.FeatureVector, 1)

	subA, err := eventBus.SubscribeChat("*", "group_a", func(m models.ChatMessage) { chatA <- m })
	if err != nil {
		t.Fatal(err)
	}
	defer subA.Unsubscribe()

	subB, err := eventBus.SubscribeChat("*", "group_b", func(m models.ChatMessage) { chatB <- m })
	if err != nil {
		t.Fatal(err)
	}
	defer subB.Unsubscribe()

	subF, err := eventBus.SubscribeFeatures("*", "predict_test", func(f models.FeatureVector) { featCh <- f })
	if err != nil {
		t.Fatal(err)
	}
	defer subF.Unsubscribe()

	ctx := context.Background()
	msg := models.ChatMessage{ID: "p1", Channel: "xqc", UserName: "u", Content: "hi", Timestamp: time.Now().UTC()}
	if err := eventBus.PublishChat(ctx, msg); err != nil {
		t.Fatal(err)
	}

	// Both durable groups should receive a copy
	for _, ch := range []chan models.ChatMessage{chatA, chatB} {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("parallel chat consumer timed out")
		}
	}

	fv := models.FeatureVector{Channel: "xqc", MsgCount: 5, WindowEnd: time.Now().UTC()}
	if err := eventBus.PublishFeatures(ctx, fv); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-featCh:
		if got.MsgCount != 5 {
			t.Fatalf("features msg_count=%v", got.MsgCount)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("features consumer timed out")
	}
}
