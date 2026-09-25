package store_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
	"github.com/Raghav-Senthilkumar/Snipr/internal/store"
)

func TestSQLiteStore_BatchFlushAt30(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := store.Open(path, 30)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for i := 0; i < 29; i++ {
		if err := s.Enqueue(models.ChatMessage{
			ID:        fmt.Sprintf("msg_%d", i),
			Channel:   "tarik",
			UserName:  "u",
			Content:   "hi",
			Timestamp: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	n, _ := s.Count(context.Background())
	if n != 0 {
		t.Fatalf("expected 0 before flush threshold, got %d", n)
	}

	if err := s.Enqueue(models.ChatMessage{
		ID: "batch_trigger", Channel: "tarik", UserName: "u", Content: "hi", Timestamp: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	n, err = s.Count(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 30 {
		t.Fatalf("expected 30 after batch flush, got %d", n)
	}
}
