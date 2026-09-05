package auth_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/auth"
)

func TestCallbackServer_Success(t *testing.T) {
	state, _ := auth.GenerateRandomState()
	srv := auth.NewCallbackServer(13338, state) // Use port 13338 for test

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start callback server: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Simulate browser redirect in a background goroutine
	go func() {
		time.Sleep(50 * time.Millisecond)
		resp, err := http.Get("http://127.0.0.1:13338/callback?code=test_code_123&state=" + state)
		if err != nil {
			t.Errorf("failed to make test GET request: %v", err)
			return
		}
		resp.Body.Close()
	}()

	code, err := srv.WaitForCode(ctx)
	if err != nil {
		t.Fatalf("unexpected error waiting for code: %v", err)
	}

	if code != "test_code_123" {
		t.Errorf("got code %q, want 'test_code_123'", code)
	}
}

func TestCallbackServer_StateMismatch(t *testing.T) {
	state, _ := auth.GenerateRandomState()
	srv := auth.NewCallbackServer(13339, state)

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start callback server: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		time.Sleep(50 * time.Millisecond)
		resp, _ := http.Get("http://127.0.0.1:13339/callback?code=test_code&state=wrong_state")
		if resp != nil {
			resp.Body.Close()
		}
	}()

	_, err := srv.WaitForCode(ctx)
	if err == nil {
		t.Fatalf("expected error due to state mismatch, got nil")
	}
}
