package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// GenerateRandomState generates a cryptographically secure random string for CSRF mitigation.
func GenerateRandomState() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// CallbackResult holds the authorization code or error returned to the local listener.
type CallbackResult struct {
	Code  string
	Error error
}

// CallbackServer manages an ephemeral local HTTP listener for the OAuth redirect.
type CallbackServer struct {
	port     int
	state    string
	resultCh chan CallbackResult
	server   *http.Server
	listener net.Listener
}

// NewCallbackServer creates an instance ready to listen on the specified port.
func NewCallbackServer(port int, state string) *CallbackServer {
	return &CallbackServer{
		port:     port,
		state:    state,
		resultCh: make(chan CallbackResult, 1),
	}
}

// Start spins up the listener. It returns once the server is listening.
func (s *CallbackServer) Start() error {
	addr := fmt.Sprintf("127.0.0.1:%d", s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind callback server to %s: %w", addr, err)
	}
	s.listener = ln

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", s.handleCallback)

	s.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		_ = s.server.Serve(s.listener)
	}()

	return nil
}

// WaitForCode blocks until the callback is called or the context expires.
func (s *CallbackServer) WaitForCode(ctx context.Context) (string, error) {
	defer s.Shutdown()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-s.resultCh:
		return res.Code, res.Error
	}
}

// Shutdown gracefully stops the local server.
func (s *CallbackServer) Shutdown() {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.server.Shutdown(ctx)
	}
}

func (s *CallbackServer) handleCallback(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// Check for errors returned by Twitch (e.g., user clicked "Cancel")
	if errMsg := query.Get("error"); errMsg != "" {
		desc := query.Get("error_description")
		err := fmt.Errorf("twitch auth denied: %s (%s)", errMsg, desc)
		s.resultCh <- CallbackResult{Error: err}
		http.Error(w, "Authentication failed. You can close this window.", http.StatusForbidden)
		return
	}

	// Verify CSRF state
	receivedState := query.Get("state")
	if receivedState != s.state {
		err := errors.New("invalid state parameter (possible CSRF attack)")
		s.resultCh <- CallbackResult{Error: err}
		http.Error(w, "Invalid state. Authentication aborted.", http.StatusBadRequest)
		return
	}

	code := query.Get("code")
	if code == "" {
		err := errors.New("missing authorization code in callback")
		s.resultCh <- CallbackResult{Error: err}
		http.Error(w, "Missing code parameter.", http.StatusBadRequest)
		return
	}

	// Send code to listener channel
	s.resultCh <- CallbackResult{Code: code}

	// Respond with a clean HTML page
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`<!DOCTYPE html>
<html>
<head>
    <title>Snipr - Connected!</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; background: #0e0e10; color: #efeff1; }
        .card { text-align: center; background: #18181b; padding: 40px; border-radius: 12px; border: 1px solid #9146ff; max-width: 400px; }
        h1 { color: #a970ff; margin-bottom: 8px; }
        p { color: #adadb8; font-size: 14px; }
    </style>
</head>
<body>
    <div class="card">
        <h1>Connected to Twitch!</h1>
        <p>Authentication was successful. You can close this tab and return to <strong>Snipr</strong>.</p>
    </div>
</body>
</html>`))
}
