package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
	_ "modernc.org/sqlite"
)

const DefaultBatchSize = 30

// SQLiteStore batches chat messages and flushes every BatchSize inserts (ValSparks Postgres path).
type SQLiteStore struct {
	db        *sql.DB
	batchSize int
	mu        sync.Mutex
	buf       []models.ChatMessage
}

// Open opens (or creates) a SQLite database and ensures the schema exists.
func Open(path string, batchSize int) (*SQLiteStore, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	schema := `
CREATE TABLE IF NOT EXISTS chat_messages (
	id TEXT PRIMARY KEY,
	channel TEXT NOT NULL,
	user_id TEXT,
	user_name TEXT,
	content TEXT,
	emotes TEXT,
	bits INTEGER DEFAULT 0,
	timestamp TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chat_channel_ts ON chat_messages(channel, timestamp);
`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}

	return &SQLiteStore{db: db, batchSize: batchSize, buf: make([]models.ChatMessage, 0, batchSize)}, nil
}

// Enqueue adds a message; flushes when the batch reaches BatchSize.
func (s *SQLiteStore) Enqueue(msg models.ChatMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buf = append(s.buf, msg)
	if len(s.buf) < s.batchSize {
		return nil
	}
	return s.flushLocked(context.Background())
}

// Flush writes any remaining buffered messages.
func (s *SQLiteStore) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked(ctx)
}

func (s *SQLiteStore) flushLocked(ctx context.Context) error {
	if len(s.buf) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.PrepareContext(ctx, `
INSERT OR IGNORE INTO chat_messages
	(id, channel, user_id, user_name, content, emotes, bits, timestamp)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, msg := range s.buf {
		emotesJSON, _ := json.Marshal(msg.Emotes)
		ts := msg.Timestamp.UTC().Format(time.RFC3339Nano)
		if _, err := stmt.ExecContext(ctx,
			msg.ID, msg.Channel, msg.UserID, msg.UserName, msg.Content,
			string(emotesJSON), msg.Bits, ts,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert message: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	s.buf = s.buf[:0]
	return nil
}

// Close flushes and closes the database.
func (s *SQLiteStore) Close() error {
	_ = s.Flush(context.Background())
	return s.db.Close()
}

// Count returns total persisted messages (for /status).
func (s *SQLiteStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM chat_messages`).Scan(&n)
	return n, err
}
