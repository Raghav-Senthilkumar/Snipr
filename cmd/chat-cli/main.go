package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Raghav-Senthilkumar/Snipr/internal/analytics"
	"github.com/Raghav-Senthilkumar/Snipr/internal/auth"
	"github.com/Raghav-Senthilkumar/Snipr/internal/bus"
	"github.com/Raghav-Senthilkumar/Snipr/internal/features"
	"github.com/Raghav-Senthilkumar/Snipr/internal/ingest"
	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
	"github.com/Raghav-Senthilkumar/Snipr/internal/pipeline"
	"github.com/Raghav-Senthilkumar/Snipr/internal/predict"
	"github.com/Raghav-Senthilkumar/Snipr/internal/store"
	"github.com/Raghav-Senthilkumar/Snipr/internal/twitch"
)

func main() {
	_ = auth.LoadEnv(".env")

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("==================================================")
	fmt.Println("  SNIPR — ValSparks-style pipeline (Go + NATS)")
	fmt.Println("  IRC → NATS → [SQLite | Features | ES] → ML → Clip")
	fmt.Println("==================================================")

	// Auth + Helix
	tokenMgr := auth.NewTokenManager(auth.ManagerConfig{TokenFile: "token.json"}, nil)
	if _, err := tokenMgr.EnsureValidToken(ctx); err != nil {
		fmt.Printf("WARN: [auth] %v\n", err)
		fmt.Println("   Tip: TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET in .env; run auth-cli to log in.")
	} else {
		user, _ := tokenMgr.GetUser()
		fmt.Printf("Authenticated as: %s\n", user)
	}

	helix := twitch.NewHelixClient(tokenMgr.GetClientID(), "", nil)
	helix.SetTokenProvider(tokenMgr.GetValidAccessToken)

	// SQLite (ValSparks Postgres equivalent)
	dbPath := os.Getenv("SNIPR_SQLITE_PATH")
	if dbPath == "" {
		dbPath = "snipr.db"
	}
	sqliteStore, err := store.Open(dbPath, store.DefaultBatchSize)
	if err != nil {
		fmt.Printf("Failed to open sqlite: %v\n", err)
		os.Exit(1)
	}
	defer sqliteStore.Close()
	fmt.Printf("SQLite: %s (flush every %d messages)\n", dbPath, store.DefaultBatchSize)

	// Elasticsearch
	elasticCfg := analytics.DefaultConfig()
	if esURL := os.Getenv("ELASTICSEARCH_URL"); esURL != "" {
		elasticCfg.URL = esURL
	}
	elasticIndexer, err := analytics.NewElasticIndexer(elasticCfg, nil)
	if err != nil {
		fmt.Printf("WARN: [elasticsearch] init: %v\n", err)
	}
	defer func() {
		if elasticIndexer != nil {
			_ = elasticIndexer.Close()
		}
	}()

	elasticOnline := false
	if elasticIndexer != nil {
		pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
		if err := elasticIndexer.Ping(pingCtx); err != nil {
			fmt.Printf("WARN: [elasticsearch] offline: %v\n", err)
			fmt.Println("   Tip: docker compose up -d")
		} else if err := elasticIndexer.InitIndices(ctx); err != nil {
			fmt.Printf("WARN: [elasticsearch] mapping: %v\n", err)
		} else {
			elasticOnline = true
			fmt.Printf("Elasticsearch: %s\n", elasticCfg.URL)
		}
		pingCancel()
	}

	// ML model (drop dump_model JSON at models/mymodel.json)
	modelPath := os.Getenv("SNIPR_MODEL_PATH")
	if modelPath == "" {
		modelPath = predict.DefaultModelPath
	}
	predictor, err := predict.LoadXGB(modelPath)
	if err != nil {
		fmt.Printf("WARN: [ml] load error: %v — continuing without clips\n", err)
		predictor = predict.NewNoop(err.Error())
	} else if predictor.Ready() {
		fmt.Printf("ML model loaded: %s (threshold %.1f)\n", modelPath, predict.ProbThreshold)
	} else if n, ok := predictor.(*predict.NoopPredictor); ok {
		fmt.Printf("ML model: not ready (%s)\n", n.Reason())
	}

	// Embedded NATS (chat + features streams)
	eventBus, err := bus.NewNATSBus(bus.Config{Port: -1})
	if err != nil {
		fmt.Printf("Failed to start NATS: %v\n", err)
		os.Exit(1)
	}
	defer eventBus.Close()
	fmt.Println("NATS: chat.twitch.* + features.twitch.*")

	monitor := features.NewMonitor(eventBus)

	var messageCount uint64
	var showChat atomic.Bool
	showChat.Store(true)

	consumers, err := pipeline.Start(ctx, pipeline.Options{
		Bus:           eventBus,
		Store:         sqliteStore,
		Monitor:       monitor,
		Elastic:       elasticIndexer,
		ElasticOnline: elasticOnline,
		Helix:         helix,
		Predictor:     predictor,
		ShowChat:      &showChat,
		MessageCount:  &messageCount,
	})
	if err != nil {
		fmt.Printf("Failed to start consumers: %v\n", err)
		os.Exit(1)
	}
	defer consumers.Stop()
	fmt.Println("Consumers: sqlite | features(24s) | elasticsearch | predict→clip")

	// IRC ingest
	client := ingest.NewClient(eventBus, ingest.ClientOptions{})
	if err := client.Connect(ctx); err != nil {
		fmt.Printf("Failed to connect IRC: %v\n", err)
		os.Exit(1)
	}
	go client.Start(ctx)

	channels := []string{"tarik", "shroud", "caedrel"}
	if len(os.Args) > 1 {
		channels = os.Args[1:]
	}
	for _, ch := range channels {
		_ = client.Join(ch)
	}

	fmt.Printf("Monitoring: %v\n", channels)
	fmt.Println("Commands: /chat /clip /status /simulate /elastic /db /add /part /list /stop")
	fmt.Println("==================================================")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	inputCh := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			inputCh <- strings.TrimSpace(scanner.Text())
		}
	}()

	running := true
	for running {
		select {
		case <-sigCh:
			fmt.Println("\nShutting down...")
			running = false

		case input := <-inputCh:
			if input == "" {
				continue
			}
			parts := strings.Fields(input)
			cmd := strings.ToLower(parts[0])

			switch cmd {
			case "/chat", "/mute", "/toggle":
				showChat.Store(!showChat.Load())
				if showChat.Load() {
					fmt.Println(">> Chat display ON")
				} else {
					fmt.Println(">> Chat display OFF (pipeline still running)")
				}

			case "/clip":
				if len(parts) < 2 {
					fmt.Println("Usage: /clip <channel>")
					continue
				}
				ch := parts[1]
				if !helix.IsConfigured() {
					fmt.Println("WARN: Helix not configured — run auth-cli first")
					continue
				}
				go func(channel string) {
					bID, err := helix.GetUserID(ctx, channel)
					if err != nil {
						fmt.Printf("ERROR: resolve #%s: %v\n", channel, err)
						return
					}
					resp, err := helix.CreateClip(ctx, bID)
					if err != nil {
						fmt.Printf("ERROR: clip #%s: %v\n", channel, err)
						return
					}
					fmt.Printf("Manual clip: https://clips.twitch.tv/%s\n   Edit: %s\n", resp.ID, resp.EditURL)
				}(ch)

			case "/status":
				targets := client.GetChannels()
				if len(parts) >= 2 {
					targets = []string{strings.ToLower(strings.TrimPrefix(parts[1], "#"))}
				}
				now := time.Now().UTC()
				fmt.Println("\n================= STATUS =================")
				fmt.Printf("  Model ready: %v\n", predictor.Ready())
				fmt.Printf("  Messages seen: %d\n", atomic.LoadUint64(&messageCount))
				for _, t := range targets {
					inCD, rem := consumers.IsInCooldown(t, now)
					probStr := "n/a"
					if v, ok := consumers.LastProb.Load(t); ok {
						probStr = fmt.Sprintf("%.3f", v.(float64))
					}
					fmt.Printf("  #%s  last_prob=%s  cooldown=%v", t, probStr, inCD)
					if inCD {
						fmt.Printf(" (%s left)", rem.Round(time.Second))
					}
					fmt.Println()
				}
				fmt.Println("==========================================")

			case "/simulate", "/hype":
				target := "tarik"
				if len(parts) >= 2 {
					target = parts[1]
				}
				fmt.Printf("Injecting hype burst into #%s...\n", target)
				go func(ch string) {
					now := time.Now().UTC()
					for i := 0; i < 40; i++ {
						_ = eventBus.PublishChat(ctx, models.ChatMessage{
							ID:        fmt.Sprintf("sim_%d_%d", now.UnixNano(), i),
							Channel:   ch,
							UserID:    fmt.Sprintf("sim_user_%d", i%12),
							UserName:  fmt.Sprintf("SimFan%d", i%12),
							Content:   "omg wtf holy wow insane kekw lmao cinema clean",
							Timestamp: now,
						})
						time.Sleep(20 * time.Millisecond)
					}
				}(target)

			case "/elastic", "/es":
				if !elasticOnline || elasticIndexer == nil {
					fmt.Println("Elasticsearch offline")
					continue
				}
				stats := elasticIndexer.GetStats()
				fmt.Printf("ES chat=%d clips=%d bulk=%d failed=%d\n",
					stats.ChatIndexed, stats.ClipsIndexed, stats.BulkRequests, stats.FailedDocs)

			case "/db", "/sqlite":
				n, err := sqliteStore.Count(ctx)
				if err != nil {
					fmt.Printf("sqlite count error: %v\n", err)
				} else {
					fmt.Printf("SQLite rows: %d (%s)\n", n, dbPath)
				}

			case "/add", "/join":
				if len(parts) < 2 {
					fmt.Println("Usage: /add <channel>")
					continue
				}
				if err := client.Join(parts[1]); err != nil {
					fmt.Printf("join failed: %v\n", err)
				} else {
					fmt.Printf("Joined #%s\n", parts[1])
				}

			case "/part", "/leave":
				if len(parts) < 2 {
					fmt.Println("Usage: /part <channel>")
					continue
				}
				if err := client.Part(parts[1]); err != nil {
					fmt.Printf("part failed: %v\n", err)
				} else {
					fmt.Printf("Left #%s\n", parts[1])
				}

			case "/list", "/channels":
				fmt.Printf("Channels: %s\n", strings.Join(client.GetChannels(), ", "))

			case "/stop", "/quit", "/exit":
				running = false

			default:
				fmt.Printf("Unknown: %s\n", cmd)
			}
		}
	}

	_ = client.Close()
	_ = sqliteStore.Flush(ctx)
	fmt.Printf("Done. Processed %d messages.\n", atomic.LoadUint64(&messageCount))
}
