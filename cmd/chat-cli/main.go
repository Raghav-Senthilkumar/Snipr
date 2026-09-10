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

	"github.com/Raghav-Senthilkumar/Snipr/internal/auth"
	"github.com/Raghav-Senthilkumar/Snipr/internal/bus"
	"github.com/Raghav-Senthilkumar/Snipr/internal/detector"
	"github.com/Raghav-Senthilkumar/Snipr/internal/ingest"
	"github.com/Raghav-Senthilkumar/Snipr/internal/models"
	"github.com/Raghav-Senthilkumar/Snipr/internal/twitch"
)

func main() {
	// 0. Auto-load .env file if present
	_ = auth.LoadEnv(".env")

	// Configure structured logger
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelWarn, // Keep stderr clean so chat & alerts stand out
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("==================================================")
	fmt.Println("  SNIPR AUTO-CLIP & DETECTION ENGINE")
	fmt.Println("==================================================")

	// 1. Initialize TokenManager (auto-loads token.json, validates, refreshes, or logs in)
	tokenMgr := auth.NewTokenManager(auth.ManagerConfig{
		TokenFile: "token.json",
	}, nil)

	// Validate or refresh token immediately on boot
	_, err := tokenMgr.EnsureValidToken(ctx)
	if err != nil {
		fmt.Printf("⚠️  [TWITCH AUTH NOTICE] %v\n", err)
		fmt.Println("   Tip: Add TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET to a .env file to enable real Twitch clipping.")
	} else {
		user, _ := tokenMgr.GetUser()
		fmt.Printf("🔑 Twitch Authenticated as: %s (OAuth Verified)\n", user)
	}

	// 2. Initialize Helix Client with dynamic TokenProvider
	helixClient := twitch.NewHelixClient(tokenMgr.GetClientID(), "", nil)
	helixClient.SetTokenProvider(tokenMgr.GetValidAccessToken)

	// 2. Initialize embedded NATS JetStream Event Bus (In-Memory)
	eventBus, err := bus.NewNATSBus(bus.Config{
		Port:       -1, // ephemeral random port
		StreamName: "SNIPR_CHAT_STREAM",
		MaxAge:     5 * time.Minute,
		MaxBytes:   32 * 1024 * 1024, // 32MB RAM cap
	})
	if err != nil {
		fmt.Printf("Failed to initialize event bus: %v\n", err)
		os.Exit(1)
	}

	// 3. Initialize Heuristic Detection Engine
	engine := detector.NewEngine(detector.DefaultConfig())

	var messageCount uint64
	var showChat atomic.Bool
	showChat.Store(true) // Display chat by default

	// 4. Subscribe consumer to ALL channels via wildcard "chat.twitch.*"
	sub, err := eventBus.SubscribeChat("*", func(msg models.ChatMessage) {
		atomic.AddUint64(&messageCount, 1)

		// Sync timestamp to local clock if Twitch server clock drifts by more than 10 seconds
		now := time.Now().UTC()
		if msg.Timestamp.IsZero() || now.Sub(msg.Timestamp).Abs() > 10*time.Second {
			msg.Timestamp = now
		}

		// Feed message into detection engine
		engine.ProcessMessage(msg)

		// Only print if chat display is toggled on
		if showChat.Load() {
			emoteInfo := ""
			if len(msg.Emotes) > 0 {
				emoteInfo = fmt.Sprintf(" | Emotes: [%s]", strings.Join(msg.Emotes, ", "))
			}
			bitsInfo := ""
			if msg.Bits > 0 {
				bitsInfo = fmt.Sprintf(" | Bits: %d", msg.Bits)
			}

			// Print live message to console
			fmt.Printf("[%s] %s: %s%s%s\n",
				msg.Channel,
				msg.UserName,
				msg.Content,
				emoteInfo,
				bitsInfo,
			)
		}
	})
	if err != nil {
		fmt.Printf("Failed to subscribe consumer to chat stream: %v\n", err)
		eventBus.Close()
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	// 5. Connect Twitch IRC Ingestor
	// Note: We use anonymous guest mode for IRC ingestion because it is bulletproof,
	// never expires, and requires no IRC scopes. The OAuth token is reserved for Helix clipping!
	opts := ingest.ClientOptions{}

	client := ingest.NewClient(eventBus, opts)
	if err := client.Connect(ctx); err != nil {
		fmt.Printf("Failed to connect to Twitch IRC: %v\n", err)
		eventBus.Close()
		os.Exit(1)
	}

	// Start IRC read loop in background
	go client.Start(ctx)

	// 6. Pre-join 3 default streamers (or command-line args if provided)
	initialStreamers := []string{"tarik", "shroud", "caedrel"}
	if len(os.Args) > 1 {
		initialStreamers = os.Args[1:]
	}

	fmt.Printf("Monitoring %d channels: %v\n", len(initialStreamers), initialStreamers)
	fmt.Println("Commands:")
	fmt.Println("  /chat (or /mute)     -> Toggle showing live chat messages on/off")
	fmt.Println("  /clip <streamer>     -> Immediately create a real clip on Twitch")
	fmt.Println("  /status <streamer>   -> Inspect live Z-Score, velocity & baseline")
	fmt.Println("  /simulate <streamer> -> Inject synthetic hype burst to test trigger")
	fmt.Println("  /add <streamer>      -> Join a new channel live")
	fmt.Println("  /part <streamer>     -> Leave a channel")
	fmt.Println("  /list                -> List active channels")
	fmt.Println("  /stop                -> Gracefully stop engine and exit")
	fmt.Println("==================================================")

	for _, streamer := range initialStreamers {
		_ = client.Join(streamer)
	}

	// 7. Background Evaluator Ticker: Evaluates each channel once per second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				now := t.UTC()
				channels := client.GetChannels()
				for _, ch := range channels {
					event, triggered := engine.Evaluate(ch, now)
					if triggered {
						fmt.Println("\n========================================================")
						fmt.Printf("🔥 [CLIP TRIGGERED] Hype moment detected on #%s!\n", ch)
						fmt.Printf("   Reason:         %s\n", event.Reason)
						fmt.Printf("   Recent Rate:    %.1f msgs/sec\n", event.Stats.RecentVelocity)
						fmt.Printf("   Baseline Mean:  %.1f msgs/sec (StdDev: %.1f)\n", event.Stats.BaselineMean, event.Stats.BaselineStdDev)
						fmt.Printf("   Z-Score:        %.2f\n", event.Stats.ZScore)
						fmt.Printf("   Hype Emote %%:   %.0f%%\n", event.Stats.HypeRatio*100)
						fmt.Printf("   Unique Users:   %d (Diversity: %.0f%%)\n", event.Stats.UniqueChatters, event.Stats.ChatterDiversity*100)
						fmt.Printf("   ⏳ Waiting 8s capture delay (stream broadcast latency)...\n")
						fmt.Println("========================================================")

						// Trigger capture delay worker
						go func(ev *detector.TriggerEvent) {
							time.Sleep(8 * time.Second)

							if !helixClient.IsConfigured() {
								fmt.Println("\n--------------------------------------------------------")
								fmt.Printf("🎬 [CLIP CAPTURE] Simulated clip for #%s (No Helix API Token)\n", ev.Channel)
								fmt.Printf("   Simulated URL: https://clips.twitch.tv/simulated_%s_%d\n", ev.Channel, ev.TriggerTime.Unix())
								fmt.Printf("   Entering 60s cooldown for #%s.\n", ev.Channel)
								fmt.Println("--------------------------------------------------------")
								return
							}

							fmt.Println("\n--------------------------------------------------------")
							fmt.Printf("🎬 [CLIP CAPTURE] Calling Twitch Helix API for #%s...\n", ev.Channel)

							bID, err := helixClient.GetUserID(ctx, ev.Channel)
							if err != nil {
								fmt.Printf("❌ Failed to resolve broadcaster ID for #%s: %v\n", ev.Channel, err)
								fmt.Println("--------------------------------------------------------")
								return
							}

							clipResp, err := helixClient.CreateClip(ctx, bID)
							if err != nil {
								fmt.Printf("❌ Twitch Helix create clip failed: %v\n", err)
								fmt.Println("--------------------------------------------------------")
								return
							}

							clipURL := fmt.Sprintf("https://clips.twitch.tv/%s", clipResp.ID)
							fmt.Println("========================================================")
							fmt.Printf("🎉 [REAL CLIP CREATED ON TWITCH!]\n")
							fmt.Printf("   Streamer:  #%s (ID: %s)\n", ev.Channel, bID)
							fmt.Printf("   Clip ID:   %s\n", clipResp.ID)
							fmt.Printf("   👉 Watch:  %s\n", clipURL)
							fmt.Printf("   👉 Edit:   %s\n", clipResp.EditURL)
							fmt.Printf("   Cooldown:  60s active for #%s\n", ev.Channel)
							fmt.Println("========================================================")
						}(event)
					}
				}
			}
		}
	}()

	// 8. Setup OS signal handler for Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// 9. Interactive console loop in a separate goroutine
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
		case sig := <-sigCh:
			fmt.Printf("\nReceived signal %s. Shutting down...\n", sig)
			running = false

		case input := <-inputCh:
			if input == "" {
				continue
			}

			parts := strings.Fields(input)
			cmd := strings.ToLower(parts[0])

			switch cmd {
			case "/chat", "/mute", "/toggle":
				current := showChat.Load()
				showChat.Store(!current)
				if !current {
					fmt.Println(">> Chat display ENABLED. Raw messages will print.")
				} else {
					fmt.Println(">> Chat display MUTED. Live messages hidden; detection engine still running!")
				}

			case "/clip":
				if len(parts) < 2 {
					fmt.Println("Usage: /clip <channel>")
					continue
				}
				channel := parts[1]
				if !helixClient.IsConfigured() {
					fmt.Println("⚠️  Twitch Helix API is not configured. Run `go run cmd/auth-cli/main.go` to log in.")
					continue
				}

				fmt.Printf("🎬 Calling Twitch Helix API to clip #%s on demand...\n", channel)
				go func(ch string) {
					bID, err := helixClient.GetUserID(ctx, ch)
					if err != nil {
						fmt.Printf("❌ Failed to resolve broadcaster ID for #%s: %v\n", ch, err)
						return
					}
					clipResp, err := helixClient.CreateClip(ctx, bID)
					if err != nil {
						fmt.Printf("❌ Twitch create clip failed: %v\n", err)
						return
					}
					fmt.Printf("\n🎉 [REAL CLIP CREATED ON DEMAND!]\n   👉 Watch: https://clips.twitch.tv/%s\n   👉 Edit:  %s\n\n", clipResp.ID, clipResp.EditURL)
				}(channel)

			case "/add", "/join":
				if len(parts) < 2 {
					fmt.Println("Usage: /add <channel>")
					continue
				}
				channel := parts[1]
				if err := client.Join(channel); err != nil {
					fmt.Printf("Failed to join %s: %v\n", channel, err)
				} else {
					fmt.Printf("Successfully joined #%s!\n", channel)
				}

			case "/part", "/leave":
				if len(parts) < 2 {
					fmt.Println("Usage: /part <channel>")
					continue
				}
				channel := parts[1]
				if err := client.Part(channel); err != nil {
					fmt.Printf("Failed to part %s: %v\n", channel, err)
				} else {
					fmt.Printf("Left #%s.\n", channel)
				}

			case "/status":
				targets := client.GetChannels()
				if len(parts) >= 2 {
					targets = []string{strings.ToLower(strings.TrimPrefix(parts[1], "#"))}
				}

				if len(targets) == 0 {
					fmt.Println("No active channels being monitored. Use /add <channel> to join one.")
					continue
				}

				now := time.Now().UTC()
				fmt.Println("\n================= LIVE METRICS =================")
				for _, target := range targets {
					stats := engine.GetStats(target, now)
					inCD, cdRem := engine.IsInCooldown(target, now)
					fmt.Printf("[#%s]\n", target)
					fmt.Printf("  Recent 15s Msgs: %d (Velocity: %.1f msgs/sec)\n", stats.RecentMessages, stats.RecentVelocity)
					fmt.Printf("  Baseline Mean:   %.1f msgs/sec (StdDev: %.1f)\n", stats.BaselineMean, stats.BaselineStdDev)
					fmt.Printf("  Z-Score:         %.2f (Trigger threshold: 2.50)\n", stats.ZScore)
					fmt.Printf("  Hype Emote %%:    %.0f%%\n", stats.HypeRatio*100)
					fmt.Printf("  Unique Chatters: %d\n", stats.UniqueChatters)
					fmt.Printf("  In Cooldown:     %v\n", inCD)
					if inCD {
						fmt.Printf("  Cooldown Left:   %v\n", cdRem.Round(time.Second))
					}
					fmt.Println("------------------------------------------------")
				}

			case "/simulate", "/hype":
				target := "tarik"
				if len(parts) >= 2 {
					target = parts[1]
				}
				fmt.Printf("Injecting synthetic hype burst of 45 KEKW messages into #%s...\n", target)
				go func(ch string) {
					now := time.Now().UTC()
					for i := 0; i < 45; i++ {
						simMsg := models.ChatMessage{
							ID:        fmt.Sprintf("sim_%d_%d", time.Now().UnixNano(), i),
							Channel:   ch,
							UserID:    fmt.Sprintf("sim_user_%d", i%15),
							UserName:  fmt.Sprintf("SimFan%d", i%15),
							Content:   "KEKW THAT WAS SICK KEKW",
							Emotes:    []string{"KEKW", "KEKW"},
							Timestamp: now,
						}
						_ = eventBus.PublishChat(ctx, simMsg)
						time.Sleep(30 * time.Millisecond)
					}
				}(target)

			case "/list", "/channels":
				channels := client.GetChannels()
				fmt.Printf("Currently monitoring (%d channels): %s\n", len(channels), strings.Join(channels, ", "))

			case "/stop", "/quit", "/exit":
				fmt.Println("Stopping ingestion and closing event bus...")
				running = false

			default:
				fmt.Printf("Unknown command: %s (Commands: /chat, /clip, /status, /simulate, /add, /part, /list, /stop)\n", cmd)
			}
		}
	}

	// 10. Graceful Teardown: Stop client and close NATS bus
	_ = client.Close()
	eventBus.Close()

	total := atomic.LoadUint64(&messageCount)
	fmt.Printf("Done! Processed %d messages. Detection engine safely stopped.\n", total)
}
