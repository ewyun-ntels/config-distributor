package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"ntels.com/upm/cfg-distributor/internal/apiserver"
	"ntels.com/upm/cfg-distributor/internal/config"
)

func main() {
	configPath := config.DefaultConfigPath
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	logLevel, err := cfg.SlogLevel()
	if err != nil {
		slog.Error("load log level", "err", err)
		os.Exit(1)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	})))
	slog.Debug("logging configured", "level", cfg.Log.Level)

	srv, err := apiserver.New(cfg)
	if err != nil {
		slog.Error("init apiserver", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		slog.Error("run apiserver", "err", err)
		os.Exit(1)
	}
}
