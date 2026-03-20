package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"ntels.com/upm/cfg-distributor/internal/apiserver"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	srv, err := apiserver.New()
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
