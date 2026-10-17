package main

import (
	"context"
	"fmt"

	"github.com/rakunlabs/into"
	"github.com/rakunlabs/logi"

	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/rakunlabs/kafka-console/internal/kafka"
	"github.com/rakunlabs/kafka-console/internal/server"
)

var (
	version = "v0.0.0"
	commit  = "-"
	date    = "-"
)

func main() {
	config.Version = version

	into.Init(run,
		into.WithLogger(logi.InitializeLog(logi.WithCaller(false))),
		into.WithMsgf("%s version:[%s] commit:[%s] date:[%s]", config.ServiceName, version, commit, date),
	)
}

func run(ctx context.Context) error {
	cfg, err := config.Load(ctx)
	if err != nil {
		return fmt.Errorf("load config; %w", err)
	}

	registry, err := kafka.NewRegistry(cfg)
	if err != nil {
		return fmt.Errorf("connect clusters; %w", err)
	}
	defer registry.Close()

	ui, err := server.UI()
	if err != nil {
		return fmt.Errorf("load ui; %w", err)
	}

	if err := server.Start(ctx, cfg, registry, ui); err != nil {
		return fmt.Errorf("start server; %w", err)
	}

	return nil
}
