package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/arkiv-events/tariterator"
	"github.com/Arkiv-Network/sqlite-bitmap-store/pebblestore"
	"github.com/urfave/cli/v2"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		dbPath    string
		pprofAddr string
	}{}

	app := &cli.App{
		Name:  "load-from-tar",
		Usage: "Load data from a node into a PebbleDB database",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "db-path",
				Value:       "arkiv-data.db",
				Destination: &cfg.dbPath,
				EnvVars:     []string{"DB_PATH"},
			},
			&cli.StringFlag{
				Name:        "pprof-addr",
				Value:       "localhost:6060",
				Destination: &cfg.pprofAddr,
				EnvVars:     []string{"PPROF_ADDR"},
			},
		},
		Action: func(c *cli.Context) error {

			go func() {
				logger.Info("starting pprof server", "addr", cfg.pprofAddr)
				if err := http.ListenAndServe(cfg.pprofAddr, nil); err != nil {
					logger.Error("pprof server failed", "error", err)
				}
			}()

			tarFileName := c.Args().First()

			if tarFileName == "" {
				return fmt.Errorf("tar file is required")
			}

			tarFile, err := os.Open(tarFileName)
			if err != nil {
				return fmt.Errorf("failed to open tar file: %w", err)
			}
			defer tarFile.Close()

			store, err := pebblestore.NewPebbleStore(logger, cfg.dbPath)
			if err != nil {
				return fmt.Errorf("failed to create PebbleDB store: %w", err)
			}
			defer store.Close()

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			iterator := tariterator.IterateTar(200, tarFile)

			err = store.FollowEvents(ctx, arkivevents.BatchIterator(iterator))
			if err != nil {
				return fmt.Errorf("failed to follow events: %w", err)
			}

			return nil

		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
