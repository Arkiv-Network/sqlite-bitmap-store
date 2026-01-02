package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	sqlitestore "github.com/Arkiv-Network/sqlite-bitmap-store"
	"github.com/Arkiv-Network/sqlite-bitmap-store/query"
	"github.com/urfave/cli/v2"
)

func main() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		dbPath string
	}{}

	app := &cli.App{
		Name:  "query",
		Usage: "Query the SQLite database",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "db-path",
				Value:       "arkiv-data.db",
				Destination: &cfg.dbPath,
				EnvVars:     []string{"DB_PATH"},
			},
		},
		Action: func(c *cli.Context) error {

			queryString := c.Args().First()

			if queryString == "" {
				return fmt.Errorf("query is required")
			}

			st, err := sqlitestore.NewSQLiteStore(logger, cfg.dbPath, 7)
			if err != nil {
				return fmt.Errorf("failed to create SQLite store: %w", err)
			}
			defer st.Close()

			q, err := query.Parse(queryString)
			if err != nil {
				return fmt.Errorf("failed to parse query: %w", err)
			}

			bitmap, err := q.Evaluate(context.Background(), *st.NewQueries())
			if err != nil {
				return fmt.Errorf("failed to evaluate query: %w", err)
			}

			fmt.Println(bitmap.GetCardinality())

			return nil

		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
