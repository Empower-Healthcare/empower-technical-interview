// migrate applies the embedded SQL migrations (db/migrations) and exits.
// Compose runs it once before the application services start.
package main

import (
	"context"

	"github.com/empower-healthcare/parcellab/internal/app"
	"github.com/empower-healthcare/parcellab/internal/storage"
)

func main() { app.Main("migrate", run) }

func run(ctx context.Context, a app.App) error {
	dsn := a.Env.DatabaseURL()
	if err := a.Env.Err(); err != nil {
		return err
	}

	_, pool, err := a.OpenStore(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := storage.Migrate(ctx, pool, a.Logger); err != nil {
		return err
	}
	a.Logger.InfoContext(ctx, "schema up to date")
	return nil
}
