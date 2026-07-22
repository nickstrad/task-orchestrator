package cli

import (
	"fmt"

	"github.com/nickstrad/task-orchestrator/internal/store"
	"github.com/nickstrad/task-orchestrator/internal/worker"
	"github.com/spf13/cobra"
)

func workerCmd() *cobra.Command {
	var (
		host       string
		port       int
		name       string
		id         int
		dbType     string
		freshStart bool
	)

	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Start a worker node in the foreground",
		Long: `Start a worker: an HTTP server that accepts tasks and runs them as
Docker containers, plus a background loop that reports its stats to the manager.

The worker runs until you Ctrl-C it. On shutdown it stops every container it
still owns and closes its store, so a persistent store is left consistent for
the next start.

Use --db-type PERSISTENT to keep task state in a bbolt file (named after the
worker) across restarts; --fresh-start controls whether that file is wiped on
start.`,
		Example: `  # In-memory worker on the default port
  orchestrator worker --name worker-1 --port 5556

  # Persistent worker that keeps its task state across restarts
  orchestrator worker --name worker-1 --port 5556 --db-type PERSISTENT --fresh-start=false`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := newLogger()
			if err != nil {
				return err
			}
			addr := fmt.Sprintf("http://%s:%d", host, port)
			wLogger := logger.With("component", name, "workerID", id, "addr", addr)

			w, err := worker.NewWorker(name, id, wLogger, dbType, freshStart)
			if err != nil {
				return fmt.Errorf("creating worker: %w", err)
			}

			done := waitForShutdown(logger)
			go w.CollectStats(done)
			go w.RunTasks(done)

			api := worker.NewAPI(&w, host, port, wLogger)
			wLogger.Info("worker listening", "host", host, "port", port, "dbType", dbType)
			// Blocks until done is closed, then shuts the HTTP server down.
			api.Start(done)

			// Foreground shutdown: stop containers and close the store.
			w.Shutdown()
			return nil
		},
	}

	cmd.Flags().StringVar(&host, "host", "localhost", "host the worker binds its HTTP API to")
	cmd.Flags().IntVarP(&port, "port", "p", 5556, "port the worker binds its HTTP API to")
	cmd.Flags().StringVarP(&name, "name", "n", "worker-1", "worker name (the manager's routing key; must be unique per manager)")
	cmd.Flags().IntVar(&id, "id", 1, "numeric worker id, used only in logs")
	cmd.Flags().StringVar(&dbType, "db-type", store.InMemoryDb, dbTypeUsage)
	cmd.Flags().BoolVar(&freshStart, "fresh-start", true, "wipe the store on start (only meaningful for --db-type PERSISTENT)")
	return cmd
}
