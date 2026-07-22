package cli

import (
	"fmt"
	"strings"

	"github.com/nickstrad/task-orchestrator/internal/manager"
	"github.com/nickstrad/task-orchestrator/internal/scheduler"
	"github.com/nickstrad/task-orchestrator/internal/store"
	"github.com/spf13/cobra"
)

func managerCmd() *cobra.Command {
	var (
		host          string
		port          int
		workerSpecs   []string
		schedulerType string
		dbType        string
	)

	cmd := &cobra.Command{
		Use:   "manager",
		Short: "Start the manager in the foreground",
		Long: `Start the manager: an HTTP server that accepts tasks, schedules them
across the workers you point it at, and runs background loops that poll worker
health and reconcile task state.

Point the manager at its workers with repeated --worker flags. Each value is a
worker's address, optionally prefixed with the worker's name:

  --worker worker-1=http://localhost:5556
  --worker http://localhost:5556          # auto-named worker-1, worker-2, ...

The name must match the worker's --name so health and stats line up in the
logs. The manager runs until you Ctrl-C it, then closes its store.`,
		Example: `  # Manager with two workers, marginal-cost scheduling
  orchestrator manager --port 5555 \
    --worker worker-1=http://localhost:5556 \
    --worker worker-2=http://localhost:5557

  # Round-robin scheduling with a persistent store
  orchestrator manager --scheduler round-robin --db-type PERSISTENT`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			workers, err := parseWorkers(workerSpecs)
			if err != nil {
				return err
			}

			logger, err := newLogger()
			if err != nil {
				return err
			}
			addr := fmt.Sprintf("http://%s:%d", host, port)
			mLogger := logger.With("component", "manager", "addr", addr)

			m, err := manager.NewManager(workers, schedulerType, mLogger, dbType)
			if err != nil {
				return fmt.Errorf("creating manager: %w", err)
			}

			done := waitForShutdown(logger)
			go m.UpdateTasks(done)
			go m.DoHealthChecks(done)
			go m.ProcessTasks(done)

			api := manager.NewAPI(host, port, m, mLogger)
			mLogger.Info("manager listening",
				"host", host, "port", port, "scheduler", schedulerType,
				"dbType", dbType, "workers", len(workers))
			// Blocks until done is closed, then shuts the HTTP server down.
			api.Start(done)

			if err := m.Close(); err != nil {
				mLogger.Error("closing manager stores failed", "err", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&host, "host", "localhost", "host the manager binds its HTTP API to")
	cmd.Flags().IntVarP(&port, "port", "p", 5555, "port the manager binds its HTTP API to")
	cmd.Flags().StringArrayVarP(&workerSpecs, "worker", "w", nil,
		"worker to manage, as [name=]address (repeatable)")
	cmd.Flags().StringVarP(&schedulerType, "scheduler", "s", scheduler.MarginalCost,
		fmt.Sprintf("scheduling strategy: %s or %s", scheduler.RoundRobin, scheduler.MarginalCost))
	cmd.Flags().StringVar(&dbType, "db-type", store.InMemoryDb, dbTypeUsage)
	return cmd
}

// parseWorkers turns the repeated --worker specs into WorkerMetadata. Each spec
// is "name=address" or a bare "address" (auto-named worker-N by position).
func parseWorkers(specs []string) ([]manager.WorkerMetadata, error) {
	workers := make([]manager.WorkerMetadata, 0, len(specs))
	for i, spec := range specs {
		name, addr, hasName := strings.Cut(spec, "=")
		if !hasName {
			name, addr = fmt.Sprintf("worker-%d", i+1), spec
		}
		if addr == "" {
			return nil, fmt.Errorf("--worker %q has an empty address", spec)
		}
		workers = append(workers, manager.WorkerMetadata{Name: name, Address: addr})
	}
	return workers, nil
}
