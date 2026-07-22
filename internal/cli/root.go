// Package cli wires the orchestrator's pieces up behind a cobra CLI. Each
// command is a thin front door onto the internal/worker, internal/manager, and
// internal/task packages — the CLI parses flags, builds a logger, and blocks in
// the foreground until Ctrl-C. Nothing here daemonizes or spawns a detached
// process; `worker` and `manager` are meant to hold a terminal.
package cli

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

// logLevel is bound to the root's persistent --log-level flag and read by
// newLogger when a command builds its logger.
var logLevel string

func rootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "orchestrator",
		Short: "Run and drive a small task orchestrator",
		Long: `orchestrator runs the pieces of the task orchestrator and lets you
drive tasks against a running manager.

A typical local setup is three terminals:

  1. orchestrator worker  --port 5556 --name worker-1
  2. orchestrator manager --port 5555 --worker worker-1=http://localhost:5556
  3. orchestrator task start -f examples/task-event.json

worker and manager run in the foreground and log to stderr until you Ctrl-C
them. The task subcommands are one-shot HTTP clients against a manager (or a
worker) that exit as soon as the call returns.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	cmd.PersistentFlags().StringVar(&logLevel, "log-level", "info",
		"log verbosity: debug, info, warn, or error")

	// worker/manager are the long-running servers; task groups the client
	// verbs. run/stop/status are top-level shortcuts for the common task verbs
	// so you don't have to type "task" first; stats reads a worker's metrics.
	cmd.AddCommand(
		workerCmd(),
		managerCmd(),
		taskCmd(),
		newStartCommand("run"),
		newStopCommand("stop"),
		newStatusCommand("status"),
		statsCmd(),
	)
	return cmd
}

// Execute builds the command tree and runs it. main() calls this and turns a
// non-nil error into a non-zero exit code.
func Execute() error {
	return rootCmd().Execute()
}

// newLogger builds the root logger for a command from the --log-level flag. It
// mirrors main.go: a text handler on stderr so logs never pollute the stdout a
// task command prints its results to.
func newLogger() (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("invalid --log-level %q: want debug, info, warn, or error", logLevel)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})), nil
}

// waitForShutdown returns a channel that is closed on the first SIGINT/SIGTERM.
// It is the single shutdown signal every long-running command selects on, so a
// Ctrl-C in the terminal unwinds the servers and background loops cleanly
// instead of killing them mid-write.
func waitForShutdown(logger *slog.Logger) <-chan struct{} {
	done := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sig
		logger.Info("received signal, shutting down", "signal", s)
		close(done)
	}()
	return done
}
