// Command orchestrator is the CLI front door to the task orchestrator: it
// starts a worker or manager in the foreground and drives tasks against a
// running manager. The wiring lives in internal/cli; this is just the
// entrypoint that turns a command error into a non-zero exit code.
package main

import (
	"fmt"
	"os"

	"github.com/nickstrad/task-orchestrator/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
