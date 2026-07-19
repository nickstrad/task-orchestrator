package manager

import (
	"fmt"
	neturl "net/url"

	"github.com/moby/moby/api/types/network"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

// This file holds the manager's pure functions: no receiver, no I/O, no
// logging, no clock. Everything here is a value in, a value out, which is what
// makes it directly testable without standing up a Manager or a fake worker.

// workerTasksURL builds the /tasks endpoint for a worker's base address.
func workerTasksURL(workerAddr string) string {
	return fmt.Sprintf("%s/tasks", workerAddr)
}

// healthCheckURL builds the URL for a task's health check endpoint.
//
// The worker's address carries the worker's own port (e.g. http://localhost:3001),
// but the health check must reach the *container*, which Docker published on a
// different host port. So the scheme and hostname come from the worker address
// while the port comes from the container — using the address verbatim would
// produce a doubled port like http://localhost:3001:32768/health.
func healthCheckURL(workerAddr, hostPort, healthCheckPath string) (string, error) {
	parsed, err := neturl.Parse(workerAddr)
	if err != nil {
		return "", fmt.Errorf("parsing worker address %q: %w", workerAddr, err)
	}

	// url.Parse accepts almost anything, including "" and bare hostnames, so a
	// nil error is not enough to know the address was usable.
	if parsed.Scheme == "" || parsed.Hostname() == "" {
		return "", fmt.Errorf("worker address %q has no scheme or host", workerAddr)
	}
	if hostPort == "" {
		return "", fmt.Errorf("no host port for worker address %q", workerAddr)
	}

	return fmt.Sprintf("%s://%s:%s%s", parsed.Scheme, parsed.Hostname(), hostPort, healthCheckPath), nil
}

// mergeTaskUpdate returns persisted with the fields the worker owns copied over
// from incoming. The manager owns everything else — notably RestartCount, which
// the worker never sees and must not clobber.
func mergeTaskUpdate(persisted, incoming task.Task) task.Task {
	persisted.State = incoming.State
	persisted.StartTime = incoming.StartTime
	persisted.FinishTime = incoming.FinishTime
	persisted.ContainerID = incoming.ContainerID
	persisted.ExposedPorts = incoming.ExposedPorts
	persisted.HostPorts = incoming.HostPorts
	return persisted
}

// healthAction is what doHealthChecks should do with a task this round.
type healthAction int

const (
	// healthSkipNotEligible: the task is not in a state health checks apply to.
	healthSkipNotEligible healthAction = iota
	// healthSkipRestartMax: the task has already been restarted too many times.
	healthSkipRestartMax
	// healthActionCheck: the task claims to be running, so probe it.
	healthActionCheck
	// healthActionRestart: the task is known failed, so restart without probing.
	healthActionRestart
)

// decideHealthAction picks the action for a task without performing it, so the
// branching can be tested without a live worker or HTTP server.
func decideHealthAction(t task.Task) healthAction {
	if t.State != task.Running && t.State != task.Failed {
		return healthSkipNotEligible
	}
	if t.RestartCount >= TaskRestartMax {
		return healthSkipRestartMax
	}
	if t.State == task.Running {
		return healthActionCheck
	}
	return healthActionRestart
}

// getHostPort returns any one published host port for a task.
//
// NOTE: PortMap is a map, so when a container publishes more than one port the
// choice here is whichever key Go's randomized iteration yields first. That is
// pre-existing behavior, left as-is.
func getHostPort(ports network.PortMap) (string, bool) {
	for k := range ports {
		return ports[k][0].HostPort, true
	}
	return "", false
}
