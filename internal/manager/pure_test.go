package manager

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/network"
	"github.com/nickstrad/task-orchestrator/internal/task"
)

func TestWorkerTasksURL(t *testing.T) {
	got := workerTasksURL("http://localhost:3001")
	want := "http://localhost:3001/tasks"
	if got != want {
		t.Errorf("workerTasksURL() = %q, want %q", got, want)
	}
}

func TestHealthCheckURL(t *testing.T) {
	tests := []struct {
		name       string
		workerAddr string
		hostPort   string
		healthPath string
		want       string
		wantErr    bool
	}{
		{
			name:       "replaces the worker port with the container host port",
			workerAddr: "http://localhost:3001",
			hostPort:   "32768",
			healthPath: "/health",
			want:       "http://localhost:32768/health",
		},
		{
			name:       "worker address without a port still works",
			workerAddr: "http://worker-1.internal",
			hostPort:   "32768",
			healthPath: "/health",
			want:       "http://worker-1.internal:32768/health",
		},
		{
			name:       "preserves the scheme",
			workerAddr: "https://localhost:3001",
			hostPort:   "32768",
			healthPath: "/health",
			want:       "https://localhost:32768/health",
		},
		{
			name:       "empty health path yields a bare root URL",
			workerAddr: "http://localhost:3001",
			hostPort:   "32768",
			healthPath: "",
			want:       "http://localhost:32768",
		},
		{
			name:       "empty worker address is rejected",
			workerAddr: "",
			hostPort:   "32768",
			healthPath: "/health",
			wantErr:    true,
		},
		{
			name:       "address without a scheme is rejected",
			workerAddr: "localhost:3001",
			hostPort:   "32768",
			healthPath: "/health",
			wantErr:    true,
		},
		{
			name:       "empty host port is rejected",
			workerAddr: "http://localhost:3001",
			hostPort:   "",
			healthPath: "/health",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := healthCheckURL(tt.workerAddr, tt.hostPort, tt.healthPath)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("healthCheckURL(%q, %q, %q) = %q, want error",
						tt.workerAddr, tt.hostPort, tt.healthPath, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("healthCheckURL(%q, %q, %q) returned unexpected error: %v",
					tt.workerAddr, tt.hostPort, tt.healthPath, err)
			}
			if got != tt.want {
				t.Errorf("healthCheckURL(%q, %q, %q) = %q, want %q",
					tt.workerAddr, tt.hostPort, tt.healthPath, got, tt.want)
			}
		})
	}
}

// The doubled port was a real bug that took several passes to fix, so pin the
// exact shape it produced.
func TestHealthCheckURLDoesNotDoubleThePort(t *testing.T) {
	got, err := healthCheckURL("http://localhost:3001", "32768", "/health")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == "http://localhost:3001:32768/health" {
		t.Errorf("healthCheckURL() = %q, worker port leaked into the URL", got)
	}
}

func TestMergeTaskUpdateCopiesWorkerOwnedFields(t *testing.T) {
	start := time.Now().UTC()
	finish := start.Add(time.Minute)
	ports := network.PortMap{
		network.MustParsePort("80/tcp"): []network.PortBinding{{HostPort: "32768"}},
	}
	exposed := network.PortSet{network.MustParsePort("80/tcp"): struct{}{}}

	persisted := task.Task{
		ID:    uuid.New(),
		Name:  "test-task",
		State: task.Scheduled,
	}
	incoming := task.Task{
		State:        task.Running,
		StartTime:    start,
		FinishTime:   finish,
		ContainerID:  "abc123",
		ExposedPorts: exposed,
		HostPorts:    ports,
	}

	got := mergeTaskUpdate(persisted, incoming)

	if got.State != task.Running {
		t.Errorf("State = %v, want %v", got.State, task.Running)
	}
	if !got.StartTime.Equal(start) {
		t.Errorf("StartTime = %v, want %v", got.StartTime, start)
	}
	if !got.FinishTime.Equal(finish) {
		t.Errorf("FinishTime = %v, want %v", got.FinishTime, finish)
	}
	if got.ContainerID != "abc123" {
		t.Errorf("ContainerID = %q, want %q", got.ContainerID, "abc123")
	}
	// HostPorts was the field the original merge forgot, which left the manager
	// unable to health check anything.
	if _, ok := getHostPort(got.HostPorts); !ok {
		t.Errorf("HostPorts = %v, want a published port", got.HostPorts)
	}
	if got.ExposedPorts == nil {
		t.Errorf("ExposedPorts = nil, want %v", exposed)
	}
}

func TestMergeTaskUpdatePreservesManagerOwnedFields(t *testing.T) {
	id := uuid.New()
	persisted := task.Task{
		ID:           id,
		Name:         "test-task",
		HealthCheck:  "/health",
		RestartCount: 2,
	}
	// A worker response carries zero values for fields the worker does not own.
	incoming := task.Task{State: task.Running}

	got := mergeTaskUpdate(persisted, incoming)

	if got.RestartCount != 2 {
		t.Errorf("RestartCount = %d, want 2 (worker must not reset it)", got.RestartCount)
	}
	if got.HealthCheck != "/health" {
		t.Errorf("HealthCheck = %q, want %q", got.HealthCheck, "/health")
	}
	if got.ID != id {
		t.Errorf("ID = %v, want %v", got.ID, id)
	}
	if got.Name != "test-task" {
		t.Errorf("Name = %q, want %q", got.Name, "test-task")
	}
}

func TestMergeTaskUpdateDoesNotMutateInputs(t *testing.T) {
	persisted := task.Task{State: task.Scheduled, RestartCount: 1}
	incoming := task.Task{State: task.Running, ContainerID: "abc123"}

	mergeTaskUpdate(persisted, incoming)

	if persisted.State != task.Scheduled {
		t.Errorf("persisted.State = %v, want %v (arg was mutated)", persisted.State, task.Scheduled)
	}
	if incoming.ContainerID != "abc123" {
		t.Errorf("incoming.ContainerID = %q, want unchanged", incoming.ContainerID)
	}
}

func TestDecideHealthAction(t *testing.T) {
	tests := []struct {
		name string
		task task.Task
		want healthAction
	}{
		{
			name: "running task is probed",
			task: task.Task{State: task.Running},
			want: healthActionCheck,
		},
		{
			name: "failed task is restarted without probing",
			task: task.Task{State: task.Failed},
			want: healthActionRestart,
		},
		{
			name: "pending task is skipped",
			task: task.Task{State: task.Pending},
			want: healthSkipNotEligible,
		},
		{
			name: "scheduled task is skipped",
			task: task.Task{State: task.Scheduled},
			want: healthSkipNotEligible,
		},
		{
			name: "completed task is skipped",
			task: task.Task{State: task.Completed},
			want: healthSkipNotEligible,
		},
		{
			name: "restart budget stops a running task",
			task: task.Task{State: task.Running, RestartCount: TaskRestartMax},
			want: healthSkipRestartMax,
		},
		{
			name: "restart budget stops a failed task",
			task: task.Task{State: task.Failed, RestartCount: TaskRestartMax},
			want: healthSkipRestartMax,
		},
		{
			name: "one restart below the max still runs",
			task: task.Task{State: task.Running, RestartCount: TaskRestartMax - 1},
			want: healthActionCheck,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decideHealthAction(tt.task); got != tt.want {
				t.Errorf("decideHealthAction(%v, restarts=%d) = %v, want %v",
					tt.task.State, tt.task.RestartCount, got, tt.want)
			}
		})
	}
}

func TestGetHostPort(t *testing.T) {
	t.Run("returns the published port", func(t *testing.T) {
		ports := network.PortMap{
			network.MustParsePort("80/tcp"): []network.PortBinding{{HostPort: "32768"}},
		}
		got, ok := getHostPort(ports)
		if !ok {
			t.Fatalf("getHostPort() ok = false, want true")
		}
		if got != "32768" {
			t.Errorf("getHostPort() = %q, want %q", got, "32768")
		}
	})

	t.Run("reports missing for an empty map", func(t *testing.T) {
		if got, ok := getHostPort(network.PortMap{}); ok {
			t.Errorf("getHostPort(empty) = %q, true; want \"\", false", got)
		}
	})

	t.Run("reports missing for a nil map", func(t *testing.T) {
		if got, ok := getHostPort(nil); ok {
			t.Errorf("getHostPort(nil) = %q, true; want \"\", false", got)
		}
	})
}
