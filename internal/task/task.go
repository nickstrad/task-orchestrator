package task

import (
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/network"
)

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

type StateMachine struct {
	machine map[State][]State
}

func NewStateMachine() StateMachine {
	return StateMachine{
		machine: map[State][]State{
			Pending:   {Scheduled},
			Scheduled: {Scheduled, Running, Failed},
			Running:   {Running, Completed, Failed, Scheduled},
			Completed: {},
			Failed:    {Scheduled},
		},
	}
}

func (s *StateMachine) IsValidTransition(cur, next State) bool {
	if validStates, exists := s.machine[cur]; exists && slices.Contains(validStates, next) {
		return true
	}

	return false
}

func (s State) String() string {
	switch s {
	case Pending:
		return "Pending"
	case Scheduled:
		return "Scheduled"
	case Running:
		return "Running"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	default:
		return ""
	}
}

type Task struct {
	Name          string
	ID            uuid.UUID
	State         State
	Image         string
	Memory        int
	Disk          int
	ExposedPorts  network.PortSet
	HostPorts     network.PortMap
	RestartPolicy string
	StartTime     time.Time
	FinishTime    time.Time
	ContainerID   string
	HealthCheck   string
	RestartCount  int
}

type Config struct {
	Name          string
	AttachStdin   bool
	AttachStdout  bool
	AttachStderr  bool
	ExposedPorts  network.PortSet
	ContainerID   string
	Cmd           []string
	Image         string
	Cpu           float64
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
}

type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}
