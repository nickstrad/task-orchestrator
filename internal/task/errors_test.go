package task

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestErrorChainUnwrapsToCause(t *testing.T) {
	origin := E("task.Docker.Run", "pulling image busybox", io.EOF)
	layered := Wrap("task.Docker.Inspect", "inspecting container abc", origin)

	if !errors.Is(layered, io.EOF) {
		t.Errorf("errors.Is(layered, io.EOF) = false, want true")
	}

	var taskErr *TaskError
	if !errors.As(layered, &taskErr) {
		t.Fatalf("errors.As(layered, *TaskError) = false, want true")
	}
	if taskErr.Op != "task.Docker.Inspect" {
		t.Errorf("errors.As extracted Op = %q, want the outermost %q", taskErr.Op, "task.Docker.Inspect")
	}

	msg := layered.Error()
	for _, want := range []string{"task.Docker.Inspect", "task.Docker.Run", io.EOF.Error()} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, want it to contain %q", msg, want)
		}
	}
}

func TestOnlyOriginCapturesStack(t *testing.T) {
	origin := E("task.Docker.Run", "pulling image busybox", io.EOF)
	if origin.Stack == "" {
		t.Errorf("E().Stack is empty, want a captured stack")
	}

	layered := Wrap("task.Docker.Inspect", "inspecting container abc", origin)
	if layered.Stack != "" {
		t.Errorf("Wrap().Stack = %q, want empty (the origin already has one)", layered.Stack)
	}
}

func TestErrorWithoutCauseOmitsCause(t *testing.T) {
	err := E("task.NewDocker", "creating docker client", nil)
	if got, want := err.Error(), "task.NewDocker: creating docker client"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if err.Unwrap() != nil {
		t.Errorf("Unwrap() = %v, want nil", err.Unwrap())
	}
}
