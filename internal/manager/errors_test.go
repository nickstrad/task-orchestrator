package manager

import (
	"errors"
	"io"
	"testing"
)

func TestErrorChainUnwrapsToCause(t *testing.T) {
	origin := E("manager.SendWork", "connecting to worker worker-0", io.EOF)
	layered := Wrap("manager.ProcessTasks", "processing pending tasks", origin)

	if !errors.Is(layered, io.EOF) {
		t.Errorf("errors.Is(layered, io.EOF) = false, want true")
	}

	var managerErr *ManagerError
	if !errors.As(layered, &managerErr) {
		t.Fatalf("errors.As(layered, *ManagerError) = false, want true")
	}

	if origin.Stack == "" {
		t.Errorf("E().Stack is empty, want a captured stack")
	}
	if layered.Stack != "" {
		t.Errorf("Wrap().Stack = %q, want empty", layered.Stack)
	}
}
