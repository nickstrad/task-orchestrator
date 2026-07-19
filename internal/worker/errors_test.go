package worker

import (
	"errors"
	"io"
	"testing"
)

func TestErrorChainUnwrapsToCause(t *testing.T) {
	origin := E("worker.StartTask", "creating docker handler", io.EOF)
	layered := Wrap("worker.RunTask", "running task", origin)

	if !errors.Is(layered, io.EOF) {
		t.Errorf("errors.Is(layered, io.EOF) = false, want true")
	}

	var workerErr *WorkerError
	if !errors.As(layered, &workerErr) {
		t.Fatalf("errors.As(layered, *WorkerError) = false, want true")
	}

	if origin.Stack == "" {
		t.Errorf("E().Stack is empty, want a captured stack")
	}
	if layered.Stack != "" {
		t.Errorf("Wrap().Stack = %q, want empty", layered.Stack)
	}
}
