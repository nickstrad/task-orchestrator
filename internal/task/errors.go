package task

import "runtime/debug"

// TaskError is this package's error type. The same ~30-line pattern lives in
// worker/ and manager/ (as WorkerError/ManagerError) — the duplication keeps
// each package self-contained, and the name says where an error came from.
type TaskError struct {
	Op      string // e.g. "task.Docker.Run"
	Message string // user-friendly
	Err     error  // wrapped cause (nil at an origin with no cause)
	Stack   string // captured ONLY at the origin
}

func (e *TaskError) Error() string {
	if e.Err != nil {
		return e.Op + ": " + e.Message + ": " + e.Err.Error()
	}
	return e.Op + ": " + e.Message
}

func (e *TaskError) Unwrap() error { return e.Err }

// E mints an ORIGIN error: the cause is nil, or it came from outside this
// codebase (the Docker SDK, net/http, encoding/json). It captures the stack.
func E(op, message string, err error) *TaskError {
	return &TaskError{Op: op, Message: message, Err: err, Stack: string(debug.Stack())}
}

// Wrap adds context to an error that already carries a stack, so it does not
// capture a second one. Use E at the boundary, Wrap everywhere above it.
func Wrap(op, message string, err error) *TaskError {
	return &TaskError{Op: op, Message: message, Err: err}
}
