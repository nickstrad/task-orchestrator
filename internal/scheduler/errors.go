package scheduler

import "runtime/debug"

// SchedulerError is this package's error type — same pattern as task.TaskError,
// worker.WorkerError and manager.ManagerError. The duplication keeps each
// package self-contained, and the name says which layer an error came from.
type SchedulerError struct {
	Op      string // e.g. "scheduler.EpvmScheduler.getNodeStats"
	Message string // user-friendly
	Err     error  // wrapped cause (nil at an origin with no cause)
	Stack   string // captured ONLY at the origin
}

func (e *SchedulerError) Error() string {
	if e.Err != nil {
		return e.Op + ": " + e.Message + ": " + e.Err.Error()
	}
	return e.Op + ": " + e.Message
}

func (e *SchedulerError) Unwrap() error { return e.Err }

// E mints an ORIGIN error: the cause is nil, or it came from outside this
// codebase (net/http, encoding/json). It captures the stack.
func E(op, message string, err error) *SchedulerError {
	return &SchedulerError{Op: op, Message: message, Err: err, Stack: string(debug.Stack())}
}

// Wrap adds context to an error that already carries a stack, so it does not
// capture a second one. Use E at the boundary, Wrap everywhere above it.
func Wrap(op, message string, err error) *SchedulerError {
	return &SchedulerError{Op: op, Message: message, Err: err}
}
