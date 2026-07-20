package store

import (
	"errors"

	"github.com/nickstrad/task-orchestrator/internal/task"
)

// ErrNotFound is the sentinel every Get returns for a missing key. Callers
// match on it with errors.Is, which keeps working through a StoreError chain
// because StoreError implements Unwrap.
var ErrNotFound = errors.New("element not found")

const (
	InMemoryDb = "IN_MEMORY"
)

// Store is a keyed collection of one concrete type.
//
// It is generic for the same reason internal/queue is: an interface{}-keyed
// store makes every caller hand-write a type assertion to get its value back,
// and nothing links the write side to the read side. A value stored as
// task.Task and read as *task.Task compiles fine and panics at runtime with
//
//	panic: interface conversion: interface {} is X, not Y
//
// which is the class of bug this codebase has hit before. With a type parameter
// the element type is fixed at construction — Put only accepts a task.Task and
// Get only returns one, both checked by the compiler — so callers cannot write
// the wrong assertion because they write none at all.
//
// The contract every implementation must honour:
//
//   - Values go in and come out BY VALUE, never by pointer. Go copies a struct
//     on assignment, so nothing a caller holds aliases what the store holds. A
//     pointer-based store would have to copy explicitly at both boundaries and
//     would silently share state the moment one implementation forgot to.
//
//   - Get returns ErrNotFound (wrapped) for a missing key, alongside the zero
//     value of T. There is no nil to guard against: a caller that ignores the
//     error gets a zero value, not a panic.
type Store[T any] interface {
	Put(key string, value T) error
	Get(key string) (T, error)
	List() ([]T, error)
	Count() (int, error)
}

// Dbs is the set of stores a Manager needs. A Worker takes only a
// Store[task.Task] and is constructed with one directly.
type Dbs struct {
	TaskDb      Store[task.Task]
	TaskEventDb Store[task.TaskEvent]
}

// GetDbs builds the set of stores for a backend. Every dbType maps to the
// in-memory backend rather than failing — it is the only backend that exists
// today, so the parameter records intent and nothing more.
//
// When a persistent backend lands this should return an error for an unknown
// type, so a typo in a config value does not silently hand back a store that
// loses every write on exit. Branching on dbType before then would only be a
// switch whose arms are all the same value.
func GetDbs(dbType string) *Dbs {
	return &Dbs{
		TaskDb:      NewInMemory[task.Task](),
		TaskEventDb: NewInMemory[task.TaskEvent](),
	}
}
