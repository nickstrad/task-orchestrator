package store

import (
	"errors"
	"fmt"

	"github.com/nickstrad/task-orchestrator/internal/task"
)

// ErrNotFound is the sentinel every Get returns for a missing key. Callers
// match on it with errors.Is, which keeps working through a StoreError chain
// because StoreError implements Unwrap.
var ErrNotFound = errors.New("element not found")

const (
	InMemoryDb   = "IN_MEMORY"
	PersistentDb = "PERSISTENT"
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
	Close() error
}

// Dbs is the set of stores a Manager needs. A Worker takes only a
// Store[task.Task] and is constructed with one directly.
type Dbs struct {
	TaskDb      Store[task.Task]
	TaskEventDb Store[task.TaskEvent]
}

func GetInMemoryDbs() *Dbs {
	return &Dbs{
		TaskDb:      NewInMemory[task.Task](),
		TaskEventDb: NewInMemory[task.TaskEvent](),
	}
}

func GetPersistentDbs(id string, freshStart bool) (*Dbs, error) {
	op := "store.GetPersistentDbs"

	if id == "" {
		return nil, E(op, "must pass in id", nil)
	}

	tDb, err := NewPersistentStore[task.Task](fmt.Sprintf("%s-tasks.db", id), 0600, "tasks", freshStart)
	if err != nil {
		return nil, Wrap(op, "unable to build task store", err)
	}
	tEventDb, err := NewPersistentStore[task.TaskEvent](fmt.Sprintf("%s-events.db", id), 0600, "events", freshStart)
	if err != nil {
		// tDb opened successfully; close it so its file handle and lock don't
		// leak when we bail out here.
		tDb.Close()
		return nil, Wrap(op, "unable to build task event store", err)
	}

	return &Dbs{
		TaskDb:      tDb,
		TaskEventDb: tEventDb,
	}, nil
}

func GetDBs(dbType, id string, freshStart bool) (*Dbs, error) {
	op := "store.GetDBs"
	switch dbType {
	case InMemoryDb:
		return GetInMemoryDbs(), nil
	case PersistentDb:
		return GetPersistentDbs(id, freshStart)
	default:
		return nil, E(op, fmt.Sprintf("unsupported db type %q: want %s or %s", dbType, InMemoryDb, PersistentDb), nil)
	}
}
