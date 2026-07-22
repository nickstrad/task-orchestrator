package store

import (
	"maps"
	"slices"
)

// InMemory is a Store[T] backed by a plain map. It is the only backend today.
//
// It is NOT safe for concurrent use — internal/manager and internal/worker each
// guard their stores with their own mutex, because both have invariants that
// span more than one store call (see docs/concurrency-and-state.md). A store
// that locked internally would make those callers pay twice and would still not
// make a read-modify-write atomic.
type InMemory[T any] struct {
	Db map[string]T
}

func NewInMemory[T any]() *InMemory[T] {
	return &InMemory[T]{Db: make(map[string]T)}
}

func (i *InMemory[T]) Close() error { return nil }

func (i *InMemory[T]) Put(key string, value T) error {
	i.Db[key] = value
	return nil
}

func (i *InMemory[T]) Get(key string) (T, error) {
	el, exists := i.Db[key]
	if !exists {
		var zero T
		return zero, E("store.InMemory.Get", "key "+key, ErrNotFound)
	}
	return el, nil
}

func (i *InMemory[T]) List() ([]T, error) {
	return slices.Collect(maps.Values(i.Db)), nil
}

func (i *InMemory[T]) Count() (int, error) {
	return len(i.Db), nil
}
