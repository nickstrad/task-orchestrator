package queue

import collq "github.com/golang-collections/collections/queue"

/*
Queue is a type-safe wrapper around golang-collections/collections/queue.

The problem it solves
---------------------
The underlying collections queue stores every item as an `interface{}`. That
means every producer calls `Enqueue(anything)` and every consumer has to hand-
write a type assertion to get its value back:

	q.Enqueue(someTaskEvent)          // accepts literally anything
	te := q.Dequeue().(*task.TaskEvent) // asserts a concrete type at runtime

Nothing links the two sides. If one place enqueues a `task.TaskEvent` (value)
and another dequeues a `*task.TaskEvent` (pointer) — or a `task.Task` gets
enqueued onto a queue everything else treats as `task.TaskEvent` — the compiler
can't see it. The mismatch only shows up at runtime as:

	panic: interface conversion: interface {} is X, not Y

which is exactly the class of panic this codebase hit repeatedly.

How the wrapper fixes it
------------------------
By making the queue generic over T, the element type is fixed at construction:

	q := queue.New[task.TaskEvent]()

From then on `Enqueue` only accepts a `task.TaskEvent`, and `Dequeue` returns a
`task.TaskEvent` — both checked by the compiler at every call site. Callers never
write a type assertion, so they can never write the wrong one. The single
`v.(T)` below is the one and only assertion in the whole flow, so it's the one
place to look if anything is ever off.

Dequeue also returns an `ok` bool instead of a nil interface, folding the
"is the queue empty?" check into the same call and removing another spot where a
nil could slip through untyped.
*/
type Queue[T any] struct {
	inner *collq.Queue
}

// New returns an empty queue whose elements are of type T.
func New[T any]() *Queue[T] {
	return &Queue[T]{inner: collq.New()}
}

// Enqueue adds v to the back of the queue.
func (q *Queue[T]) Enqueue(v T) {
	q.inner.Enqueue(v)
}

// Dequeue removes and returns the item at the front of the queue. The bool is
// false (and the returned T is the zero value) when the queue is empty.
func (q *Queue[T]) Dequeue() (T, bool) {
	v := q.inner.Dequeue()
	if v == nil {
		var zero T
		return zero, false
	}
	return v.(T), true // the ONLY type assertion in the codebase
}

// Len reports how many items are currently in the queue.
func (q *Queue[T]) Len() int {
	return q.inner.Len()
}
