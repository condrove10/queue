package queue

import (
	"container/list"
	"context"
	"sync"
)

// Queue is a concurrency-safe FIFO linked list with blocking read capabilities.
// Optimized for high-throughput message processing with minimal overhead.
type Queue[T any] struct {
	mu     sync.RWMutex
	cond   *sync.Cond
	l      *list.List
	cursor *list.Element
	length int
	closed bool
}

// New creates a new Queue with pre-allocated list structure.
func New[T any]() *Queue[T] {
	q := &Queue[T]{
		l: list.New(),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Enqueue appends a new element to the tail of the queue.
// Returns error if queue is closed. Signals waiting readers when new data is available.
func (q *Queue[T]) Enqueue(value T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	elem := q.l.PushBack(value)
	if q.cursor == nil {
		q.cursor = elem
	}
	q.length++

	// Signal waiting readers
	q.cond.Signal()
	return nil
}

// Dequeue returns the current value and removes it from the queue.
// Returns hasNext to indicate if more elements remain after dequeuing.
// Non-blocking operation for compatibility.
func (q *Queue[T]) Dequeue() (value T, hasNext bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.length == 0 || q.cursor == nil {
		var zero T
		return zero, false
	}

	// Get the current value
	val := q.cursor.Value.(T)

	// Move cursor to next before removing
	next := q.cursor.Next()
	q.l.Remove(q.cursor)
	q.cursor = next
	q.length--

	return val, q.length > 0
}

// DequeueBlocking blocks until a message is available and returns it.
// Context can be used for cancellation and timeout control.
// It avoids spawning helper goroutines by checking ctx.Done() inline.
func (q *Queue[T]) DequeueBlocking(ctx context.Context) (T, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		// If queue has elements, dequeue immediately.
		if q.length > 0 {
			elem := q.l.Front()
			val := elem.Value.(T)
			q.l.Remove(elem)
			q.length--
			return val, nil
		}

		// If closed and empty → error.
		if q.closed {
			var zero T
			return zero, ErrQueueClosed
		}

		// Before waiting, check context cancellation.
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		default:
			// Nothing cancelled yet; wait for signal.
			q.cond.Wait()
		}
	}
}

// Size returns the number of elements in the queue.
func (q *Queue[T]) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.length
}

// IsEmpty returns true if the queue is empty.
func (q *Queue[T]) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.length == 0
}

// Clear removes all elements from the queue.
func (q *Queue[T]) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.l.Init()
	q.cursor = nil
	q.length = 0
}

// Close marks the queue as closed and signals all waiting readers.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.cond.Broadcast()
}

// IsClosed returns true if the queue is closed.
func (q *Queue[T]) IsClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}

// IsOpen returns true if the queue is open (not closed).
func (q *Queue[T]) IsOpen() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return !q.closed
}

// Custom errors
var (
	ErrQueueClosed = &QueueError{"queue is closed"}
	ErrQueueEmpty  = &QueueError{"queue is empty"}
)

type QueueError struct {
	message string
}

func (e *QueueError) Error() string {
	return e.message
}
