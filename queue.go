package queue

import (
	"container/list"
	"context"
	"sync"
)

type Queue struct {
	mu     sync.RWMutex
	cond   *sync.Cond
	l      *list.List
	length int
	closed bool
}

func New() *Queue {
	q := &Queue{
		l: list.New(),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue) Enqueue(value interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	q.l.PushBack(value)
	q.length++

	q.cond.Signal()
	return nil
}

func (q *Queue) Dequeue() (value interface{}, hasNext bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.length == 0 {
		var zero interface{}
		return zero, false
	}

	elem := q.l.Front()
	val := elem.Value
	q.l.Remove(elem)
	q.length--

	return val, q.length > 0
}

// DequeueBlocking blocks until a message is available and returns it.
// Context can be used for cancellation and timeout control.
func (q *Queue) DequeueBlocking(ctx context.Context) (interface{}, error) {
	// Use a channel to signal context cancellation
	ctxDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		q.mu.Lock()
		q.cond.Broadcast() // Wake up the waiting goroutine
		q.mu.Unlock()
		close(ctxDone)
	}()

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		// If queue has elements, dequeue immediately
		if q.length > 0 {
			elem := q.l.Front()
			val := elem.Value
			q.l.Remove(elem)
			q.length--
			return val, nil
		}

		// If closed and empty → error
		if q.closed {
			var zero interface{}
			return zero, ErrQueueClosed
		}

		// Check context before waiting
		select {
		case <-ctxDone:
			var zero interface{}
			return zero, ctx.Err()
		default:
		}

		// Wait for signal (releases lock while waiting)
		q.cond.Wait()

		// After waking, check if it was due to context cancellation
		select {
		case <-ctxDone:
			var zero interface{}
			return zero, ctx.Err()
		default:
		}
	}
}

func (q *Queue) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.length
}

func (q *Queue) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.length == 0
}

func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.l.Init()
	q.length = 0
}

func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.cond.Broadcast()
}

func (q *Queue) IsClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}

func (q *Queue) IsOpen() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return !q.closed
}

var (
	ErrQueueClosed = &QueueError{"queue is closed"}
)

type QueueError struct {
	message string
}

func (e *QueueError) Error() string {
	return e.message
}
