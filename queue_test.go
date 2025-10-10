package queue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/condrove10/queue"
	"github.com/stretchr/testify/assert"
)

func TestNewQueue(t *testing.T) {
	q := queue.New()
	assert.NotNil(t, q)
	assert.Equal(t, 0, q.Size())
	assert.True(t, q.IsEmpty())
	assert.False(t, q.IsClosed())
	assert.True(t, q.IsOpen())
}

func TestEnqueueDequeue(t *testing.T) {
	q := queue.New()
	err := q.Enqueue(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, q.Size())
	assert.False(t, q.IsEmpty())

	val, hasNext := q.Dequeue()
	assert.Equal(t, 1, val)
	assert.False(t, hasNext)
	assert.Equal(t, 0, q.Size())
	assert.True(t, q.IsEmpty())
}

func TestEnqueueDequeueMultiple(t *testing.T) {
	q := queue.New()
	err := q.Enqueue(1)
	assert.NoError(t, err)
	err = q.Enqueue(2)
	assert.NoError(t, err)
	err = q.Enqueue(3)
	assert.NoError(t, err)

	assert.Equal(t, 3, q.Size())

	val, hasNext := q.Dequeue()
	assert.Equal(t, 1, val)
	assert.True(t, hasNext)

	val, hasNext = q.Dequeue()
	assert.Equal(t, 2, val)
	assert.True(t, hasNext)

	val, hasNext = q.Dequeue()
	assert.Equal(t, 3, val)
	assert.False(t, hasNext)

	assert.True(t, q.IsEmpty())
}

func TestDequeueEmpty(t *testing.T) {
	q := queue.New()
	val, hasNext := q.Dequeue()
	assert.Nil(t, val)
	assert.False(t, hasNext)
}

func TestClear(t *testing.T) {
	q := queue.New()
	err := q.Enqueue(1)
	assert.NoError(t, err)
	err = q.Enqueue(2)
	assert.NoError(t, err)

	q.Clear()
	assert.Equal(t, 0, q.Size())
	assert.True(t, q.IsEmpty())

	val, hasNext := q.Dequeue()
	assert.Nil(t, val)
	assert.False(t, hasNext)
}

func TestClose(t *testing.T) {
	q := queue.New()
	q.Close()
	assert.True(t, q.IsClosed())
	assert.False(t, q.IsOpen())

	err := q.Enqueue(1)
	assert.Equal(t, queue.ErrQueueClosed, err)

	val, err := q.DequeueBlocking(context.Background())
	assert.Equal(t, queue.ErrQueueClosed, err)
	assert.Nil(t, val)
}

func TestDequeueBlocking(t *testing.T) {
	q := queue.New()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		val, err := q.DequeueBlocking(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, val)
	}()

	time.Sleep(100 * time.Millisecond) // Give the goroutine time to start blocking
	err := q.Enqueue(1)
	assert.NoError(t, err)

	wg.Wait()
}

func TestDequeueBlockingCanceled(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Immediately cancel the context

	val, err := q.DequeueBlocking(ctx)
	assert.Error(t, err)
	assert.Nil(t, val)
	assert.Equal(t, context.Canceled, err)
}

func TestDequeueBlockingTimeout(t *testing.T) {
	q := queue.New()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	val, err := q.DequeueBlocking(ctx)
	assert.Error(t, err)
	assert.Nil(t, val)
	assert.Error(t, err) // Expect a timeout error
}

func TestDequeueBlockingClosed(t *testing.T) {
	q := queue.New()
	q.Close()

	ctx := context.Background()
	val, err := q.DequeueBlocking(ctx)

	assert.Error(t, err)
	assert.Nil(t, val)
	assert.Equal(t, queue.ErrQueueClosed, err)
}

func TestSize(t *testing.T) {
	q := queue.New()
	assert.Equal(t, 0, q.Size())

	q.Enqueue(1)
	assert.Equal(t, 1, q.Size())

	q.Enqueue(2)
	assert.Equal(t, 2, q.Size())

	q.Dequeue()
	assert.Equal(t, 1, q.Size())

	q.Clear()
	assert.Equal(t, 0, q.Size())
}

func TestIsEmpty(t *testing.T) {
	q := queue.New()
	assert.True(t, q.IsEmpty())

	q.Enqueue(1)
	assert.False(t, q.IsEmpty())

	q.Dequeue()
	assert.True(t, q.IsEmpty())
}

func TestIsOpen(t *testing.T) {
	q := queue.New()
	assert.True(t, q.IsOpen())

	q.Close()
	assert.False(t, q.IsOpen())
}

func TestIsClosed(t *testing.T) {
	q := queue.New()
	assert.False(t, q.IsClosed())

	q.Close()
	assert.True(t, q.IsClosed())
}
