# Queue - Concurrent FIFO Queue for Go

A high-performance, thread-safe FIFO (First-In-First-Out) queue implementation in Go with blocking read capabilities. This package is optimized for high-throughput message processing scenarios with minimal overhead.

---

## Features

- **Thread-safe**: All operations are protected by read-write mutexes
- **Blocking reads**: `DequeueBlocking()` with context support for cancellation and timeouts
- **Non-blocking reads**: `Dequeue()` for immediate operations
- **High performance**: Optimized for minimal overhead and high throughput
- **Graceful shutdown**: Queue can be closed to signal completion to waiting consumers

## Installation

```bash
go github.com/condrove10/queue
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/condrove10/queue"
)

func main() {
    // Create a new queue for strings
    q := queue.New()
    
    // Add some items
    q.Enqueue("hello")
    q.Enqueue("world")
    
    // Remove items (non-blocking)
    value, hasNext := q.Dequeue()
    fmt.Println(value, hasNext) // Output: hello true
    
    value, hasNext = q.Dequeue()
    fmt.Println(value, hasNext) // Output: world false
}
```

## API Reference

### Creating a Queue

```go
// Create a new queue for any type T
q := queue.New()
```

### Adding Items

```go
// Enqueue adds an item to the tail of the queue
err := q.Enqueue(item)
if err != nil {
    // Handle error (queue is closed)
}
```

### Removing Items

#### Non-blocking Dequeue

```go
// Dequeue removes and returns the front item
value, hasNext := q.Dequeue()
// hasNext indicates if more items remain in the queue
```

#### Blocking Dequeue

```go
// DequeueBlocking waits until an item is available
ctx := context.Background()
value, err := q.DequeueBlocking(ctx)
if err != nil {
    // Handle error (context cancelled or queue closed)
}
```

### Queue Operations

```go
// Check queue size
size := q.Size()

// Check if empty
empty := q.IsEmpty()

// Clear all items
q.Clear()

// Close the queue (signals waiting consumers)
q.Close()

// Check if closed
closed := q.IsClosed()
open := q.IsOpen()
```

## Examples

### Producer-Consumer Pattern

```go
package main

import (
    "context"
    "fmt"
    "sync"
    "time"
    "github.com/condrove10/queue"
)

func main() {
    q := queue.New()
    var wg sync.WaitGroup
    
    // Producer goroutine
    wg.Add(1)
    go func() {
        defer wg.Done()
        defer q.Close() // Signal consumers when done
        
        for i := 0; i < 10; i++ {
            q.Enqueue(i)
            fmt.Printf("Produced: %d\n", i)
            time.Sleep(100 * time.Millisecond)
        }
    }()
    
    // Consumer goroutines
    for i := 0; i < 3; i++ {
        wg.Add(1)
        go func(consumerID int) {
            defer wg.Done()
            ctx := context.Background()
            
            for {
                value, err := q.DequeueBlocking(ctx)
                if err != nil {
                    fmt.Printf("Consumer %d: Queue closed\n", consumerID)
                    return
                }
                fmt.Printf("Consumer %d consumed: %d\n", consumerID, value)
            }
        }(i)
    }
    
    wg.Wait()
}
```

### With Context Timeout

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/condrove10/queue"
)

func main() {
    q := queue.New()
    
    // Try to dequeue with a 2-second timeout
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()
    
    value, err := q.DequeueBlocking(ctx)
    if err != nil {
        if err == context.DeadlineExceeded {
            fmt.Println("Timeout: No items available within 2 seconds")
        }
    } else {
        fmt.Printf("Received: %s\n", value)
    }
}
```

### Work Queue with Graceful Shutdown

```go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "github.com/condrove10/queue"
)

type Task struct {
    ID   int
    Data string
}

func main() {
    q := queue.New()
    ctx, cancel := context.WithCancel(context.Background())
    var wg sync.WaitGroup
    
    // Handle shutdown signals
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    
    go func() {
        <-sigCh
        fmt.Println("\nShutdown signal received...")
        cancel()
        q.Close()
    }()
    
    // Start workers
    for i := 0; i < 3; i++ {
        wg.Add(1)
        go worker(ctx, q, i, &wg)
    }
    
    // Add some tasks
    for i := 0; i < 20; i++ {
        task := Task{ID: i, Data: fmt.Sprintf("task-%d", i)}
        if err := q.Enqueue(task); err != nil {
            break // Queue is closed
        }
    }
    
    // Wait for workers to finish
    wg.Wait()
    fmt.Println("All workers finished")
}

func worker(ctx context.Context, q *queue.Queue, workerID int, wg *sync.WaitGroup) {
    defer wg.Done()
    
    for {
        task, err := q.DequeueBlocking(ctx)
        if err != nil {
            fmt.Printf("Worker %d: Shutting down (%v)\n", workerID, err)
            return
        }
        
        // Process task
        fmt.Printf("Worker %d processing task %d: %s\n", workerID, task.ID, task.Data)
        // Simulate work...
    }
}
```

## Error Handling

The queue defines custom errors:

- `queue.ErrQueueClosed`: Returned when trying to enqueue to a closed queue

```go
err := q.Enqueue(item)
if err == queue.ErrQueueClosed {
    // Handle closed queue
}
```
