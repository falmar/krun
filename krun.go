package krun

import (
	"context"
	"errors"
	"sync"
)

// ErrPoolClosed is returned when operations are attempted on a closed pool.
var ErrPoolClosed = errors.New("pool's closed")

// Result represents the result of a job execution.
type Result struct {
	Data  interface{}
	Error error
}

// Job represents a function that can be executed by the worker pool.
// It receives a context and returns a result and an error.
type Job func(ctx context.Context) (interface{}, error)

// Krun is the interface for a worker pool that can execute jobs concurrently.
type Krun interface {
	// Run executes a job and returns a channel that will receive the result.
	Run(ctx context.Context, f Job) <-chan *Result
	// Wait blocks until all running jobs complete or the context is cancelled.
	Wait(ctx context.Context)
	// Size returns the number of workers in the pool.
	Size() int
	// Close shuts down the pool, waiting for all running jobs to complete.
	// Returns ErrPoolClosed if called multiple times.
	Close() error
}

type krun struct {
	poolSize int
	closed   bool

	workers chan *worker
	mu      sync.RWMutex

	wg sync.WaitGroup
}
type worker struct {
	job    Job
	result chan *Result
}

// Config configures a new Krun instance.
type Config struct {
	Size int
}

// New creates a new Krun worker pool with the given configuration.
func New(cfg *Config) Krun {
	size := 1
	if cfg != nil && cfg.Size > 0 {
		size = cfg.Size
	}

	k := &krun{
		poolSize: size,
		closed:   false,

		workers: make(chan *worker, size),
		wg:      sync.WaitGroup{},
		mu:      sync.RWMutex{},
	}

	for i := 0; i < size; i++ {
		k.push(&worker{})
	}

	return k
}

func (k *krun) Size() int {
	k.mu.RLock()
	s := k.poolSize
	k.mu.RUnlock()
	return s
}

func (k *krun) Run(ctx context.Context, f Job) <-chan *Result {
	cr := make(chan *Result, 1)

	// Check if context is already cancelled before trying to get a worker
	if ctx.Err() != nil {
		cr <- &Result{Error: ctx.Err()}
		return cr
	}

	// get worker from the channel
	select {
	case <-ctx.Done():
		cr <- &Result{Error: ctx.Err()}
		return cr
	case w, ok := <-k.workers:
		if !ok {
			// Channel was closed
			cr <- &Result{Error: ErrPoolClosed}
			return cr
		}

		// Check if pool was closed after getting worker
		k.mu.RLock()
		closed := k.closed
		k.mu.RUnlock()

		if closed {
			// Pool was closed, discard worker
			cr <- &Result{Error: ErrPoolClosed}
			return cr
		}

		k.wg.Add(1)

		// assign Job to the worker and Run it
		w.job = f
		w.result = cr
		go k.work(ctx, w)

		return cr
	}
}

func (k *krun) Wait(ctx context.Context) {
	done := make(chan struct{})

	go func() {
		k.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return
	case <-done:
		return
	}
}

func (k *krun) Close() error {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return ErrPoolClosed
	}
	k.closed = true
	k.mu.Unlock()

	// Close worker channel first to unblock any waiting Run() calls
	close(k.workers)

	// Wait for all work to complete
	k.wg.Wait()

	return nil
}

func (k *krun) work(ctx context.Context, w *worker) {
	// run the job
	d, err := w.job(ctx)

	// send Result into the caller channel
	w.result <- &Result{d, err}
	k.wg.Done()

	// return worker to Krun if pool is still open
	k.push(w)
}

func (k *krun) push(w *worker) {
	k.mu.RLock()
	closed := k.closed
	k.mu.RUnlock()

	if closed {
		// Pool is closed, discard worker
		return
	}

	k.workers <- w
}
