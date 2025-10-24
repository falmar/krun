package krun

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrPoolClosed it is closed (hahah)
var ErrPoolClosed = errors.New("pool's closed")

type Result struct {
	Data  interface{}
	Error error
}
type Job func(ctx context.Context) (interface{}, error)

type Krun interface {
	Run(ctx context.Context, f Job) <-chan *Result
	Wait(ctx context.Context)
	Size() int
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

type Config struct {
	Size      int
	WaitSleep time.Duration
}

func New(cfg *Config) Krun {
	k := &krun{
		poolSize: cfg.Size,
		closed:   false,

		workers: make(chan *worker, cfg.Size),
		wg:      sync.WaitGroup{},
		mu:      sync.RWMutex{},
	}

	for i := 0; i < cfg.Size; i++ {
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
	// get worker from the channel
	w := k.pop()
	k.wg.Add(1)

	// assign Job to the worker and Run it
	cr := make(chan *Result, 1)
	w.job = f
	w.result = cr
	go k.work(ctx, w)

	// return channel to the caller
	return cr
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

	// Wait for all work to complete
	k.wg.Wait()

	// Close worker channel
	close(k.workers)

	return nil
}

func (k *krun) work(ctx context.Context, w *worker) {
	// run the job
	d, err := w.job(ctx)

	// send Result into the caller channel
	w.result <- &Result{d, err}

	// return worker to Krun
	k.push(w)
	k.wg.Done()
}
func (k *krun) push(w *worker) {
	k.workers <- w
}

func (k *krun) pop() *worker {
	return <-k.workers
}
