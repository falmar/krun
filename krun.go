package krun

import (
	"context"
	"sync"
	"time"
)

type Result struct {
	Data  interface{}
	Error error
}
type Job func(ctx context.Context) (interface{}, error)

type Krun interface {
	Run(ctx context.Context, f Job) <-chan *Result
	Wait(ctx context.Context)
	Size() int
}

type krun struct {
	n         int
	waitSleep time.Duration
	workers   chan *worker
	working   int
	mu        sync.RWMutex
}
type worker struct {
	ctx    context.Context
	job    Job
	result chan *Result
}

type NewConfig struct {
	Size      int
	WaitSleep time.Duration
}

func New(cfg NewConfig) Krun {
	k := &krun{
		n:         cfg.Size,
		workers:   make(chan *worker, cfg.Size),
		waitSleep: cfg.WaitSleep,
	}

	for i := 0; i < cfg.Size; i++ {
		k.push(&worker{})
	}

	return k
}

func (k *krun) Size() int {
	k.mu.RLock()
	s := k.n
	k.mu.RUnlock()
	return s
}

func (k *krun) Run(ctx context.Context, f Job) <-chan *Result {
	// get worker from the channel
	w := k.pop()

	// assign Job to the worker and Run it
	cr := make(chan *Result)
	w.job = f
	w.result = cr
	go k.work(ctx, w)

	// return channel to the caller
	return cr
}

func (k *krun) Wait(ctx context.Context) {
breakL:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// "wait" until all workers are back
			if k.len() < k.n {
				time.Sleep(k.waitSleep)
				continue
			}

			break breakL
		}
	}
}

func (k *krun) work(ctx context.Context, w *worker) {
	// run the job
	d, err := w.job(ctx)

	// send Result into the caller channel
	// this will block until is read
	w.result <- &Result{d, err}

	// return worker to Krun
	k.push(w)
}
func (k *krun) push(w *worker) {
	k.workers <- w
}

func (k *krun) pop() *worker {
	w := <-k.workers
	return w
}

func (k *krun) len() int {
	k.mu.RLock()
	l := len(k.workers)
	k.mu.RUnlock()

	return l
}
