package krun

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWork(t *testing.T) {
	t.Run("data is passed through", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
		}

		// data is passed through
		rChan := make(chan *Result)
		w := &worker{
			job: func(ctx context.Context) (interface{}, error) {
				return 5, nil
			},
			result: rChan,
		}

		go k.work(context.Background(), w)

		r := <-rChan
		if i, ok := r.Data.(int); !ok {
			t.Errorf("Expected int, got %T", r.Data)
		} else if i != 5 {
			t.Errorf("Expected 5, got %v", i)
		}
		close(rChan)

		_ = <-k.workers
		close(k.workers)
	})

	t.Run("error is passed through", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
		}

		// error is passed through
		errChan := make(chan *Result)
		testErr := errors.New("test error")
		w := &worker{
			job: func(ctx context.Context) (interface{}, error) {
				return nil, testErr
			},
			result: errChan,
		}

		go k.work(context.Background(), w)

		r := <-errChan
		if !errors.Is(r.Error, testErr) {
			t.Errorf("Expected %v, got %v", testErr, r.Error)
		}
		close(errChan)

		_ = <-k.workers
		close(k.workers)
	})

	t.Run("context is passed through", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
		}

		// context carries info given to it
		ctxChan := make(chan *Result)
		ctx := context.WithValue(context.Background(), "test", "test")
		w := &worker{
			job: func(ctx context.Context) (interface{}, error) {
				return ctx.Value("test"), nil
			},
			result: ctxChan,
		}

		go k.work(ctx, w)

		r := <-ctxChan
		if r.Data != "test" {
			t.Errorf("Expected test, got %v", r.Data)
		}
		close(ctxChan)

		_ = <-k.workers
		close(k.workers)
	})

	t.Run("worker is pushed back to the channel", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
		}

		// worker is pushed back to the channel
		w := &worker{
			job: func(ctx context.Context) (interface{}, error) {
				return nil, nil
			},
			result: make(chan *Result),
		}

		go k.work(context.Background(), w)

		if len(k.workers) != 0 {
			t.Errorf("Expected 0, got %v", len(k.workers))
		}

		_ = <-w.result

		time.Sleep(time.Millisecond)

		if len(k.workers) != 1 {
			t.Errorf("Expected 1, got %v", len(k.workers))
		}

		bw := <-k.workers
		if w != bw {
			t.Errorf("Expected %p, got %p", w, w)
		}

		close(k.workers)
	})
}

func TestPush(t *testing.T) {
	t.Run("pushes worker to channel", func(t *testing.T) {
		k := krun{
			waitSleep: time.Microsecond,
			workers:   make(chan *worker, 3),
			mu:        sync.RWMutex{},
			n:         3,
		}
		workers := []*worker{
			&worker{},
			&worker{},
			&worker{},
		}

		k.push(workers[0])
		k.push(workers[1])
		k.push(workers[2])

		if len(k.workers) != 3 {
			t.Errorf("Expected 3, got %v", len(k.workers))
		}

		for i := 0; i < 3; i++ {
			w := <-k.workers
			var found bool

			for _, expect := range workers {
				if w == expect {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected worker to be in workers")
			}
		}

		close(k.workers)
	})

	t.Run("blocks until worker is pulled", func(t *testing.T) {
		k := krun{
			waitSleep: time.Microsecond,
			workers:   make(chan *worker, 1),
			mu:        sync.RWMutex{},
			n:         1,
		}

		errChan := make(chan error)

		go func() {
			k.push(&worker{})
			k.push(&worker{})
			errChan <- errors.New("pushed more than 1 worker")
		}()

		select {
		case <-errChan:
			t.Errorf("Expected goroutine to be blocked")
		case <-time.After(time.Millisecond):
			defer close(errChan)
		}
	})
}

func TestPop(t *testing.T) {
	k := krun{
		waitSleep: time.Microsecond,
		workers:   make(chan *worker, 3),
		mu:        sync.RWMutex{},
		n:         3,
	}
	workers := []*worker{
		&worker{},
		&worker{},
		&worker{},
	}

	for _, w := range workers {
		k.workers <- w
	}

	for i := 0; i < 3; i++ {
		w := k.pop()
		var found bool

		for _, expect := range workers {
			if w == expect {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Expected worker to be in workers")
		}
	}
}

func TestLen(t *testing.T) {
	k := krun{
		waitSleep: time.Microsecond,
		workers:   make(chan *worker, 3),
		mu:        sync.RWMutex{},
		n:         3,
	}
	workers := []*worker{
		&worker{},
		&worker{},
		&worker{},
	}

	if k.len() != 0 {
		t.Errorf("Expected 0, got %v", k.len())
	}

	for i, w := range workers {
		k.workers <- w

		if k.len() != i+1 {
			t.Errorf("Expected %d, got %v", i+1, k.len())
		}
	}

	close(k.workers)
}
