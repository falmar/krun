package krun

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("returns a Krun", func(t *testing.T) {
		k := New(NewConfig{})
		if k == nil {
			t.Errorf("Expected Krun, got nil")
		}

		v, ok := k.(*krun)
		if !ok {
			t.Errorf("Expected *krun, got %T", v)
		}
	})

	t.Run("returns a Krun with the correct size", func(t *testing.T) {
		k := New(NewConfig{Size: 5}).(*krun)

		if k.n != 5 {
			t.Errorf("Expected 5, got %v", k.Size())
		}

		if len(k.workers) != 5 {
			t.Errorf("Expected 5, got %v", len(k.workers))
		}
	})

	t.Run("returns a Krun with the correct waitSleep", func(t *testing.T) {
		k := New(NewConfig{WaitSleep: time.Second}).(*krun)

		if k.waitSleep != time.Second {
			t.Errorf("Expected 1s, got %v", k.waitSleep)
		}
	})
}

func TestKrun_Size(t *testing.T) {
	t.Parallel()

	t.Run("returns the correct size", func(t *testing.T) {
		k := krun{
			n: 5,
		}

		if k.Size() != 5 {
			t.Errorf("Expected 5, got %v", k.Size())
		}
	})
}

func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("returns a channel", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
		}
		k.push(&worker{})

		r := k.Run(context.Background(), func(ctx context.Context) (interface{}, error) {
			return nil, nil
		})

		if r == nil {
			t.Errorf("Expected channel, got nil")
		}
	})

	t.Run("blocks waiting for available worker", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
		}

		errChan := make(chan error)

		go func() {
			k.Run(context.Background(), func(ctx context.Context) (interface{}, error) {
				return nil, nil
			})
			errChan <- errors.New("expected k.Run to block")
		}()

		select {
		case e := <-errChan:
			t.Errorf(e.Error())
		case <-time.After(time.Millisecond):
			return
		}
	})

	t.Run("sends off the job for work and return result", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
		}
		k.push(&worker{})

		r := k.Run(context.Background(), func(ctx context.Context) (interface{}, error) {
			return 5, nil
		})

		res := <-r
		if i, ok := res.Data.(int); !ok {
			t.Errorf("Expected int, got %T", res.Data)
		} else if i != 5 {
			t.Errorf("Expected 5, got %v", i)
		}
	})

	t.Run("pass context to job", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
		}
		k.push(&worker{})

		ctx := context.Background()

		r := k.Run(ctx, func(ctx2 context.Context) (interface{}, error) {
			if ctx != ctx2 {
				t.Errorf("Expected %v, got %v", ctx, ctx2)
			}
			return nil, nil
		})

		if d := <-r; d.Error != nil {
			t.Errorf("Expected nil, got %v", d.Error)
		} else if d.Data != nil {
			t.Errorf("Expected nil, got %v", d.Data)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		r = k.Run(ctx, func(ctx2 context.Context) (interface{}, error) {
			select {
			case <-ctx2.Done():
				return nil, ctx2.Err()
			default:
				t.Errorf("Expected context to be cancelled")
				return nil, nil
			}
		})

		if d := <-r; d.Error == nil {
			t.Errorf("Expected error, got nil")
		} else if !errors.Is(d.Error, context.Canceled) {
			t.Errorf("Expected context.Canceled, got %v", d.Error)
		}
	})
}

func TestKrun_Wait(t *testing.T) {
	t.Parallel()

	t.Run("blocks if workers are not done", func(t *testing.T) {
		k := krun{
			waitSleep: time.Millisecond * 50,
			workers:   make(chan *worker, 1),
			mu:        sync.RWMutex{},
			n:         1,
		}
		ctx := context.Background()
		wChan := make(chan struct{})
		// len 1, has no workers

		go func() {
			k.Wait(ctx)
			wChan <- struct{}{}
		}()

		select {
		case <-wChan:
			t.Errorf("Expected k.Wait to block")
		case <-time.After(time.Millisecond * 100):
			break
		}
	})

	t.Run("unblocks when workers are done", func(t *testing.T) {
		k := krun{
			waitSleep: time.Millisecond * 50,
			workers:   make(chan *worker, 1),
			mu:        sync.RWMutex{},
			n:         1,
		}
		ctx := context.Background()
		wChan := make(chan struct{})
		// len 1, work is "done"
		k.workers <- &worker{}

		go func() {
			k.Wait(ctx)
			wChan <- struct{}{}
		}()

		time.Sleep(time.Millisecond)

		select {
		case <-wChan:
			break
		case <-time.After(time.Millisecond):
			t.Errorf("Expected k.Wait to unblock")
		}
	})

	t.Run("sleep between checks", func(t *testing.T) {
		k := krun{
			waitSleep: time.Millisecond * 50,
			workers:   make(chan *worker, 1),
			mu:        sync.RWMutex{},
			n:         1,
		}
		ctx := context.Background()
		wChan := make(chan struct{})
		// len 1, work is "done"

		var start time.Time
		var end time.Time

		go func() {
			time.Sleep(time.Millisecond * 60)
			k.workers <- &worker{}
		}()

		go func() {
			start = time.Now()
			k.Wait(ctx)
			end = time.Now()
			wChan <- struct{}{}
		}()

		time.Sleep(time.Millisecond)

		select {
		case <-wChan:
			if end.Sub(start) < time.Millisecond*50 {
				t.Errorf("Expected k.Wait to sleep for at least 50ms")
			}
		case <-time.After(time.Millisecond * 100):
			t.Errorf("Expected k.Wait to unblock")
		}
	})

	t.Run("context done unblock", func(t *testing.T) {
		k := krun{
			waitSleep: time.Millisecond * 50,
			workers:   make(chan *worker, 1),
			mu:        sync.RWMutex{},
			n:         1,
		}

		var start time.Time
		var end time.Time
		c := make(chan struct{})

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
			defer cancel()

			start = time.Now()
			k.Wait(ctx)
			end = time.Now()

			c <- struct{}{}
		}()

		select {
		case <-c:
			if end.Sub(start) < time.Millisecond*100 {
				t.Errorf("Expected k.Wait to sleep for at least 100ms")
			}

			break
		case <-time.After(time.Millisecond * 200):
			t.Errorf("Expected k.Wait to unblock")
		}
	})
}

func TestKrun_Work(t *testing.T) {
	t.Parallel()

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

func TestKrun_Push(t *testing.T) {
	t.Parallel()

	t.Run("pushes worker to channel", func(t *testing.T) {
		k := krun{
			workers: make(chan *worker, 3),
			mu:      sync.RWMutex{},
			n:       3,
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
			workers: make(chan *worker, 1),
			mu:      sync.RWMutex{},
			n:       1,
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

func TestKrun_Pop(t *testing.T) {
	t.Parallel()

	k := krun{
		workers: make(chan *worker, 3),
		mu:      sync.RWMutex{},
		n:       3,
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

func TestKrun_Len(t *testing.T) {
	t.Parallel()

	k := krun{
		workers: make(chan *worker, 3),
		mu:      sync.RWMutex{},
		n:       3,
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
