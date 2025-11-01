package krun

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("returns a Krun", func(t *testing.T) {
		k := New(&Config{})
		if k == nil {
			t.Fatalf("Expected Krun, got nil")
		}

		v, ok := k.(*krun)
		if !ok {
			t.Fatalf("Expected *krun, got %T", v)
		}
	})

	t.Run("handles nil Config", func(t *testing.T) {
		k := New(nil)
		if k == nil {
			t.Fatalf("Expected Krun, got nil")
		}
		if k.Size() != 1 {
			t.Fatalf("Expected default size 1, got %d", k.Size())
		}
	})

	t.Run("handles Size 0", func(t *testing.T) {
		k := New(&Config{Size: 0})
		if k == nil {
			t.Fatalf("Expected Krun, got nil")
		}
		if k.Size() != 1 {
			t.Fatalf("Expected default size 1, got %d", k.Size())
		}
	})

	t.Run("handles negative Size", func(t *testing.T) {
		k := New(&Config{Size: -5})
		if k == nil {
			t.Fatalf("Expected Krun, got nil")
		}
		if k.Size() != 1 {
			t.Fatalf("Expected default size 1, got %d", k.Size())
		}
	})
}

func TestKrun_Size(t *testing.T) {
	t.Parallel()

	t.Run("returns the correct size", func(t *testing.T) {
		k := New(&Config{Size: 5})

		if k.Size() != 5 {
			t.Fatalf("Expected 5, got %v", k.Size())
		}

		k = New(&Config{Size: 2})

		if k.Size() != 2 {
			t.Fatalf("Expected 2, got %v", k.Size())
		}
	})
}

func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("returns a channel", func(t *testing.T) {
		k := New(&Config{Size: 1})

		r := k.Run(context.Background(), func(ctx context.Context) (interface{}, error) {
			return nil, nil
		})

		if r == nil {
			t.Fatalf("Expected channel, got nil")
		}
	})

	t.Run("runs and return value", func(t *testing.T) {
		k := New(&Config{Size: 1})

		r := <-k.Run(context.Background(), func(ctx context.Context) (interface{}, error) {
			return "my-string", nil
		})

		switch tp := r.Data.(type) {
		case string:
			if tp != "my-string" {
				t.Fatalf("expected \"my-string\", received: %s", tp)
			}
		default:
			t.Fatalf("expected string, got %t", tp)
		}
	})

	t.Run("runs and return error", func(t *testing.T) {
		k := New(&Config{Size: 1})

		myErr := errors.New("something went wrong")

		r := <-k.Run(context.Background(), func(ctx context.Context) (interface{}, error) {
			return nil, myErr
		})

		if r.Error == nil {
			t.Fatalf("expected an error, got nil")
		} else if !errors.Is(r.Error, myErr) {
			t.Fatalf("expected error to equal: %v, got: %v", myErr, r.Error)
		}
	})

	t.Run("blocks waiting for available worker", func(t *testing.T) {
		ctx := t.Context()
		k := New(&Config{Size: 1})

		errChan := make(chan error)

		k.Run(ctx, func(ctx context.Context) (interface{}, error) {
			time.Sleep(time.Millisecond * 5)
			return nil, nil
		})

		go func() {
			k.Run(ctx, func(ctx context.Context) (interface{}, error) {
				return nil, nil
			})
			errChan <- errors.New("expected k.Run to block")
		}()

		select {
		case e := <-errChan:
			t.Fatalf("Expected nil, got %v", e)
		case <-time.After(time.Millisecond):
			return
		}
	})

	t.Run("pass context to job", func(t *testing.T) {
		k := New(&Config{Size: 1})

		ctx := context.Background()

		r := k.Run(ctx, func(ctx2 context.Context) (interface{}, error) {
			if ctx != ctx2 {
				t.Fatalf("Expected %v, got %v", ctx, ctx2)
			}
			return nil, nil
		})

		if d := <-r; d.Error != nil {
			t.Fatalf("Expected nil, got %v", d.Error)
		} else if d.Data != nil {
			t.Fatalf("Expected nil, got %v", d.Data)
		}
	})

	t.Run("returns error if context already cancelled", func(t *testing.T) {
		k := New(&Config{Size: 1})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		r := <-k.Run(ctx, func(ctx context.Context) (interface{}, error) {
			return "should not run", nil
		})

		if r.Error == nil {
			t.Fatalf("Expected context cancelled error, got nil")
		}
		if r.Error != ctx.Err() {
			t.Fatalf("Expected context cancelled error, got %v", r.Error)
		}
	})

	t.Run("returns error if pool is closed", func(t *testing.T) {
		k := New(&Config{Size: 1})

		// Close the pool
		if err := k.Close(); err != nil {
			t.Fatalf("Expected nil on first close, got %v", err)
		}

		// Try to run a job after close
		r := <-k.Run(context.Background(), func(ctx context.Context) (interface{}, error) {
			return "should not run", nil
		})

		if r.Error == nil {
			t.Fatalf("Expected ErrPoolClosed, got nil")
		}
		if !errors.Is(r.Error, ErrPoolClosed) {
			t.Fatalf("Expected ErrPoolClosed, got %v", r.Error)
		}
	})

	t.Run("handles Run() after Close() while job waiting", func(t *testing.T) {
		// t.SkipNow()
		// return
		k := New(&Config{Size: 1})

		ctx := context.Background()

		// Start a job that takes time
		started := make(chan struct{})
		jobDone := make(chan struct{})
		_ = k.Run(ctx, func(ctx context.Context) (interface{}, error) {
			started <- struct{}{}
			<-jobDone
			return "done", nil
		})

		// Wait for job to start
		<-started

		// Close in another goroutine
		closeDone := make(chan error)
		go func() {
			closeDone <- k.Close()
		}()

		// Try to run another job while closing
		r := <-k.Run(ctx, func(ctx context.Context) (interface{}, error) {
			return "should not run", nil
		})

		if r.Error == nil {
			t.Fatalf("Expected ErrPoolClosed, got nil")
		}
		if !errors.Is(r.Error, ErrPoolClosed) {
			t.Fatalf("Expected ErrPoolClosed, got %v", r.Error)
		}

		// Finish the running job
		close(jobDone)

		// Wait for close to complete
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Expected nil on close, got %v", err)
			}
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("Close did not complete in time")
		}
	})
}

func TestKrun_Wait(t *testing.T) {
	t.Parallel()

	t.Run("blocks if workers are not done", func(t *testing.T) {
		k := New(&Config{Size: 1})
		ctx := context.Background()

		release := make(chan struct{})
		resCh := k.Run(ctx, func(ctx context.Context) (interface{}, error) {
			<-release
			return "ok", nil
		})

		doneWait := make(chan struct{})
		go func() {
			k.Wait(ctx)
			close(doneWait)
		}()

		select {
		case <-doneWait:
			t.Fatalf("expected Wait to block while job is running")
		case <-time.After(5 * time.Millisecond):
		}

		close(release)

		select {
		case <-resCh:
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("job did not complete in time")
		}

		select {
		case <-doneWait:
		case <-time.After(20 * time.Millisecond):
			t.Fatalf("Wait did not return after jobs finished")
		}
	})

	t.Run("unblocks when workers are done", func(t *testing.T) {
		k := New(&Config{Size: 1})
		ctx := context.Background()

		doneWait := make(chan struct{})
		go func() {
			k.Wait(ctx)
			close(doneWait)
		}()

		select {
		case <-doneWait:
			// ok
		case <-time.After(5 * time.Millisecond):
			t.Fatalf("expected Wait to return promptly when there are no jobs running")
		}
	})

	t.Run("unblocks when context time out", func(t *testing.T) {
		k := New(&Config{Size: 1})

		release := make(chan struct{})
		_ = k.Run(context.Background(), func(ctx context.Context) (interface{}, error) {
			<-release
			return nil, nil
		})

		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()

		doneWait := make(chan struct{})
		go func() {
			k.Wait(ctx)
			close(doneWait)
		}()

		select {
		case <-doneWait:
			if time.Since(start) < 5*time.Millisecond {
				t.Fatalf("expected Wait to respect context timeout; returned too early after %s", time.Since(start))
			}
		case <-time.After(20 * time.Millisecond):
			t.Fatalf("expected Wait to unblock due to context timeout")
		}

		close(release)
		k.Wait(context.Background())
	})
}

func TestKrun_Close(t *testing.T) {
	t.Run("first close returns nil", func(t *testing.T) {
		k := New(&Config{Size: 1})
		if err := k.Close(); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("double close returns ErrPoolClosed", func(t *testing.T) {
		k := New(&Config{Size: 1})
		if err := k.Close(); err != nil {
			t.Fatalf("expected first close to be nil, got %v", err)
		}
		if err := k.Close(); !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("expected ErrPoolClosed, got %v", err)
		}
	})

	t.Run("waits for running jobs to finish", func(t *testing.T) {
		k := New(&Config{Size: 1})

		ctx := context.Background()
		started := make(chan struct{}, 1)
		resCh := k.Run(ctx, func(ctx context.Context) (interface{}, error) {
			started <- struct{}{}
			time.Sleep(10 * time.Millisecond)
			return "done", nil
		})

		// Ensure job actually started
		select {
		case <-started:
		case <-time.After(5 * time.Millisecond):
			t.Fatalf("job did not start in time")
		}

		doneClose := make(chan error, 1)
		startClose := time.Now()
		go func() {
			doneClose <- k.Close()
		}()

		// get job result before its closed
		var got *Result
		select {
		case got = <-resCh:
			// ok
		case <-time.After(30 * time.Millisecond):
			t.Fatalf("timed out waiting for job result while closing")
		}
		if got == nil || got.Error != nil || got.Data != "done" {
			t.Fatalf("unexpected result: %#v", got)
		}

		// finish shortly after the jobs done
		var closeErr error
		select {
		case closeErr = <-doneClose:
			if closeErr != nil {
				t.Fatalf("expected close nil, got %v", closeErr)
			}
			if time.Since(startClose) < 9*time.Millisecond {
				t.Fatalf("Close returned too early; expected it to wait for the job")
			}
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("Close did not complete in time")
		}
	})

	t.Run("concurrent Close() calls", func(t *testing.T) {
		k := New(&Config{Size: 5})

		results := make(chan error, 10)
		for i := 0; i < 10; i++ {
			go func() {
				results <- k.Close()
			}()
		}

		// One should succeed, rest should get ErrPoolClosed
		successCount := 0
		errorCount := 0

		for i := 0; i < 10; i++ {
			select {
			case err := <-results:
				if err == nil {
					successCount++
				} else if errors.Is(err, ErrPoolClosed) {
					errorCount++
				} else {
					t.Fatalf("Unexpected error: %v", err)
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatalf("Close did not complete in time")
			}
		}

		if successCount != 1 {
			t.Fatalf("Expected exactly 1 successful close, got %d", successCount)
		}
		if errorCount != 9 {
			t.Fatalf("Expected 9 ErrPoolClosed, got %d", errorCount)
		}
	})
}
