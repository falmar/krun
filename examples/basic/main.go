package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/falmar/krun"
)

func main() {
	queue := krun.New(&krun.Config{
		Size: 5, // number of workers
	})

	job := func(ctx context.Context) (interface{}, error) {
		time.Sleep(time.Millisecond * 100)
		return "Hello, Krun!", nil
	}

	ctx := context.Background()
	resChan := queue.Run(ctx, job)

	res := <-resChan
	if res.Error != nil {
		fmt.Println("Error:", res.Error)
		os.Exit(1)
	}

	queue.Wait(ctx)

	fmt.Println("Result:", res.Data.(string))
}
