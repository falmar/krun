package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/falmar/krun"
)

func main() {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx := context.Background()
	k := krun.New(krun.NewConfig{
		Size: 10,
	})

	for i := 0; i < 100; i++ {
		ctx := context.WithValue(ctx, "key", i)

		r := k.Run(ctx, func(ctx context.Context) (interface{}, error) {
			// do some work

			return ctx.Value("key"), nil
		})

		go func(r <-chan *krun.Result) {
			time.Sleep(time.Millisecond * (300 + time.Duration(random.Intn(700))))
			fmt.Println("hello from index:", (<-r).Data)
		}(r)
	}

	k.Wait(context.Background())
}
