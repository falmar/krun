# Krun - QueueRun

[![Build Krun](https://github.com/falmar/krun/actions/workflows/build.yaml/badge.svg)](https://github.com/falmar/krun/actions/workflows/build.yaml)

Krun is a simple worker queue, which provides an easy way to manage and execute jobs concurrently. It can wait for all jobs to finish, and get the results from the executed jobs. The package can be found at [github.com/falmar/krun](https://github.com/falmar/krun).

## Installation

To install the Krun package, simply run:

```bash
go get -u github.com/falmar/krun
```

## Usage

Here's a basic example of how to use the Krun package:

```go
package main

import (
	"context"
	"fmt"
	"github.com/falmar/krun"
	"os"
	"time"
)

func main() {
	queue := krun.New(krun.NewConfig{
		Size:      5, // number of workers
		WaitSleep: time.Millisecond * 10,
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

	fmt.Println("Result:", res.Data)
}
```

## License

Krun is released under the MIT License. See [LICENSE](LICENSE) for more information.
