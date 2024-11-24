package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func producer(ctx context.Context, wg *sync.WaitGroup, messages chan<- any) {
	defer wg.Done()

	// Producer
	i := 0
loop:
	for {
		select {
		case <-ctx.Done():
			slog.Debug("producer: context closed")
			break loop
		default:
			select {
			case messages <- i:
				// message sent
				//time.Sleep(time.Duration(int64(rand.Intn(10)) * 10 * int64(time.Millisecond)))
				i++
			default:
				// message dropped
				//slog.Debug("producer: messages dropped", "message", i)
			}
		}
	}
	slog.Debug("producer: closing messages channel")
	close(messages)
	slog.Debug("producer: exit")
}

func batcher(ctx context.Context, wg *sync.WaitGroup, size int, messages <-chan any, batches chan<- []any) {
	defer wg.Done()
	// Batcher
	batch := make([]any, 0, size)
loop:
	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				slog.Debug("batcher: messages channel closed")
				messages = nil
				break loop
			}
			batch = append(batch, msg)
			if len(batch) == size {
				select {
				case batches <- batch:
					// message sent
				default:
					// message dropped
				}
				batch = make([]any, 0, size)
			}
		case <-ctx.Done():
			slog.Debug("batcher: context closed")
			break loop
		}
	}

	if len(batch) > 0 {
		select {
		case batches <- batch:
			// message sent
		default:
			// message dropped
		}
	}
	slog.Debug("batcher: closing batches channel")
	close(batches)
	slog.Debug("batcher: exit")
}

func consumer(ctx context.Context, wg *sync.WaitGroup, batches <-chan []any) {
	last := 0
	defer wg.Done()
loop:
	for {
		select {
		case <-ctx.Done():
			slog.Debug("consumer: context closed")
			break loop
		case batch, ok := <-batches:
			if !ok {
				slog.Debug("consumer: batches channel closed")
				break loop
			}

			for _, msg := range batch {
				if i, ok := msg.(int); ok {
					if i > last+1 {
						fmt.Printf("got %d, expected %d\n", i, last+1)
					}
					last = i
					if i%1_000_000 == 0 {
						fmt.Printf("%d\n", i)
					}
				}
				// fmt.Print(". ")
			}
		}
	}
	// Consumer
	// for batch := range batches {
	// 	for range batch {
	// 		fmt.Print(". ")
	// 	}
	// }
	slog.Debug("consumer: exit")
}

// kill -9 $(ps -ef | grep fastbank | grep dist | awk '{print $2}')
func main() {
	// Create channels
	messages := make(chan any, 100)
	batches := make(chan []any, 100)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	// Start goroutines
	var wg sync.WaitGroup
	wg.Add(3)

	go producer(ctx, &wg, messages)

	go batcher(ctx, &wg, 2000, messages, batches)

	go consumer(ctx, &wg, batches)

	wg.Wait()
}

// works:
// time=2024-11-19T18:09:45.462+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:22 msg="producer: context closed"
// time=2024-11-19T18:09:45.462+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:39 msg="producer: closing messages channel"
// time=2024-11-19T18:09:45.462+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:63 msg="batcher: context closed"
// time=2024-11-19T18:09:45.462+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:41 msg="producer: exit"
// time=2024-11-19T18:09:45.462+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:83 msg="batcher: closing batches channel"
// time=2024-11-19T18:09:45.462+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:94 msg="consumer: context closed"
// time=2024-11-19T18:09:45.462+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:85 msg="batcher: exit"
// time=2024-11-19T18:09:45.462+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:112 msg="consumer: exit"

// doesn't work:
// time=2024-11-19T18:10:14.838+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:94 msg="consumer: context closed"
// time=2024-11-19T18:10:14.838+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:112 msg="consumer: exit"
// time=2024-11-19T18:10:14.838+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:22 msg="producer: context closed"
// time=2024-11-19T18:10:14.838+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:39 msg="producer: closing messages channel"
// time=2024-11-19T18:10:14.838+01:00 level=DEBUG source=/data/workspaces/gomods/fastbank/main.go:41 msg="producer: exit"

/*
func sendMessages(ctx context.Context, wg *sync.WaitGroup, in chan<- any) {
	i := 0
	defer func() {
		slog.Debug("sender done")
		wg.Done()
	}()
loop:
	for {
		select {
		case <-ctx.Done():
			slog.Debug("context cancelled")
			break
		default:
			// Send messages to the input channel
			time.Sleep(time.Duration(int64(rand.Intn(10)) * int64(time.Millisecond)))
			slog.Debug("enqueuing message", "message", i)
			if ctx.Err() != nil {
				slog.Debug("context cancelled")
				break loop
			}
			in <- i
			i++
		}
	}
}

func batchMessages(ctx context.Context, wg *sync.WaitGroup, in <-chan any, out chan<- []any, batchSize int, timeout time.Duration) {
	var batch []any
	ticker := time.NewTicker(timeout)
	defer func() {
		slog.Debug("batcher done")
		ticker.Stop()
		wg.Done()
	}()

loop:
	for {
		select {
		case msg := <-in:
			slog.Debug("batching message", "message", msg)
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				slog.Debug("sending out message batch", "batch", batch)
				if ctx.Err() != nil {
					slog.Debug("processing cancelled")
					break loop
				}
				out <- batch
				batch = nil
			}
		case <-ticker.C:
			slog.Debug("timeout elapsed", "timeout", timeout)
			if len(batch) > 0 {
				slog.Debug("sending out messages batch", "batch", batch)
				if ctx.Err() != nil {
					slog.Debug("processing cancelled")
					break loop
				}
				out <- batch
				batch = nil
			}

		case <-ctx.Done():
			slog.Debug("context cancelled")
			if len(batch) > 0 {
				slog.Debug("sending out messages batch", "batch", batch)
				out <- batch
				batch = nil
			}
			break loop
		}
	}
}

func receiveMessages(ctx context.Context, wg *sync.WaitGroup, out <-chan []any) {
	defer func() {
		slog.Debug("receiver done")
		wg.Done()
	}()

loop:
	for {
		select {
		case batch := <-out:
			slog.Debug("received batch")
			for _, msg := range batch {
				slog.Debug("received message", "message", msg)
			}
		case <-ctx.Done():
			slog.Debug("context cancelled")
			break loop
		}
	}
}

func main() {
	// Example usage:
	in := make(chan any)
	out := make(chan []any)

	var wg sync.WaitGroup

	defer func() {
		slog.Debug("closing main")
		close(in)
		close(out)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	go sendMessages(ctx, &wg, in)
	wg.Add(1)

	go batchMessages(ctx, &wg, in, out, 10, 50*time.Millisecond)
	wg.Add(1)

	go receiveMessages(ctx, &wg, out)
	wg.Add(1)

	select {
	case <-time.After(5 * time.Second):
		fmt.Println("missed signal")
	case <-ctx.Done():
		//stop()
		fmt.Println("signal received")
	}
	slog.Debug("waiting for goroutines to complete")

	wg.Wait()
	//time.Sleep(20 * time.Millisecond)
}
*/
