package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func sender(ctx context.Context, wg *sync.WaitGroup, messages chan<- string) {
	defer wg.Done()
	// Producer
	for i := 0; i < 1000; i++ {
		messages <- fmt.Sprintf("Message %d", i)
	}
	close(messages)
}

func batcher(ctx context.Context, wg *sync.WaitGroup, messages <-chan string, batches chan<- []string) {
	defer wg.Done()
	// Batcher
	batch := make([]string, 0, 50)
	for msg := range messages {
		batch = append(batch, msg)
		if len(batch) == 50 {
			batches <- batch
			batch = make([]string, 0, 50)
		}
	}
	if len(batch) > 0 {
		batches <- batch
	}
	close(batches)
}

func receiver(ctx context.Context, wg *sync.WaitGroup, batches <-chan []string) {
	defer wg.Done()
	// Consumer
	for batch := range batches {
		fmt.Println(batch)
	}
}

func main() {
	// Create channels
	messages := make(chan string)
	batches := make(chan []string)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	// Start goroutines
	var wg sync.WaitGroup
	wg.Add(3)

	go sender(ctx, &wg, messages)

	go batcher(ctx, &wg, messages, batches)

	go receiver(ctx, &wg, batches)

	wg.Wait()
}

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
