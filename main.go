package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"math/rand"
)

func sendMessages(ctx context.Context, wg *sync.WaitGroup, in chan<- any) {
	i := 0
	defer wg.Done()
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
			in <- i
			i++
			if ctx.Err() != nil {
				slog.Debug("context cancelled")
				break loop
			}
		}
	}
}

func batchMessages(ctx context.Context, wg *sync.WaitGroup, in <-chan any, out chan<- []any, batchSize int, timeout time.Duration) {
	var batch []any
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	defer wg.Done()

loop:
	for {
		select {
		case msg := <-in:
			slog.Debug("batching message", "message", msg)
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				slog.Debug("sending out message batch", "batch", batch)
				out <- batch
				batch = nil
			}
		case <-ticker.C:
			slog.Debug("timeout elapsed", "timeout", timeout)
			if len(batch) > 0 {
				slog.Debug("sending out messages batch", "batch", batch)
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
	defer wg.Done()
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

	defer close(in)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	go batchMessages(ctx, &wg, in, out, 10, 50*time.Millisecond)
	wg.Add(1)

	go sendMessages(ctx, &wg, in)
	wg.Add(1)

	go receiveMessages(ctx, &wg, out)
	wg.Add(1)

	select {
	case <-time.After(5 * time.Second):
		fmt.Println("missed signal")
	case <-ctx.Done():
		stop()
		fmt.Println("signal received")
	}

	wg.Wait()
	//time.Sleep(20 * time.Millisecond)
}
