// Package pipelines provides helper functions for constructing concurrent processing pipelines. Most helpers in this
// package fork a new goroutine and return a receive-only channel representing their output. Each pipeline stages
// responds to context cancellation and closure of the input channel by closing the output channel and stopping the
// forked goroutine.
//
// Channels returned by helpers in this package are unbuffered.
package pipelines

import (
	"context"
	"sync"
)

// Chan converts a slice of type T to a buffered channel which contains the values from in. The resulting channel is
// closed after its contents are drained.
func Chan[T any](in []T) <-chan T {
	result := make(chan T, len(in))
	defer close(result) // non-empty buffered channels can be drained even when closed.
	for _, t := range in {
		result <- t
	}
	return result
}

// Flatten provides a pipeline stage which converts a channel of []T to a channel of T.
func Flatten[T any](ctx context.Context, in <-chan []T) <-chan T {
	result := make(chan T)
	go func() {
		defer close(result)
		doFlatten(ctx, in, result)
	}()
	return result
}

// FlattenPool provides a pipeline stage which converts a channel of []T to a channel of T. Starts n goroutines. The
// order values are received in the output channel may differ from the order they appear in the input channel.
func FlattenPool[T any](ctx context.Context, nWorkers int, in <-chan []T) <-chan T {
	return doPooled(ctx, nWorkers, func(ctx context.Context, result chan<- T) {
		doFlatten(ctx, in, result)
	})
}

// doFlatten implements flatten with context cancellation.
func doFlatten[T any](ctx context.Context, in <-chan []T, result chan<- T) {
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-in:
			if !ok {
				return
			}
			SendAll(ctx, t, result)
		}
	}
}

// Map provides a map pipeline stage, applying the provided function to every input received from the in channel and
// sending it to the returned channel. The result channel is closed when the input channel is closed or the provided
// context is cancelled.
func Map[S, T any](ctx context.Context, in <-chan S, f func(S) T) <-chan T {
	result := make(chan T)

	go func() {
		defer close(result)
		doMap(ctx, in, f, result)
	}()
	return result
}

// MapPool provides a pooled map pipeline stage, starting n workers which each apply the provided function to every
// input received from the in channel and sending it to the returned channel. The result channel is closed when the
// input channel is closed or the provided context is cancelled.
func MapPool[S, T any](ctx context.Context, nWorkers int, in <-chan S, f func(S) T) <-chan T {
	return doPooled(ctx, nWorkers, func(ctx context.Context, result chan<- T) {
		doMap(ctx, in, f, result)
	})
}

// doMap implements map with cancellation.
func doMap[S, T any](ctx context.Context, in <-chan S, f func(S) T, result chan<- T) {
	for {
		select {
		case <-ctx.Done():
			return
		case s, ok := <-in:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case result <- f(s):
			}
		}
	}
}

// MapCtx provides a map pipeline stage, applying the provided function to every input received from the in channel,
// passing the provided context, and passing the result to a new channel which is returned. The result channel is closed
// when the input channel is closed or the provided context is cancelled.
func MapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) T) <-chan T {
	tChan := make(chan T)

	go func() {
		defer close(tChan)
		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-in:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case tChan <- f(ctx, s):
				}
			}
		}
	}()
	return tChan
}

// FlatMap provides a pipeline stage which applies the provided function to every input received from the in channel
// and passes every element of the slice returned to the output channel.
func FlatMap[S, T any](ctx context.Context, in <-chan S, f func(S) []T) <-chan T {
	tChan := make(chan T)

	go func() {
		defer close(tChan)
		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-in:
				if !ok {
					return
				}
				SendAll(ctx, f(s), tChan)
			}
		}
	}()
	return tChan
}

// FlatMapCtx provides a pipeline stage which applies the provided context-aware function to every input received from
// the in channel and passes every element of the slice returned to the output channel, which is returned.
func FlatMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) []T) <-chan T {
	tChan := make(chan T)

	go func() {
		defer close(tChan)
		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-in:
				if !ok {
					return
				}
				SendAll(ctx, f(ctx, s), tChan)
			}
		}
	}()
	return tChan
}

// Combine combines the values from two channels into a third, which is returned. The returned channel is closed once
// both of the input channels have been closed, or the provided context is cancelled.
func Combine[T any](ctx context.Context, t1 <-chan T, t2 <-chan T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		closedCount := 0
		for closedCount < 2 {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-t1:
				if !ok {
					t1 = nil
					closedCount++
					continue
				}
				out <- v
			case v, ok := <-t2:
				if !ok {
					t2 = nil
					closedCount++
					continue
				}
				out <- v
			}
		}
	}()
	return out
}

// WithCancel returns a new channel which receives each value from ch. If the provided context is cancelled or the
// input channel is closed, the returned channel is also closed.
func WithCancel[T any](ctx context.Context, ch <-chan T) <-chan T {
	result := make(chan T)
	go func() {
		defer close(result)
		for {
			select {
			case <-ctx.Done():
				return
			case t, ok := <-ch:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case result <- t:
				}
			}
		}
	}()
	return result
}

// SendAll blocks the current thread and sends all values in ts to the provided channel while handling context
// cancellation. It blocks until the channel is closed or the provided context is cancelled.
func SendAll[T any](ctx context.Context, ts []T, ch chan<- T) {
	for _, t := range ts {
		select {
		case <-ctx.Done():
			return
		case ch <- t:
		}
	}
}

// ForkMapCtx starts a pipeline stage which, for each value of T received from in, forks an invocation of f onto a
// new goroutine. Any values sent to the channel provided to f are serialized and made available in the output channel.
// To avoid leaking a goroutine, any function passed must respect context cancellation while sending values to the
// output channel.
func ForkMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S, chan<- T)) <-chan T {
	tChan := make(chan T)
	go func() {
		defer close(tChan)

		var wg sync.WaitGroup
		defer wg.Wait()

		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-in:
				if !ok {
					return
				}
				wg.Add(1)
				go func(s S) {
					defer wg.Done()
					f(ctx, s, tChan)
				}(s)
			}
		}
	}()
	return tChan
}

// Drain blocks the current goroutine and receives all values from the provided channel until it has been closed. Each
// value recieved is appended to a new slice, which is returned.
func Drain[T any](ctx context.Context, in <-chan T) []T {
	var result []T
	for {
		select {
		case <-ctx.Done():
			return result
		case repo, ok := <-in:
			if !ok {
				return result
			}
			result = append(result, repo)
		}
	}
}

// waitClose is a helper for pooled pipeline stages. Calls done on the waitgroup. If the workerID is 0, the waitgroup
// is waited on, and the channel is closed.
func waitClose[T any](workerID int, wg *sync.WaitGroup, closeMe chan T) {
	wg.Done()
	if workerID == 0 {
		wg.Wait()
		close(closeMe)
	}
}

// doPooled runs the implementation provided via doIt in a workerpool of size n. doIt must respect context cancellation.
func doPooled[T any](ctx context.Context, nWorkers int, doIt func(context.Context, chan<- T)) <-chan T {
	result := make(chan T)
	var wg sync.WaitGroup
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer waitClose(id, &wg, result)
			doIt(ctx, result)
		}(i)
	}
	return result
}
