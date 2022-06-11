// Package pipelines provides helper functions for constructing concurrent processing pipelines.
// Each pipeline stage represents a single stage in a parallel computation, with an input channel and an output channel.
// Generally, pipeline stages have signatures starting with a context and input channel as their first arguments, and returning a channel.
// The return value from a pipeline stage is referred to as the stage's 'output' channel.
//
//   Stage[S,T any](ctx context.Context, in <-chan S, ...) <-chan T
//
// Many pipeline stages take a function as an argument, these are run on one or more goroutines, which are managed by this package.
// Each stage responds to context cancellation or closure of its input channel by closing its output channel and cleaning up all goroutines it manages.
//
//
// By default, each pipeline starts the minimum number of threads required, and returns an unbuffered channel.
// For performance tuning, pipeline stages in this package can be optionally configured to run on multiple threads or use a buffered output channel.
//
// A pipeline stage can be run on a worker pool by passing pipelines.WithPool(n), where n is the number of threads to
// run the pipeline. A stage can be configured to use a buffered output channel by pasing pipelines.WithBuffer(n), where
// n is the desired size of the buffered channel.
package pipelines

import (
	"context"
	"sync"
)

type config[T any] struct {
	Channer func() chan T
	Workers int
}

// OptionFunc provides options for configuring pipeline stages.
type OptionFunc[T any] func(*config[T])

// WithBuffer configures a pipeline to use a buffered input channel.
func WithBuffer[T any](size int) OptionFunc[T] {
	return func(conf *config[T]) {
		conf.Channer = func() chan T {
			return make(chan T, size)
		}
	}
}

// WithPool configures a pipeline to run the provided stage on a worker pool of the given size.
func WithPool[T any](numWorkers int) OptionFunc[T] {
	return func(conf *config[T]) {
		conf.Workers = numWorkers
	}
}

func configure[T any](opts []OptionFunc[T]) config[T] {
	result := config[T]{
		Channer: func() chan T { return make(chan T) },
		Workers: 1,
	}
	for _, opt := range opts {
		opt(&result)
	}
	return result
}

// Chan converts a slice of type T to a buffered channel containing the same values.
func Chan[T any](in []T) <-chan T {
	result := make(chan T, len(in))
	defer close(result) // non-empty buffered channels can be drained even when closed.
	for _, t := range in {
		result <- t
	}
	return result
}

// Flatten provides a pipeline stage which converts a channel of slices to a channel of scalar values.
// Each value contained in slices received from the input channel is sent to the output channel.
func Flatten[T any](ctx context.Context, in <-chan []T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doFlatten(ctx, in, out)
	})
}

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

// Map applies f to every value received from the input channel and sends the result to the output channel.
// The output channel is closed when the input channel is closed or the provided context is cancelled.
func Map[S, T any](ctx context.Context, in <-chan S, f func(S) T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doMap(ctx, in, f, out)
	})
}

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

// MapCtx applies f to every value received from its input channel and sends the result to its output channel.
// The same context passed to MapCtx is passed as an argument to f.
func MapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doMapCtx(ctx, in, f, out)
	})
}

func doMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) T, result chan<- T) {
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
			case result <- f(ctx, s):
			}
		}
	}
}

// FlatMap applies f to every value received from its input channel and sends all values found in the slice returned from
// f to its output channnel.
func FlatMap[S, T any](ctx context.Context, in <-chan S, f func(S) []T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doFlatMap(ctx, in, f, out)
	})
}

func doFlatMap[S, T any](ctx context.Context, in <-chan S, f func(S) []T, out chan<- T) {
	for {
		select {
		case <-ctx.Done():
			return
		case s, ok := <-in:
			if !ok {
				return
			}
			SendAll(ctx, f(s), out)
		}
	}
}

// FlatMapCtx applies f to every value received from its input channel and sends all values found in the slice returned from
// f to its output channnel.
// The same context passed to FlatMapCtx is passed as an argument to f.
func FlatMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) []T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doFlatMapCtx(ctx, in, f, out)
	})
}

func doFlatMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) []T, out chan<- T) {
	for {
		select {
		case <-ctx.Done():
			return
		case s, ok := <-in:
			if !ok {
				return
			}
			SendAll(ctx, f(ctx, s), out)
		}
	}
}

// Combine sends all values received from both of its input channels to its output channel.
func Combine[T any](ctx context.Context, t1 <-chan T, t2 <-chan T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doCombine(ctx, t1, t2, out)
	})
}

func doCombine[T any](ctx context.Context, t1 <-chan T, t2 <-chan T, out chan<- T) {
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
}

// WithCancel passes each value received from its input channel to its output channel.
// If the provided context is cancelled or the input channel is closed, the output channel is also closed.
func WithCancel[T any](ctx context.Context, ch <-chan T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doWithCancel(ctx, ch, out)
	})
}

func doWithCancel[T any](ctx context.Context, ch <-chan T, out chan<- T) {
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
			case out <- t:
			}
		}
	}
}

// OptionMap applies f to every value received from in and sends all non-nil results to its output channel.
func OptionMap[S, T any](ctx context.Context, in <-chan S, f func(S) *T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doOptionMap(ctx, in, out, f)
	})
}

func doOptionMap[S, T any](ctx context.Context, in <-chan S, out chan<- T, f func(S) *T) {
	for {
		select {
		case <-ctx.Done():
			return
		case s, ok := <-in:
			if !ok {
				return
			}
			t := f(s)
			if t == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case out <- *t:
			}
		}
	}
}

// OptionMapCtx applies f to every value received from in and sends all non-nil results to its output channel.
// The same context passed to OptionMapCtx is passed as an argument to f.
func OptionMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) *T, opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doOptionMapCtx(ctx, in, out, f)
	})
}

func doOptionMapCtx[S, T any](ctx context.Context, in <-chan S, out chan<- T, f func(context.Context, S) *T) {
	for {
		select {
		case <-ctx.Done():
			return
		case s, ok := <-in:
			if !ok {
				return
			}
			t := f(ctx, s)
			if t == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case out <- *t:
			}
		}
	}
}

// SendAll sends all values in a slice to the provided channel. It blocks until the channel is closed or the provided
// context is cancelled.
func SendAll[T any](ctx context.Context, ts []T, ch chan<- T) {
	for _, t := range ts {
		select {
		case <-ctx.Done():
			return
		case ch <- t:
		}
	}
}

// ForkMapCtx forks an invocation of f onto a new goroutine for each value received from in.
// f is passed the output channel directly, and is expected responsible to send its output to this channel.
// To avoid resource leaks, f must respect context cancellation while sending to this channel.
//
// ForkMap should be used with caution, as it introduces potentially unbounded parallelism to a pipeline computation.
func ForkMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S, chan<- T), opts ...OptionFunc[T]) <-chan T {
	return doWithConf(ctx, configure(opts), func(ctx context.Context, out chan<- T) {
		doForkMapCtx(ctx, in, f, out)
	})
}

func doForkMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S, chan<- T), out chan<- T) {
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
				f(ctx, s, out)
			}(s)
		}
	}
}

// Drain receives all values from the provided channel, blocking until the channel has been drained. All values received
// are provided in a slice to the caller.
func Drain[T any](ctx context.Context, in <-chan T) ([]T, error) {
	var result []T
	for {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case repo, ok := <-in:
			if !ok {
				return result, nil
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

// doWithConf runs the implementation provided via doIt on goroutines according to the parameters in the provided
// config.
func doWithConf[T any](ctx context.Context, conf config[T], doIt func(context.Context, chan<- T)) <-chan T {
	out := conf.Channer()
	if conf.Workers == 1 {
		go func() {
			defer close(out)
			doIt(ctx, out)
		}()
	} else {
		var wg sync.WaitGroup
		for i := 0; i < conf.Workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer waitClose(id, &wg, out)
				doIt(ctx, out)
			}(i)
		}
	}
	return out
}

// Reduce runs a reducer function on every input received from the in chan and returns the output
func Reduce[S, T string](ctx context.Context, in <-chan S, f func(T, S) T) T {
	var result T
	for {
		select {
		case <-ctx.Done():
			return result
		case s, ok := <-in:
			if !ok {
				return result
			}
			result = f(result, s)
		}
	}
}
