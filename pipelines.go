// Package pipelines provides helper functions for constructing concurrent processing pipelines.
// Each pipeline stage represents a single stage in a parallel computation, with an input channel and an output channel.
// Generally, pipeline stages have signatures starting with a context and input channel as their first arguments, and
// returning a channel, as below:
//
//	Stage[S,T any](ctx context.Context, in <-chan S, ...) <-chan T
//
// The return value from a pipeline stage is referred to as the stage's 'output' channel. Each stage is a non-blocking
// call which starts one or more goroutines which listen on the input channel and send results to the output channel.
// Goroutines started by each stage respond to context cancellation or closure of the input channel by closing its
// output channel and cleaning up all goroutines started. Many pipeline stages take a function as an argument, which
// transform the input and output in some way.
//
// By default, each pipeline starts the minimum number of threads required for its operation, and returns an unbuffered channel.
// These defaults can be modified by passing the result of WithBuffer or WithPool as optional arguments.
package pipelines

import (
	"context"
	"fmt"
	"sync"
)

// Chan converts a slice to a channel. The channel returned is a closed, buffered channel containing exactly the same
// values. Unlike other funcs in this package, Chan does not start any new goroutines.
func Chan[T any](in []T) <-chan T {
	result := make(chan T, len(in))
	defer close(result) // non-empty buffered channels can be drained even when closed.
	for _, t := range in {
		result <- t
	}
	return result
}

// Flatten starts a pipeline stage which converts a "chan []T" to a "chan T". It provides a pipeline stage which converts a channel of slices to a
// channel of scalar values. Each value contained in slices received from the input channel is sent to the output channel.
func Flatten[T any](ctx context.Context, in <-chan []T, opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doFlatten(ctx, in, out[0])
	}, configure(opts)))
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
			sendAll(ctx, t, result)
		}
	}
}

// Map starts a pipeline stage which converts a "chan S" to a "chan T", by converting each S to exactly one T. It
// applies f to every value received from the input channel and sends the result to the output channel. The output
// channel is closed whenever the input channel is closed or the provided context is cancelled.
func Map[S, T any](ctx context.Context, in <-chan S, f func(S) T, opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doMap(ctx, in, f, out[0])
	}, configure(opts)))
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

// MapCtx starts a pipeline stage which converts a "chan S" to "chan T",  by converting each S to exactly one T.
// Unlike Map, the function which performs the conversion also accepts a context.Context.
// Map applies f to every value received from its input channel and sends the result to its output channel.
// The context passed to MapCtx is passed as an argument to f, unchanged.
func MapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) T, opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doMapCtx(ctx, in, f, out[0])
	}, configure(opts)))
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

// FlatMap starts a pipeline stage which converts a "chan S" to a "chan T", by converting each S to zero or more Ts.
// It applies f to every value received from its input channel and sends all values found in the
// slice returned from f to its output channel.
func FlatMap[S, T any](ctx context.Context, in <-chan S, f func(S) []T, opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doFlatMap(ctx, in, f, out[0])
	}, configure(opts)))
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
			sendAll(ctx, f(s), out)
		}
	}
}

// FlatMapCtx starts a pipeline stage which converts a "chan S" to a "chan T", by converting each S to zero or more Ts.
// It applies f to every value received from its input channel and sends all values found in the slice returned from
// f to its output channel.
//
//	The context passed to FlatMapCtx is passed as an argument to f, unchanged.
func FlatMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) []T, opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doFlatMapCtx(ctx, in, f, out[0])
	}, configure(opts)))
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
			sendAll(ctx, f(ctx, s), out)
		}
	}
}

// Combine converts two "chan T" into a single "chan T". It sends all values received from both of its input
// channels to its output channel.
func Combine[T any](ctx context.Context, t1 <-chan T, t2 <-chan T, opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doCombine(ctx, t1, t2, out[0])
	}, configure(opts)))
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

// Tee converts a "chan T" into two "chan T", each of which receive exactly the same values, possibly in different
// orders.
// Both output channels must be drained concurrently to avoid blocking this pipeline stage.
func Tee[T any](ctx context.Context, ch <-chan T, opts ...Option) (<-chan T, <-chan T) {
	conf := configure(opts)
	conf.outputs = 2
	return return2(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doTee(ctx, ch, out[0], out[1])
	}, conf))
}

func doTee[T any](ctx context.Context, ch <-chan T, chan1, chan2 chan<- T) {
	for {
		select {
		case t, ok := <-ch:
			if !ok {
				return
			}
			select {
			case chan1 <- t:
				select {
				case chan2 <- t:
				case <-ctx.Done():
					return
				}
			case chan2 <- t:
				select {
				case chan1 <- t:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// OptionMap converts a "chan S" to a "chan T" by converting each "S" to zero or one "T"s.
// It applies f to every value received from in and sends any non-nil results to its output channel.
func OptionMap[S, T any](ctx context.Context, in <-chan S, f func(S) *T, opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doOptionMap(ctx, in, out[0], f)
	}, configure(opts)))
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

// OptionMapCtx converts a "chan S" to a "chan T" by converting each "S" to zero or one "T"s. It applies f to every
// value received from in and sends all non-nil results to its output channel. The same context passed to OptionMapCtx
// is passed as an argument to f.
func OptionMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) *T, opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doOptionMapCtx(ctx, in, out[0], f)
	}, configure(opts)))
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

// sendAll sends all values in a slice to the provided channel. It blocks until the channel is closed or the provided
// context is cancelled.
func sendAll[T any](ctx context.Context, ts []T, ch chan<- T) {
	for _, t := range ts {
		select {
		case <-ctx.Done():
			return
		case ch <- t:
		}
	}
}

// ForkMapCtx converts a "chan S" into a "chan T" by converting each S into zero or more Ts. Unlike FlatMap or FlatMapCtx,
// this func starts a new goroutine for converting each value of S.
// Each goroutine is responsible for sending its output to this channel.
// To avoid resource leaks, f must respect context cancellation when sending to its output channel.
// The same context passed to ForkMapCtx is passed to f.
//
// ForkMapCtx should be used with caution, as it introduces potentially unbounded parallelism to a pipeline computation.
//
// Variants of ForkMapCtx are intentionally omitted from this package.
// ForkMap is omitted because the caller cannot listen for context cancellation in some cases.
// ForkFlatMap is omitted because it is more efficient for the caller range over the slice and send individual values themselves.
func ForkMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S, chan<- T), opts ...Option) <-chan T {
	return return1(doWithConf(ctx, func(ctx context.Context, out ...chan T) {
		doForkMapCtx(ctx, in, f, out[0])
	}, configure(opts)))
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

// An Option is passed to optionally configure a pipeline stage.
type Option func(*config)

// WithBuffer configures a pipeline to return a buffered output channel with a buffer of the provided size.s
func WithBuffer(size int) Option {
	return func(conf *config) {
		conf.bufferSize = size
	}
}

// WithPool configures a pipeline to run the provided stage on a parallel worker pool of the given size. All workers are
// kept alive until the input channel is closed or the provided context is cancelled.
func WithPool(numWorkers int) Option {
	return func(conf *config) {
		conf.workers = numWorkers
	}
}

// WithDone configures a pipeline stage to cancel the returned context when all goroutines started by the stage
// have been stopped.
// This is appropriate for termination detection for ANY stages in a pipeline.
// To await termination of ALL stages in a pipeline, use WithWaitGroup.
func WithDone(ctx context.Context) (Option, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return func(conf *config) {
		conf.doneCancel = cancel
	}, ctx
}

// WithWaitGroup configures a pipeline stage to add a value to the provided WaitGroup for each goroutine started by the
// stage, and signal Done when each goroutine has completed. This option is appropriate for termination detection of
// ALL stages in a pipeline. To detect termination of ANY stage in a pipeline, use WithDone.
func WithWaitGroup(wg *sync.WaitGroup) Option {
	return func(conf *config) {
		conf.wg = wg
	}
}

type config struct {
	// bufferSize is the size of the buffer for every output channel created for this stage.
	bufferSize int
	// workers is the number of goroutines on which to run this stage.
	workers int
	// outputs is the number of output channels for this stage.
	outputs int
	// doNotClose is true if pipelines should not close output channels for this stage. Should be false in nearly every
	// case. Stage should provide a close function if this functionality is unused.
	doNotClose bool
	// doneCancel is a context.CancelFunc to call when this stage is halted.
	doneCancel context.CancelFunc
	// wg is a sync.WaitGroup to decrement when this stage is halted.
	wg *sync.WaitGroup
}

func makeOutputChannels[T any](c config) []chan T {
	var result []chan T
	for i := 0; i < c.outputs; i++ {
		result = append(result, make(chan T, c.bufferSize))
	}
	return result
}

// add1 calls Add(1) on the WaitGroup, if one has been configured.
func (c *config) add1() {
	if c.wg != nil {
		c.wg.Add(1)
	}
}

// done calls Done() on the waitgroup, if one has been configured
func (c *config) done() {
	if c.wg != nil {
		c.wg.Done()
	}
}

// cancel calls doneCancel, if it has been configured
func (c config) cancel() {
	if c.doneCancel != nil {
		c.doneCancel()
	}
}

func configure(opts []Option) config {
	result := config{
		bufferSize: 0,
		workers:    1,
		outputs:    1,
	}
	for _, opt := range opts {
		opt(&result)
	}
	return result
}

// doWithConf runs the implementation provided via doIt on goroutines according to the provided options.
func doWithConf[T any](ctx context.Context, doIt func(context.Context, ...chan T), conf config) []chan T {
	outs := makeOutputChannels[T](conf)
	if conf.workers == 1 {
		conf.add1()
		// run without a worker pool to avoid overhead from the WaitGroup
		go func() {
			defer func() {
				if !conf.doNotClose {
					for _, ch := range outs {
						close(ch)
					}
				}
				conf.cancel()
				conf.done()
			}()
			doIt(ctx, outs...)
		}()
	} else {
		// run on worker pool.
		var poolStopped sync.WaitGroup
		for i := 0; i < conf.workers; i++ {
			poolStopped.Add(1)
			conf.add1()
			go func(id int) {
				defer func() {
					poolStopped.Done()
					if id == 0 { // first thread closes the output channel.
						poolStopped.Wait()
						defer func() {
							if !conf.doNotClose {
								for _, ch := range outs {
									close(ch)
								}
							}
						}()
						conf.cancel()
					}
					conf.done()
				}()
				doIt(ctx, outs...)
			}(i)
		}
	}
	return outs
}

// Drain receives all values from the provided channel and returns them in a slice.
// Drain blocks the caller until the input channel is closed or the provided context is cancelled.
// An error is returned if and only if the provided context was cancelled before the input channel was closed.
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

// Reduce runs a reducer function on every input received from the in chan and returns the output. Reduce blocks the
// caller until the input channel is closed or the provided context is cancelled.
// An error is returned if and only if the provided context was cancelled before the input channel was closed.
func Reduce[S, T any](ctx context.Context, in <-chan S, f func(T, S) T) (T, error) {
	var result T
	for {
		select {
		case <-ctx.Done():
			fmt.Println("reduce closed")
			return result, ctx.Err()
		case s, ok := <-in:
			if !ok {
				return result, nil
			}
			result = f(result, s)
		}
	}
}

// ErrorSink provides an error-handling solution for pipelines created by this package. It manages a pipeline stage
// which can receive fatal and non-fatal errors that may occur during the course of a pipeline.
//
// ErrorSinks are safe to use from multiple pipeline stages concurrently. They must be closed to avoid leaking
// resources. ErrorSinks are eventually consistent. Calls to All() return as many errors as have been processed to date.
// At any point in time, there is no guarantee that all errors sent to ErrorSink have been received.
type ErrorSink struct {
	errors chan errWrapper
	cancel context.CancelFunc
	errs   []error
	lock   *sync.Mutex
}

// NewErrorSink returns a new ErrorSink, along with a context which is cancelled when a fatal error is sent to the
// ErrorSink. Starts a new, configurable pipeline stage which catches any errors reported.
func NewErrorSink(ctx context.Context, opts ...Option) (context.Context, *ErrorSink) {
	ctx, cancel := context.WithCancel(ctx)
	result := &ErrorSink{cancel: cancel, lock: &sync.Mutex{}}
	config := configure(opts)
	config.doNotClose = true

	outs := doWithConf(ctx, func(ctx context.Context, in ...chan errWrapper) {
		result.doErrSink(ctx, in[0])
	}, config)
	result.errors = outs[0]

	return ctx, result
}

func (s *ErrorSink) doErrSink(ctx context.Context, errors chan errWrapper) {
	for {
		select {
		case <-ctx.Done():
			return
		case werr := <-errors:
			s.appendErr(werr.err)
			if werr.isFatal {
				s.cancel()
			}
		}
	}
}

func (s *ErrorSink) appendErr(err error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.errs = append(s.errs, err)
}

// Fatal sends a fatal error to this ErrorSink, cancelling the child context which was created by NewErrorSink,
// as well as reporting this error.
func (s *ErrorSink) Fatal(err error) {
	s.errors <- errWrapper{isFatal: true, err: err}
}

// Error sends a non-fatal error to this ErrorSink, which is reported and included along with All()
func (s *ErrorSink) Error(err error) {
	s.errors <- errWrapper{isFatal: false, err: err}
}

// Close closes the channel used to send errors for this ErrorSink. Each ErrorSink must be closed to avoid resources
// leaks
func (s *ErrorSink) Close() {
	close(s.errors)
}

// All returns all errors which have been received by this ErrorSink so far. Subsequent calls to All can return strictly
// more errors, but will never return fewer errors. While all errors sent to an ErrorSink eventually end up being
// reported, there is no timeframe within which they are all guaranteed to be available.
func (s *ErrorSink) All() []error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.errs
}

type errWrapper struct {
	isFatal bool
	err     error
}

func return1[T any](chans []chan T) <-chan T {
	return chans[0]
}

func return2[T any](chans []chan T) (<-chan T, <-chan T) {
	return chans[0], chans[1]
}
