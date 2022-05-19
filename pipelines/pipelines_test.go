package pipelines_test

import (
	"context"
	"fmt"
	"github.com/matryer/is"
	"github.com/splunk/go-genlib/pipelines"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestFlatMapCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	double := func(ctx context.Context, x int) []string {
		return []string{fmt.Sprintf("%d", x), fmt.Sprintf("%d!", x)}
	}
	t.Run("maps and serializes output", func(t *testing.T) {
		is := is.New(t)
		in := pipelines.Chan([]int{1, 2, 3})
		ch := pipelines.FlatMapCtx(ctx, in, double)

		out := drain(t, ch)
		is.Equal([]string{"1", "1!", "2", "2!", "3", "3!"}, out)
	})

	testClosesOnClose(t, func(ctx context.Context, s <-chan int) <-chan string {
		return pipelines.FlatMapCtx(ctx, s, double)
	})
	testClosesOnContextDone(t, func(ctx context.Context, s <-chan int) <-chan string {
		return pipelines.FlatMapCtx(ctx, s, double)
	})
}

func TestFlatMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	double := func(x int) []int {
		return []int{x, x * 2}
	}
	t.Run("maps and serializes output", func(t *testing.T) {
		is := is.New(t)
		in := pipelines.Chan([]int{1, 2, 3, 4, 5})
		ch := pipelines.FlatMap(ctx, in, double)

		out := drain(t, ch)
		is.Equal([]int{1, 2, 2, 4, 3, 6, 4, 8, 5, 10}, out)
	})

	testClosesOnClose(t, func(ctx context.Context, s <-chan int) <-chan int {
		return pipelines.FlatMap(ctx, s, double)
	})
	testClosesOnContextDone(t, func(ctx context.Context, s <-chan int) <-chan int {
		return pipelines.FlatMap(ctx, s, double)
	})
}

func TestCombine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Parallel()

	t.Run("combines values", func(t *testing.T) {
		is := is.New(t)
		ch1 := pipelines.Chan([]string{"1", "2", "3"})
		ch2 := pipelines.Chan([]string{"4", "5", "6"})

		ch := pipelines.Combine(ctx, ch1, ch2)
		out := drain(t, ch)
		sort.Strings(out) // values may arrive out-of-order
		is.Equal([]string{"1", "2", "3", "4", "5", "6"}, out)
	})

	closed := make(chan int)
	close(closed)
	// each argument channel must be tested separately
	testClosesOnClose(t, func(ctx context.Context, in1 <-chan int) <-chan int {
		return pipelines.Combine(ctx, in1, closed)
	})
	testClosesOnClose(t, func(ctx context.Context, in2 <-chan int) <-chan int {
		return pipelines.Combine(ctx, closed, in2)
	})
	testClosesOnContextDone(t, func(ctx context.Context, in1 <-chan int) <-chan int {
		return pipelines.Combine(ctx, in1, closed)
	})
	testClosesOnContextDone(t, func(ctx context.Context, in2 <-chan int) <-chan int {
		return pipelines.Combine(ctx, closed, in2)
	})
}

func TestForkMapCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	triplify := func(ctx context.Context, id int, out chan<- int) {
		for i := 0; i < 3; i++ { // map each input to the range [3*id, 3*id+2]
			out <- 3*id + i
		}
	}

	t.Run("maps all values", func(t *testing.T) {
		is := is.New(t)

		in := pipelines.Chan([]int{0, 1, 2})
		ch := pipelines.ForkMapCtx(ctx, in, triplify)
		out := drain(t, ch)
		sort.Ints(out) // values may arrive out-of-order
		is.Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8}, out)
	})
	testClosesOnClose(t, func(ctx context.Context, in <-chan int) <-chan int {
		return pipelines.ForkMapCtx(ctx, in, triplify)
	})
	testClosesOnContextDone(t, func(ctx context.Context, in <-chan int) <-chan int {
		return pipelines.ForkMapCtx(ctx, in, triplify)
	})
}

func TestMapCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	lenCtx := func(ctx context.Context, s string) int {
		return len(s)
	}
	t.Run("maps all values", func(t *testing.T) {
		is := is.New(t)
		in := pipelines.Chan([]string{"ab", "abcd", "abcdef", ""})
		ch := pipelines.MapCtx(ctx, in, lenCtx)

		out := drain(t, ch)
		is.Equal([]int{2, 4, 6, 0}, out)
	})

	testClosesOnContextDone(t, func(ctx context.Context, in <-chan string) <-chan int {
		return pipelines.MapCtx(ctx, in, lenCtx)
	})
	testClosesOnClose(t, func(ctx context.Context, in <-chan string) <-chan int {
		return pipelines.MapCtx(ctx, in, lenCtx)
	})
}

func TestMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	t.Run("maps all values", func(t *testing.T) {
		is := is.New(t)

		in := pipelines.Chan([]int{1, 2, 3, 4, 5})
		ch := pipelines.Map(ctx, in, strconv.Itoa)

		out := drain(t, ch)
		is.Equal([]string{"1", "2", "3", "4", "5"}, out)
	})

	testClosesOnContextDone(t, func(ctx context.Context, in <-chan int) <-chan string {
		return pipelines.Map(ctx, in, strconv.Itoa)
	})
	testClosesOnClose(t, func(ctx context.Context, in <-chan int) <-chan string {
		return pipelines.Map(ctx, in, strconv.Itoa)
	})
}

func TestFlatten(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	t.Run("flattens all values", func(t *testing.T) {
		is := is.New(t)

		in := pipelines.Chan([][]int{{1, 2, 3}, {4, 5, 6}, {}, {7}})
		ch := pipelines.Flatten(ctx, in)

		output := drain(t, ch)
		is.Equal([]int{1, 2, 3, 4, 5, 6, 7}, output)
	})

	testClosesOnContextDone(t, pipelines.Flatten[int])
	testClosesOnClose(t, pipelines.Flatten[string])
}

func TestChan(t *testing.T) {
	t.Parallel()

	t.Run("forwards all values", func(t *testing.T) {
		is := is.New(t)

		expected := []int{1, 2, 3, 4}
		ch := pipelines.Chan(expected)
		out := drain(t, ch)
		is.Equal(expected, out)
	})

	t.Run("closes after drain", func(t *testing.T) {
		ch := pipelines.Chan([]int{1, 2, 3, 4})
		testClosesAfterDrain(t, ch)
	})

	t.Run("closes on empty", func(t *testing.T) {
		is := is.New(t)
		ch := pipelines.Chan[int](nil)
		out := drain(t, ch)
		is.True(len(out) == 0)
	})
}

func TestSendAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	t.Run("closes on empty", func(t *testing.T) {
		is := is.New(t)

		in := make(chan string)
		go func() {
			pipelines.SendAll(ctx, nil, in)
			close(in)
		}()
		out := drain(t, in)
		is.True(len(out) == 0)
	})

	t.Run("forwards all values", func(t *testing.T) {
		is := is.New(t)

		in := make(chan string)
		expected := []string{"1", "2", "3", "4"}
		go func() {
			pipelines.SendAll(ctx, expected, in)
			close(in)
		}()
		out := drain(t, in)
		is.Equal(expected, out)
	})

	t.Run("returns on cancelled context", func(t *testing.T) {
		in := make(chan int)
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		go func() {
			pipelines.SendAll(ctx, []int{1, 2, 3}, in)
			close(in) // if SendAll blocks, channel is never closed.
		}()
		testClosesAfterDrain(t, in)
	})
}

func TestWithCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Parallel()
	t.Run("result channel closes on context done", func(t *testing.T) {
		in := make(chan int)
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		ch := pipelines.WithCancel(ctx, in)

		testClosesAfterDrain(t, ch)
	})
	t.Run("forwards all values", func(t *testing.T) {
		is := is.New(t)

		in := pipelines.Chan([]string{"1", "2", "3", "4", "5"})
		ch := pipelines.WithCancel(ctx, in)

		result := drain(t, ch)
		is.Equal(result, []string{"1", "2", "3", "4", "5"})
	})
	t.Run("result channel closes on closed input channel", func(t *testing.T) {
		in := make(chan int)
		close(in)
		ch := pipelines.WithCancel(ctx, in)

		testClosesAfterDrain(t, ch)
	})
}

func TestDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()
	t.Run("receives all values", func(t *testing.T) {
		is := is.New(t)

		in := pipelines.Chan([]int{1, 2, 34, 5, 6, 7, 8, 9})

		result := pipelines.Drain(ctx, in)
		is.Equal(result, []int{1, 2, 34, 5, 6, 7, 8, 9})
	})
	t.Run("halts on closed input channel", func(t *testing.T) {
		is := is.New(t)

		in := make(chan string)
		close(in)

		result := pipelines.Drain(ctx, in)
		is.Equal(len(result), 0)
	})

	t.Run("halts on done context", func(t *testing.T) {
		is := is.New(t)

		in := make(chan complex64)
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		
		result := pipelines.Drain(ctx, in)
		is.Equal(len(result), 0)
	})
}

func testClosesOnClose[S, T any](t *testing.T, stage func(context.Context, <-chan S) <-chan T) {
	t.Run("closes on closed input channel", func(t *testing.T) {
		in := make(chan S)
		close(in)
		out := stage(context.Background(), in)
		testClosesAfterDrain(t, out)
	})
}

// testClosesOnContextDone tests that a pipeline stage closes its output channel when the context is closed.
func testClosesOnContextDone[S, T any](t *testing.T, stage func(context.Context, <-chan S) <-chan T) {
	t.Run("result channel closes on context done", func(t *testing.T) {
		in := make(chan S)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out := stage(ctx, in)
		testClosesAfterDrain(t, out)
	})
}

// testClosesAfterDrain blocks and tests whether the provided channel is closed after being drained. Fails after 1
// second if the channel remains open with no value being received.
func testClosesAfterDrain[T any](t *testing.T, ch <-chan T) {
	for {
		select {
		case <-time.After(1 * time.Second):
			t.Error("channel not cancelled: timed out")
			return
		case _, ok := <-ch: // eventually the channel is drained and should close
			if !ok {
				return
			}
		}
	}
}

// drain blocks and drains the provided channel, returning the sequence of values received as a slice. Returns once the
// channel has been closed.
func drain[T any](t *testing.T, ch <-chan T) []T {
	var result []T
	for {
		select {
		case <-time.After(1 * time.Second):
			t.Error("unable to drain: timed out")
			return nil
		case t, ok := <-ch:
			if !ok {
				return result
			}
			result = append(result, t)
		}
	}
}

func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := pipelines.Chan([]int{1, 3, 5})
	doubled := pipelines.FlatMap(ctx, input, func(x int) []int { return []int{x, x + 1} })         // (x) => [x, x+1]
	expanded := pipelines.Map(ctx, doubled, func(x int) int { return x * 2 })                      // x => x*2
	exclaimed := pipelines.Map(ctx, expanded, func(x int) string { return fmt.Sprintf("%d!", x) }) // x => "${x}!"

	for out := range exclaimed {
		fmt.Print(out, " ")
	}

	// Output: 2! 4! 6! 8! 10! 12!
}
