package pipelines_test

import (
	"context"
	"fmt"
	"github.com/matryer/is"
	"github.com/splunk/pipelines"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// opts provides a battery of tests configurations in basic combinations.
func opts() optionMap {
	doneOpt, _ := pipelines.WithDone(context.Background())
	return optionMap{
		"default":                 {},
		"pooled 3":                {pipelines.WithPool(3)},
		"buffered 10":             {pipelines.WithBuffer(10)},
		"waitgroup":               {pipelines.WithWaitGroup(&sync.WaitGroup{})},
		"pooled 5+waitgroup":      {pipelines.WithWaitGroup(&sync.WaitGroup{}), pipelines.WithPool(5)},
		"pooled 5+waitgroup+done": {pipelines.WithWaitGroup(&sync.WaitGroup{}), pipelines.WithPool(5), doneOpt},
	}
}

func TestFlatMapCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	double := func(ctx context.Context, x int) []string {
		return []string{fmt.Sprintf("%d", x), fmt.Sprintf("%d!", x)}
	}
	t.Run("maps and serializes output", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)
			in := pipelines.Chan([]int{1, 2, 3})
			ch := pipelines.FlatMapCtx(ctx, in, double, opts...)

			out := drain(t, ch)
			sort.Strings(out)
			is.Equal([]string{"1", "1!", "2", "2!", "3", "3!"}, out)
		})
	})

	optStage := func(ctx context.Context, opt pipelines.Option, s <-chan int) <-chan string {
		return pipelines.FlatMapCtx(ctx, s, double, opt)
	}
	testWithDone(t, optStage)
	testWithWaitGroup(t, optStage)

	stage := func(ctx context.Context, s <-chan int) <-chan string {
		return pipelines.FlatMapCtx(ctx, s, double)
	}
	testClosesOnClose(t, stage)
	testClosesOnContextDone(t, stage)
}

func TestFlatMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	double := func(x int) []int {
		return []int{x, x * 2}
	}
	t.Run("maps and serializes output", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)
			in := pipelines.Chan([]int{1, 2, 3, 4, 5})
			ch := pipelines.FlatMap(ctx, in, double, opts...)

			out := drain(t, ch)
			sort.Ints(out) //
			is.Equal([]int{1, 2, 2, 3, 4, 4, 5, 6, 8, 10}, out)
		})
	})

	optStage := func(ctx context.Context, opt pipelines.Option, s <-chan int) <-chan int {
		return pipelines.FlatMap(ctx, s, double, opt)
	}
	testWithDone(t, optStage)
	testWithWaitGroup(t, optStage)

	stage := func(ctx context.Context, s <-chan int) <-chan int {
		return pipelines.FlatMap(ctx, s, double)
	}
	testClosesOnClose(t, stage)
	testClosesOnContextDone(t, stage)
}

func TestCombine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Parallel()

	t.Run("combines values", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)
			ch1 := pipelines.Chan([]string{"1", "2", "3"})
			ch2 := pipelines.Chan([]string{"4", "5", "6"})

			ch := pipelines.Combine(ctx, ch1, ch2, opts...)
			out := drain(t, ch)
			sort.Strings(out) // values may arrive out-of-order
			is.Equal([]string{"1", "2", "3", "4", "5", "6"}, out)
		})
	})

	closed := make(chan int)
	close(closed)

	// each argument channel must be tested separately
	optStage1 := func(ctx context.Context, opt pipelines.Option, in1 <-chan int) <-chan int {
		return pipelines.Combine(ctx, in1, closed, opt)
	}
	optStage2 := func(ctx context.Context, opt pipelines.Option, in2 <-chan int) <-chan int {
		return pipelines.Combine(ctx, closed, in2, opt)
	}
	testWithDone(t, optStage1)
	testWithDone(t, optStage2)
	testWithWaitGroup(t, optStage1)
	testWithWaitGroup(t, optStage2)

	stage1 := func(ctx context.Context, in1 <-chan int) <-chan int {
		return pipelines.Combine(ctx, in1, closed)
	}
	stage2 := func(ctx context.Context, in2 <-chan int) <-chan int {
		return pipelines.Combine(ctx, closed, in2)
	}
	testClosesOnClose(t, stage1)
	testClosesOnClose(t, stage2)
	testClosesOnContextDone(t, stage1)
	testClosesOnContextDone(t, stage2)
}

func TestTee(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	t.Run("forks values", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)

			in := pipelines.Chan([]int{0, 1, 2, 3})
			ch1, ch2 := pipelines.Tee(ctx, in, opts...)
			var out1, out2 []int
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				out1 = drain(t, ch1) // both channels must be drained simultaneously.
			}()
			out2 = drain(t, ch2)
			wg.Wait()

			sort.Ints(out1)
			sort.Ints(out2)
			is.Equal([]int{0, 1, 2, 3}, out1)
			is.Equal([]int{0, 1, 2, 3}, out2)
		})
	})
	t.Run("closes on closed input channel", func(t *testing.T) {
		in := make(chan string)
		close(in)
		out1, out2 := pipelines.Tee(context.Background(), in)
		testClosesAfterDrain(t, out1)
		testClosesAfterDrain(t, out2)
	})
	t.Run("result channel closes on context done", func(t *testing.T) {
		in := make(chan int)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		out1, out2 := pipelines.Tee(ctx, in)
		testClosesAfterDrain(t, out1)
		testClosesAfterDrain(t, out2)
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
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)

			in := pipelines.Chan([]int{0, 1, 2})
			ch := pipelines.ForkMapCtx(ctx, in, triplify, opts...)
			out := drain(t, ch)
			sort.Ints(out) // values may arrive out-of-order
			is.Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8}, out)
		})
	})
	optStage := func(ctx context.Context, opt pipelines.Option, in <-chan int) <-chan int {
		return pipelines.ForkMapCtx(ctx, in, triplify, opt)
	}
	testWithDone(t, optStage)
	testWithWaitGroup(t, optStage)

	stage := func(ctx context.Context, in <-chan int) <-chan int {
		return pipelines.ForkMapCtx(ctx, in, triplify)
	}
	testClosesOnClose(t, stage)
	testClosesOnContextDone(t, stage)
}

func TestMapCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	lenCtx := func(ctx context.Context, s string) int {
		return len(s)
	}
	t.Run("maps all values", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)
			in := pipelines.Chan([]string{"ab", "abcd", "abcdef", ""})
			ch := pipelines.MapCtx(ctx, in, lenCtx, opts...)

			out := drain(t, ch)
			sort.Ints(out)
			is.Equal([]int{0, 2, 4, 6}, out)
		})
	})

	optStage := func(ctx context.Context, opt pipelines.Option, in <-chan string) <-chan int {
		return pipelines.MapCtx(ctx, in, lenCtx, opt)
	}
	testWithDone(t, optStage)
	testWithWaitGroup(t, optStage)

	stage := func(ctx context.Context, in <-chan string) <-chan int {
		return pipelines.MapCtx(ctx, in, lenCtx)
	}
	testClosesOnContextDone(t, stage)
	testClosesOnClose(t, stage)
}

func TestMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	t.Run("maps all values", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)

			in := pipelines.Chan([]int{1, 2, 3, 4, 5})
			ch := pipelines.Map(ctx, in, strconv.Itoa, opts...)

			out := drain(t, ch)
			sort.Strings(out)
			is.Equal([]string{"1", "2", "3", "4", "5"}, out)
		})
	})
	optStage := func(ctx context.Context, opt pipelines.Option, in <-chan int) <-chan string {
		return pipelines.Map(ctx, in, strconv.Itoa, opt)
	}
	testWithDone(t, optStage)
	testWithWaitGroup(t, optStage)
	stage := func(ctx context.Context, in <-chan int) <-chan string {
		return pipelines.Map(ctx, in, strconv.Itoa)
	}
	testClosesOnContextDone(t, stage)
	testClosesOnClose(t, stage)
}

func TestOptionMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	evenStrConv := func(i int) *string {
		if i%2 == 0 {
			x := strconv.Itoa(i)
			return &x
		}
		return nil
	}

	t.Run("maps and filters all values", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)

			in := pipelines.Chan([]int{1, 2, 3, 4, 5, 6})
			ch := pipelines.OptionMap(ctx, in, evenStrConv, opts...)

			out := drain(t, ch)
			sort.Strings(out)
			is.Equal([]string{"2", "4", "6"}, out)
		})
	})
	optStage := func(ctx context.Context, opt pipelines.Option, in <-chan int) <-chan string {
		return pipelines.OptionMap(ctx, in, evenStrConv, opt)
	}
	testWithDone(t, optStage)
	testWithWaitGroup(t, optStage)

	stage := func(ctx context.Context, in <-chan int) <-chan string {
		return pipelines.OptionMap(ctx, in, evenStrConv)
	}
	testClosesOnContextDone(t, stage)
	testClosesOnClose(t, stage)
}

func TestOptionMapCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	lenCtx := func(ctx context.Context, s string) *int {
		n := len(s)
		if n%2 == 0 {
			return &n
		}
		return nil
	}
	t.Run("maps all values", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)
			in := pipelines.Chan([]string{"ab", "a", "abcd", "abcdef", "abc", ""})
			ch := pipelines.OptionMapCtx(ctx, in, lenCtx, opts...)

			out := drain(t, ch)
			sort.Ints(out)
			is.Equal([]int{0, 2, 4, 6}, out)
		})
	})

	optStage := func(ctx context.Context, opt pipelines.Option, in <-chan string) <-chan int {
		return pipelines.OptionMapCtx(ctx, in, lenCtx, opt)
	}
	testWithDone(t, optStage)
	testWithWaitGroup(t, optStage)

	stage := func(ctx context.Context, in <-chan string) <-chan int {
		return pipelines.OptionMapCtx(ctx, in, lenCtx)
	}
	testClosesOnContextDone(t, stage)
	testClosesOnClose(t, stage)
}

func TestFlatten(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	t.Run("flattens all values", func(t *testing.T) {
		withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
			is := is.New(t)

			in := pipelines.Chan([][]int{{1, 2, 3}, {4, 5, 6}, {}, {7}})
			ch := pipelines.Flatten(ctx, in, opts...)

			output := drain(t, ch)
			sort.Ints(output)
			is.Equal([]int{1, 2, 3, 4, 5, 6, 7}, output)
		})
	})

	optStage := func(ctx context.Context, opt pipelines.Option, s <-chan []int) <-chan int {
		return pipelines.Flatten(ctx, s, opt)
	}
	testWithDone(t, optStage)
	testWithWaitGroup(t, optStage)

	stage := func(ctx context.Context, s <-chan []int) <-chan int {
		return pipelines.Flatten(ctx, s)
	}
	testClosesOnContextDone(t, stage)
	testClosesOnClose(t, stage)
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

func TestDrain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()
	t.Run("receives all values", func(t *testing.T) {
		is := is.New(t)

		in := pipelines.Chan([]int{1, 2, 34, 5, 6, 7, 8, 9})

		result, err := pipelines.Drain(ctx, in)
		is.Equal(err, nil)
		is.Equal(result, []int{1, 2, 34, 5, 6, 7, 8, 9})
	})
	t.Run("halts on closed input channel", func(t *testing.T) {
		is := is.New(t)

		in := make(chan string)
		close(in)

		result, err := pipelines.Drain(ctx, in)
		is.Equal(err, nil)
		is.Equal(len(result), 0)
	})

	t.Run("halts on done context", func(t *testing.T) {
		is := is.New(t)

		in := make(chan complex64)
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		result, err := pipelines.Drain(ctx, in)
		is.Equal(err.Error(), "context canceled")
		is.Equal(len(result), 0)
	})
}

func TestReduce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Parallel()

	reducer := func(a, b string) string {
		return a + b
	}

	t.Run("returns reduced value", func(t *testing.T) {
		is := is.New(t)

		in := pipelines.Chan([]string{"l", "3", "3", "7"})
		result, err := pipelines.Reduce(ctx, in, reducer)
		is.Equal(err, nil)
		is.Equal(result, "l337")

		emptyIn := pipelines.Chan([]string{})
		emptyResult, err := pipelines.Reduce(ctx, emptyIn, reducer)
		is.Equal(err, nil)
		is.Equal(emptyResult, "")
	})

	t.Run("stops early on done context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		is := is.New(t)

		in := pipelines.Chan([]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0"})
		result, err := pipelines.Reduce(ctx, in, reducer)
		is.Equal(err.Error(), "context canceled")
		is.True(result != "1234567890") // technically possible for this to happen, but very unlikely.
	})

	t.Run("returns empty string on closed channel", func(t *testing.T) {
		is := is.New(t)

		in := make(chan string)
		close(in)

		result, err := pipelines.Reduce(ctx, in, reducer)
		is.Equal(err, nil)
		is.Equal(result, "")
	})
}

func TestWithWaitGroup(t *testing.T) {

	t.Run("multi-stage single call", func(t *testing.T) {
		ctx := context.Background()
		var wg sync.WaitGroup

		opt := pipelines.WithWaitGroup(&wg)
		in := pipelines.Chan([]int{1, 2, 3, 4, 5, 6})

		out1 := pipelines.Map(ctx, in, inc, opt)
		out2 := pipelines.Map(ctx, out1, inc, opt)

		testClosesAfterDrain(t, out2)

		// validate WaitGroup hits zero
		complete := make(chan struct{})
		go func() {
			wg.Wait()
			close(complete)
		}()

		select {
		case <-time.After(1 * time.Second):
			t.Errorf("done not cancelled: timed out")
		case <-complete:
			return
		}
	})
}

func TestErrorSink(t *testing.T) {
	withOptions(t, opts(), func(t *testing.T, opts []pipelines.Option) {
		is := is.New(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx, errs := pipelines.NewErrorSink(ctx, opts...)

		ints := pipelines.Chan([]int{1, 2, 3, 4})

		strs := pipelines.Map(ctx, ints, func(x int) string {
			errs.Error(fmt.Errorf("err%d", x))
			return fmt.Sprintf("%d!", x)
		})
		_, err := pipelines.Reduce(ctx, strs, func(x int, str string) int {
			errs.Error(fmt.Errorf("%s", str))
			return 0
		})

		all := errs.All()
		is.Equal(err, nil)
		rgx := regexp.MustCompile(`\d!|err\d`)
		for _, err := range all { // not all errors will be reported; every error that does should match
			is.True(rgx.MatchString(err.Error()))
		}
	})

	t.Run("fatal errors cancel returned context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ctx, errs := pipelines.NewErrorSink(ctx)
		errs.Fatal(fmt.Errorf("a bad thing"))
		select {
		case <-time.After(1 * time.Second):
			t.Errorf("done not cancelled; timed out")
		case <-ctx.Done():
			return
		}
	})
}

func testWithWaitGroup[S, T any](t *testing.T, stage func(context.Context, pipelines.Option, <-chan S) <-chan T) {
	t.Helper()
	t.Run("WithWaitGroup", func(t *testing.T) {
		ctx := context.Background()
		var wg sync.WaitGroup

		in := make(chan S)
		out := stage(ctx, pipelines.WithWaitGroup(&wg), in)
		close(in)
		testClosesAfterDrain(t, out)

		// validate WaitGroup has hit zero
		complete := make(chan struct{})
		go func() {
			wg.Wait()
			close(complete)
		}()

		select {
		case <-time.After(1 * time.Second):
			t.Errorf("done not cancelled: timed out")
		case <-complete:
			return
		}
	})
}

func testWithDone[S, T any](t *testing.T, stage func(context.Context, pipelines.Option, <-chan S) <-chan T) {
	t.Helper()
	t.Run("WithDone", func(t *testing.T) {
		in := make(chan S)
		opt, ctx := pipelines.WithDone(context.Background())
		out := stage(ctx, opt, in)
		close(in)
		testClosesAfterDrain(t, out)
		select {
		case <-time.After(1 * time.Second):
			t.Errorf("done not cancelled: timed out")
			return
		case <-ctx.Done():
			return
		}
	})
}

func testClosesOnClose[S, T any](t *testing.T, stage func(context.Context, <-chan S) <-chan T) {
	t.Helper()
	t.Run("closes on closed input channel", func(t *testing.T) {
		in := make(chan S)
		close(in)
		out := stage(context.Background(), in)
		testClosesAfterDrain(t, out)
	})
}

// testClosesOnContextDone tests that a pipeline stage closes its output channel when the context is closed.
func testClosesOnContextDone[S, T any](t *testing.T, stage func(context.Context, <-chan S) <-chan T) {
	t.Helper()
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
	t.Helper()
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
	t.Helper()
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

type optionMap map[string][]pipelines.Option

func withOptions(t *testing.T, opts optionMap, f func(*testing.T, []pipelines.Option)) {
	t.Helper()
	for name, options := range opts {
		t.Run(name, func(t *testing.T) {
			f(t, options)
		})
	}
}

func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := pipelines.Chan([]int{1, 3, 5})
	doubled := pipelines.FlatMap(ctx, input, func(x int) []int { return []int{x, x + 1} })         // (x) => [x, x+1]
	expanded := pipelines.Map(ctx, doubled, func(x int) int { return x * 2 })                      // x => x*2
	exclaimed := pipelines.Map(ctx, expanded, func(x int) string { return fmt.Sprintf("%d!", x) }) // x => "${x}!"

	result, err := pipelines.Reduce(ctx, exclaimed, func(prefix string, str string) string {
		return prefix + " " + str
	})
	if err != nil {
		fmt.Println("context was cancelled!")
	}

	fmt.Print(result)

	// Output: 2! 4! 6! 8! 10! 12!
}

func ExampleForkMapCtx() {
	ctx := context.Background()

	// fetch multiple URLs in parallel, sending the results to an output channel.
	urls := pipelines.Chan([]string{"https://www.google.com", "https://www.splunk.com"})
	responses := pipelines.ForkMapCtx(ctx, urls, func(ctx context.Context, url string, out chan<- *http.Response) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			out <- resp
		}
	})

	for response := range responses {
		fmt.Printf("%s: %d\n", response.Request.URL, response.StatusCode)
	}
}

func ExampleErrorSink() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, errs := pipelines.NewErrorSink(ctx)
	defer errs.Close()

	urls := pipelines.Chan([]string{
		"https://httpstat.us/200",
		"https://httpstat.us/410",
		"wrong://not.a.url/test", // malformed URL; triggers a fatal error
		"https://httpstat.us/502",
	})

	// fetch a bunch of URLs, reporting errors along the way.
	responses := pipelines.OptionMapCtx(ctx, urls, func(ctx context.Context, url string) *http.Response {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			errs.Fatal(fmt.Errorf("error forming request context: %w", err))
			return nil
		}
		resp, err := http.DefaultClient.Do(req)

		if err != nil {
			errs.Error(fmt.Errorf("error fetching %s: %w", url, err))
			return nil
		}
		if resp.StatusCode >= 400 {
			errs.Error(fmt.Errorf("unsuccessful %s: %d", url, resp.StatusCode))
			return nil
		}
		return resp
	})

	// retrieve all responses; there should be only one
	for response := range responses {
		fmt.Printf("success: %s: %d\n", response.Request.URL, response.StatusCode)
	}

	// retrieve all errors; the 502 error should be skipped, since the malformed URL triggers
	// a fatal error.
	for _, err := range errs.All() {
		fmt.Printf("error:   %v\n", err.Error())
	}
}

func ExampleWithWaitGroup() {
	ctx := context.Background()

	incAndPrint := func(x int) int {
		x = x + 1
		fmt.Printf("%d ", x)
		return x
	}
	ints := pipelines.Chan([]int{1, 2, 3, 4, 5, 6})

	// WithWaitGroup
	var wg sync.WaitGroup
	out := pipelines.Map(ctx, ints, incAndPrint, pipelines.WithWaitGroup(&wg))
	_, err := pipelines.Drain(ctx, out)
	if err != nil {
		fmt.Println("could not drain!")
	}

	wg.Wait() // wait for the Map stage to complete before continuing.

	// Output: 2 3 4 5 6 7
}

func ExampleWithDone() {
	ctx := context.Background()

	doubleAndPrint := func(x int) int {
		x = x * 2
		fmt.Printf("%d ", x)
		return x
	}
	ints := pipelines.Chan([]int{2, 4, 6, 8, 10})

	opt, done := pipelines.WithDone(ctx)
	out := pipelines.Map(ctx, ints, doubleAndPrint, opt)
	_, err := pipelines.Drain(ctx, out)
	if err != nil {
		fmt.Println("could not drain!")
	}

	<-done.Done() // wait for the Map stage to complete before continuing.

	// Output: 4 8 12 16 20
}
