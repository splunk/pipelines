package pipelines_test

import (
	"context"
	"github.com/splunk/pipelines"
	"testing"
)

// generate benchmark calls based on helpers and benchmark matrices defined in this file.
//go:generate go run gen/pipelines_bench_gen.go pipelines_bench_test

//bench:matrix matrix1
var benchMatrix = []Test{
	{1, 1, 1},
	{10, 1, 1},
	{100, 1, 1},
	{1000, 1, 1},
	{100, 3, 1},
	{1000, 3, 1},
	{10000, 3, 1},
}

type Test struct {
	Load   int
	Pool   int
	Buffer int
}

func (t Test) Opts() []pipelines.Option {
	return []pipelines.Option{pipelines.WithPool(t.Pool), pipelines.WithBuffer(t.Buffer)}
}

func ints(n int) <-chan int {
	ints := make([]int, 0, n)
	for i := 0; i < n; i++ {
		ints = append(ints, i)
	}
	return pipelines.Chan(ints)
}

func intChunks(n int) <-chan []int {
	const chunkSize = 2
	ints := make([]int, 0, chunkSize)
	for i := 0; i < n; i++ {
		ints = append(ints, i)
	}
	chunks := make([][]int, 0, n)
	for i := 0; i < n; i++ {
		chunks = append(chunks, ints)
	}
	return pipelines.Chan(chunks)
}

func sum(x, y int) int                            { return x + y }
func flatIncCtx(ctx context.Context, x int) []int { return []int{x, x + 1} }
func flatInc(x int) []int                         { return []int{x, x + 1} }
func incCtx(ctx context.Context, x int) int       { return x + 1 } // simple map funcs
func inc(x int) int                               { return x + 1 }

//bench:func matrix1
func benchmarkMap(test Test, b *testing.B) {
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		in := ints(test.Load)

		out := pipelines.Map(ctx, in, inc, test.Opts()...)
		_, err := pipelines.Reduce(ctx, out, sum) // use reduce to avoid allocations from Drain
		if err != nil {
			b.Failed()
		}
	}
}

//bench:func matrix1
func benchmarkMapCtx(test Test, b *testing.B) {
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		in := ints(test.Load)

		out := pipelines.MapCtx(ctx, in, incCtx, test.Opts()...)
		_, err := pipelines.Reduce(ctx, out, sum)
		if err != nil {
			b.Failed()
		}
	}
}

//bench:func matrix1
func benchmarkFlatten(test Test, b *testing.B) {
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		in := intChunks(test.Load)

		out := pipelines.Flatten(ctx, in, test.Opts()...)
		_, err := pipelines.Reduce(ctx, out, sum)
		if err != nil {
			b.Failed()
		}
	}
}

//bench:func matrix1
func benchmarkFlatMap(test Test, b *testing.B) {
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		in := ints(test.Load)
		out := pipelines.FlatMap(ctx, in, flatInc, test.Opts()...)
		_, err := pipelines.Reduce(ctx, out, sum)
		if err != nil {
			b.Failed()
		}
	}
}
