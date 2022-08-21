package pipelines_test

import (
	"context"
	"fmt"
	"github.com/splunk/pipelines"
	"testing"
)

var benchMatrix = []Test{
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

func (t Test) Name(b *testing.B) string {
	return fmt.Sprintf("%s_l%d_p%d_b%d", b.Name(), t.Load, t.Pool, t.Buffer)
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

func BenchmarkMap(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			ctx := context.Background()
			for n := 0; n < b.N; n++ {
				in := ints(bench.Load)

				out := pipelines.Map(ctx, in, inc, bench.Opts()...)
				_, err := pipelines.Reduce(ctx, out, sum) // use reduce to avoid allocations from Drain
				if err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkMapCtx(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			ctx := context.Background()
			for n := 0; n < b.N; n++ {
				in := ints(bench.Load)

				out := pipelines.MapCtx(ctx, in, incCtx, bench.Opts()...)
				_, err := pipelines.Reduce(ctx, out, sum)
				if err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkFlatten(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {

			ctx := context.Background()
			for n := 0; n < b.N; n++ {
				in := intChunks(bench.Load)

				out := pipelines.Flatten(ctx, in, bench.Opts()...)
				_, err := pipelines.Reduce(ctx, out, sum)
				if err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkFlatMap(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			ctx := context.Background()
			for n := 0; n < b.N; n++ {
				in := ints(bench.Load)
				out := pipelines.FlatMap(ctx, in, flatInc, bench.Opts()...)
				_, err := pipelines.Reduce(ctx, out, sum)
				if err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkFlatMapCtx(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			ctx := context.Background()
			for n := 0; n < b.N; n++ {
				in := ints(bench.Load)
				out := pipelines.FlatMapCtx(ctx, in, flatIncCtx, bench.Opts()...)
				_, err := pipelines.Reduce(ctx, out, sum)
				if err != nil {
					b.Failed()
				}
			}
		})
	}
}
