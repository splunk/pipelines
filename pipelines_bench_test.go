package pipelines_test

import (
	"context"
	"fmt"
	"github.com/splunk/pipelines"
	"sync"
	"testing"
)

const chunkSize = 10

var benchMatrix = []Test{
	{10, 1, 1},
	{100, 1, 1},
	{1000, 1, 1},
	{100, 3, 1},
	{1000, 3, 1},
	{10000, 3, 1},
	{1000, 5, 1},
	{1000, 5, 10},
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

func incOpt(x int) *int                         { x += 1; return &x }
func incOptCtx(_ context.Context, x int) *int   { x += 1; return &x }
func sum(x, y int) int                          { return x + y }
func flatIncCtx(_ context.Context, x int) []int { return []int{x, x + 1} }
func flatInc(x int) []int                       { return []int{x, x + 1} }
func incCtx(_ context.Context, x int) int       { return x + 1 } // simple map funcs
func inc(x int) int                             { return x + 1 }

func BenchmarkMap(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan int
			for n := 0; n < b.N; n++ {
				ins = append(ins, ints(bench.Load))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.Map(ctx, ins[n], inc, bench.Opts()...)
				if _, err := pipelines.Reduce(ctx, out, sum); err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkMapCtx(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan int
			for n := 0; n < b.N; n++ {
				ins = append(ins, ints(bench.Load))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.MapCtx(ctx, ins[n], incCtx, bench.Opts()...)
				_, err := pipelines.Reduce(ctx, out, sum)
				if err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkOptionMap(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan int
			for n := 0; n < b.N; n++ {
				ins = append(ins, ints(bench.Load))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.OptionMap(ctx, ins[n], incOpt, bench.Opts()...)
				if _, err := pipelines.Reduce(ctx, out, sum); err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkOptionMapCtx(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan int
			for n := 0; n < b.N; n++ {
				ins = append(ins, ints(bench.Load))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.OptionMapCtx(ctx, ins[n], incOptCtx, bench.Opts()...)
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
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan []int
			for n := 0; n < b.N; n++ {
				ins = append(ins, intChunks(bench.Load/chunkSize))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.Flatten(ctx, ins[n], bench.Opts()...)
				if _, err := pipelines.Reduce(ctx, out, sum); err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkFlatMap(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan int
			for n := 0; n < b.N; n++ {
				ins = append(ins, ints(bench.Load))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.FlatMap(ctx, ins[n], flatInc, bench.Opts()...)
				if _, err := pipelines.Reduce(ctx, out, sum); err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkFlatMapCtx(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan int
			for n := 0; n < b.N; n++ {
				ins = append(ins, ints(bench.Load))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.FlatMapCtx(ctx, ins[n], flatIncCtx, bench.Opts()...)
				if _, err := pipelines.Reduce(ctx, out, sum); err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkCombine(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins1 []<-chan int
			var ins2 []<-chan int
			for n := 0; n < b.N; n++ {
				ins1 = append(ins1, ints(bench.Load/2))
				ins2 = append(ins2, ints(bench.Load/2))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.Combine(ctx, ins1[n], ins2[n], bench.Opts()...)
				if _, err := pipelines.Reduce(ctx, out, sum); err != nil {
					b.Failed()
				}
			}
		})
	}
}

func BenchmarkTee(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan int
			for n := 0; n < b.N; n++ {
				ins = append(ins, ints(bench.Load))
			}
			var wg sync.WaitGroup
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out1, out2 := pipelines.Tee(ctx, ins[n], bench.Opts()...)
				wg.Add(1)
				go func() {
					if _, err := pipelines.Reduce(ctx, out1, sum); err != nil {
						b.Failed()
					}
					wg.Done()
				}()
				if _, err := pipelines.Reduce(ctx, out2, sum); err != nil {
					b.Failed()
				}
				wg.Wait()
			}
		})
	}
}

func BenchmarkForkMapCtx(b *testing.B) {
	for _, bench := range benchMatrix {
		b.Run(bench.Name(b), func(b *testing.B) {
			b.StopTimer()
			ctx := context.Background()
			var ins []<-chan []int
			for n := 0; n < b.N; n++ {
				ins = append(ins, intChunks(bench.Load/chunkSize))
			}
			b.StartTimer()
			for n := 0; n < b.N; n++ {
				out := pipelines.ForkMapCtx(ctx, ins[n], func(ctx context.Context, in []int, out chan<- int) {
					for _, x := range in {
						select {
						case out <- x:
						case <-ctx.Done():
							return
						}
					}
				}, bench.Opts()...)
				if _, err := pipelines.Reduce(ctx, out, sum); err != nil {
					b.Failed()
				}
			}
		})
	}
}
