package pipelines_test

import (
	"context"
	"github.com/splunk/pipelines"
	"testing"
)

func ints(n int) <-chan int {
	ints := make([]int, 0, n)
	for i := 0; i < n; i++ {
		ints = append(ints, i)
	}
	return pipelines.Chan(ints)
}

func benchmarkMap(i int, b *testing.B) {
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		in := ints(i)
		out := pipelines.Map(ctx, in, inc)
		_, err := pipelines.Drain(ctx, out)
		if err != nil {
			b.Failed()
		}
	}
}

func BenchmarkMap1(b *testing.B)     { benchmarkMap(1, b) }
func BenchmarkMap10(b *testing.B)    { benchmarkMap(10, b) }
func BenchmarkMap100(b *testing.B)   { benchmarkMap(100, b) }
func BenchmarkMap1000(b *testing.B)  { benchmarkMap(1000, b) }
func BenchmarkMap10000(b *testing.B) { benchmarkMap(10000, b) }

func inc(x int) int { return x + 1 } // using a simple map func
