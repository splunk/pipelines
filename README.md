![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/splunk/pipelines/test.yml?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/splunk/pipelines)](https://goreportcard.com/report/github.com/splunk/pipelines)
![Coveralls](https://img.shields.io/coveralls/github/splunk/pipelines)
![GitHub](https://img.shields.io/github/license/splunk/pipelines)
[![Go Reference](https://pkg.go.dev/badge/github.com/splunk/pipelines.svg)](https://pkg.go.dev/github.com/splunk/pipelines)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/splunk/pipelines?label=version)

# splunk/pipelines

Manages concurrency using Go + Generics. Separate _what_ your code does from _how_ it runs on threads. 

Install it locally:

```shell
go get github.com/splunk/pipelines
```

## Overview

`Pipelines` provides a set of primitives for managing concurrent pipelines.
A pipeline is a set of processing stages connected by channels.
Each stage is run on one or more goroutines.
`pipelines` manages starting, stopping, and scaling up each pipeline stage for you, allowing you to keep concurrency concerns away from your business logic.

### Stages

Each pipeline stage is provided as a top-level `func` which accepts one or more inputs channels
and return one or more output channels.

* `Map`: converts a `chan S` to a `chan T`, by converting each `S` to exactly one `T`.
* `OptionMap`: converts a `chan S` to a `chan T`, by converting each `S` to zero or one `T`s.
* `FlatMap`: converts a `chan S` to a `chan T`, by converting each `S` to zero or more `T`s.
* `Combine`: combines two `chan T` into a single `chan T`.
* `Flatten`: converts a `chan []T` to a `chan T`.
* `Tee`: converts a `chan T` into two `chan T`, each of which receive exactly the same values.
* `ForkMapCtx`: converts a `chan S` into a `chan T`, by converting each `S` to zero or more `T`s. Unlike `FlatMap`, a
  new goroutine is started to convert each value of `S`. 

Any stage which converts a `chan S` into a `chan T` requires that the caller pass a conversion func that knows how to 
turn `S` into `T`.

`Map`, `FlatMap`, and `OptionMap`, each have variants `MapCtx`, `FlatMapCtx`, and `OptionMapCtx`, which allow the caller
to pass a conversion func which accepts a `context.Context` as its first argument. This allows a conversion func to
perform I/O safely.

Each stage creates and manages the closure of its output channel, and listens for shutdown via `context.Context`.

### Combining Stages

Stages are meant to be combined by passing output channels into input channels using the pattern shown in the
toy example below:

```go
func main()  {
  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()
  
  input := pipelines.Chan([]int{1, 3, 5})
  expanded := pipelines.FlatMap(ctx, input, withAdjacent) // (x) => [x, x+1]:  yields [1,2,3,4,5,6]
  doubled := pipelines.Map(ctx, expanded, double)         // (x) => x*2:       yields [2,4,6,8,10,12]
  exclaimed := pipelines.Map(ctx, doubled, exclaim)       // (x) => "${x}!":   yields [2!,4!,6!,8!,10!,12!]
  
  result, err := pipelines.Reduce(ctx, exclaimed, func(prefix string, str string) string {
    return prefix + " " + str
  })
  if err != nil {
    fmt.Println("context was cancelled!")
  }

  fmt.Print(result)
  
  // Output: 2! 4! 6! 8! 10! 12!
}

func withAdjacent(x int) []int { return []int{x, x+1} }
func double(x int) int { return x*2 }
func exclaim(x int) string { return fmt.Sprintf("%d!", x)}
```

In real-world applications, the functions used to convert values flowing through the pipeline can be much more complex. 
The `pipelines` package provides a way to separate _what_ each stage of the pipeline is doing from the code used to make it concurrent.

### Configuring Stages

Each pipeline stage can be configured with a set of powerful options which modifies the concurrent behavior of the
pipelines.

* `WithBuffer(n int)`: buffers the output channel with size `n`. Output channels are unbuffered by default.
* `WithPool(n int)`: runs the pipeline stage on a worker pool of size `n`.

A few options are provided for listening to when a pipeline has halted. 

* `WithDone(context.Context)`: configures the stage to cancel a derived context when the stage has stopped. 
  Can be used to signal when _ANY_ stage in a pipeline has been stopped.
* `WithWaitGroup(sync.WaitGroup)`: configures the stage to use the provided WaitGroup to signal when all goroutines
  started in the stage have stopped. Can be used to signal when _ALL_ stages in a pipeline have been stopped.

### Sinks and Helpers

A sink serves as the last stage of a processing pipeline. All sinks are implemented as blocking calls which don't
start any new goroutines.

* `Drain`: converts a `chan T` to a `[]T`.
* `Reduce`: converts a `chan S` to a `T`, by combining multiple values of `S` into one value of `T`.

The following helpers are included to make conversion from standard to channels simpler.

* `Chan`: converts any `[]T` to a `chan T`.

### Error Handling

Fatal and non-fatal errors that occur during a pipeline can be reported via an `ErrorSink`.
To ensure fatal errors shut down pipeline stages, `NewErrorSink` wraps and returns a context which is cancelled
whenever a fatal error is reported.
Errors can be reported by calling `ErrorSink.All()`, which reports all errors in flight.

See the [example](https://pkg.go.dev/github.com/splunk/pipelines#ErrorSink) in the documentation for usage.
