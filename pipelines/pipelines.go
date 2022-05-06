package pipelines

import (
    "context"
    "sync"
)

// Chan converts a slice of type T to a channel which receives the provided values. The result channel is closed if the
// provided Context is cancelled or all values have been sent.
func Chan[T any](ctx context.Context, in []T) chan T {
    result := make(chan T)
    go func() {
        SendAll(ctx, in, result)
        close(result)
    }()
    return result
}

// MultiSink converts a channel of []T to a channel of T.
func MultiSink[T any](ctx context.Context, in <-chan []T) <-chan T {
    result := make(chan T)
    go func() {
        for ss := range in {
            select {
            case <-ctx.Done():
                return
            default:
                SendAll(ctx, ss, result)
            }
        }
        close(result)
    }()
    return result
}

// Map provides a map pipeline stage, applying the provided function to every input received from the in channel and
// sending it to the returned channel. The result channel is closed when the input channel is closed or the provided
// context is cancelled.
func Map[S, T any](ctx context.Context, in <-chan S, f func(S) T) <-chan T {
    tChan := make(chan T)

    go func() {
        for s := range in {
            select {
            case <-ctx.Done():
                return
            case tChan <- f(s):
            }
        }
        close(tChan)
    }()
    return tChan
}

// MapCtx provides a map pipeline stage, applying the provided function to every input received from the in channel,
// passing the provided context, and passing the result to a new channel which is returned. The result channel is closed
// when the input channel is closed or the provided context is cancelled.
func MapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) T) <-chan T {
    tChan := make(chan T)

    go func() {
        for s := range in {
            select {
            case <-ctx.Done():
                return
            case tChan <- f(ctx, s):
            }
        }
        close(tChan)
    }()
    return tChan
}

// FlatMap provides a pipeline stage which applies the provided function to every input received from the in channel
// and passes every element of the slice returned to the output channel.
func FlatMap[S, T any](ctx context.Context, in <-chan S, f func(S) []T) <-chan T {
    tChan := make(chan T)

    go func() {
        for ss := range in {
            select {
            case <-ctx.Done():
                return
            default:
                SendAll(ctx, f(ss), tChan)
            }
        }
        close(tChan)
    }()
    return tChan
}

// FlatMapCtx provides a pipeline stage which applies the provided context-aware function to every input received from
// the in channel and passes every element of the slice returned to the output channel, which is returned.
func FlatMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S) []T) <-chan T {
    tChan := make(chan T)

    go func() {
        for ss := range in {
            select {
            case <-ctx.Done():
                return
            default:
                SendAll(ctx, f(ctx, ss), tChan)
            }

        }
        close(tChan)
    }()
    return tChan
}

// Combine combines the values from two channels into a third, which is returned. The returned channel is closed once
// either of the input channels are closed, or the provided context is cancelled.
func Combine[T any](ctx context.Context, s1 <-chan T, s2 <-chan T) chan T {
    out := make(chan T)
    var wg sync.WaitGroup
    wg.Add(2)
    go func() {
        defer wg.Done()
        for u := range s1 {
            select {
            case <-ctx.Done():
                return
            case out <- u:
            }

        }
    }()
    go func() {
        defer wg.Done()
        for u := range s2 {
            select {
            case <-ctx.Done():
                return
            case out <- u:
            }
        }
    }()
    go func() {
        wg.Wait()
        close(out)
    }()
    return out
}

// WithCancel returns a new channel which receives each value from ch. If the provided context is cancelled or the
// input channel is closed, the returned channel is also closed.
func WithCancel[T any](ctx context.Context, ch <-chan T) <-chan T {
    result := make(chan T)
    go func() {
        for u := range ch {
            select {
            case <-ctx.Done():
                return
            case result <- u:
            }
        }
        close(result)
    }()
    return result
}

// SendAll sends all values in ts to the provided channel while handling context closure. It blocks until the channel
// is closed, or the provided context is cancelled.
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
func ForkMapCtx[S, T any](ctx context.Context, in <-chan S, f func(context.Context, S, chan<- T)) <-chan T {
    tChan := make(chan T)
    var wg sync.WaitGroup
    go func() {
        for s := range in {
            select {
            case <-ctx.Done():
                return
            default:
                wg.Add(1)
                go func(s S) {
                    f(ctx, s, tChan)
                    wg.Done()
                }(s)
            }
        }
        wg.Wait()
        close(tChan)
    }()
    return tChan
}
