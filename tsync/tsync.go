// Package tsync contains type-safe wrappers around functionality from the sync package in the standard library.
package tsync

import "sync"

// Map is a type-safe version of a sync.Map. It overloads all functions provided by sync.Map with type-safe equivalents.
type Map[K comparable, V any] struct {
	sync.Map
}

// Delete deletes the value for a key.
func (sm *Map[K, V]) Delete(key K) {
	sm.Map.Delete(key)
}

// Load returns the value stored in the map for a key, or nil if no value is present.
// The ok result indicates whether value was found in the map.
func (sm *Map[K, V]) Load(key K) (value V, ok bool) {
	return sm.Map.Load(key)
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (sm *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	return sm.Map.LoadAndDelete(key)
}

// LoadOrStore returns the existing value for the key if present. Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (sm *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	return sm.Map.LoadOrStore(key, value)
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the Map's contents: no key will be visited more
// than once, but if the value for any key is stored or deleted concurrently (including by f), Range may reflect any
// mapping for that key from any point during the Range call.
// Range does not block other methods on the receiver; even f itself may call any method on m.
//
// Range may be O(N) with the number of elements in the map even if f returns false after a constant number of calls.
func (sm *Map[K, V]) Range(f func(key K, value V) bool) {
	sm.Map.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

// Store sets the value for a key.
func (sm *Map[K, V]) Store(key K, value V) {
	sm.Map.Store(key, value)
}

// Pool is a type-safe version of a sync.Pool. It replaces all public members of Pool with their type-safe alternatives.
type Pool[T any] struct {
	pool       sync.Pool
	wrappedNew func() T
}

// SetNew optionally specifies a function to generate a value when Get would otherwise return nil.
// It may not be called concurrently with calls to Get.
func (p *Pool[T]) SetNew(f func() T) {
	p.pool.New = p.doWrappedNew
	p.wrappedNew = f
}

// Get selects an arbitrary item from the Pool, removes it from the Pool, and returns it to the caller.
// Get may choose to ignore the pool and treat it as empty.
// Callers should not assume any relation between values passed to Put and the values returned by Get.
//
// If Get would otherwise return nil and pool.SetNew was called previously, Get returns the result of calling pool.New.
func (p *Pool[T]) Get() T {
	return p.pool.Get()
}

// Put adds x to the pool.
func (p *Pool[T]) Put(x T) {
	p.pool.Put(x)
}

func (p *Pool[T]) doWrappedNew() any {
	// doWrappedNew is used to avoid allocating a new function on each call to SetNew.
	return p.wrappedNew()
}
