// Package sets contains functions related to a generic Set implementation.
package sets

// Set represents a finite set (in the sense of Discrete mathematics) of comparable elements. It supports standard set
// operations such as union and set difference.
type Set[T comparable] struct {
    items map[T]struct{}
}

// New constructs a new set with the provided elements. Duplicate elements are collapsed.
func New[T comparable](elems ...T) Set[T] {
    result := Set[T]{items: make(map[T]struct{}, len(elems))}
    for _, elem := range elems {
        result.items[elem] = struct{}{}
    }
    return result
}

// Add adds the provided element to this set, modifying it if it does not already exist.
func (s Set[T]) Add(elem T) {
    s.items[elem] = struct{}{}
}

// Contains returns true if and only if this set contains the provided element.
func (s Set[T]) Contains(elem T) bool {
    _, ok := s.items[elem]
    return ok
}

// ContainsSet returns true if and only if this set contains the other set.
func (s Set[T]) ContainsSet(other Set[T]) bool {
    for b := range other.items {
        if !s.Contains(b) {
            return false
        }
    }
    return true
}

// Equals returns true if and only if this set is equal to other.
func (s Set[T]) Equals(other Set[T]) bool {
    return len(s.items) == len(other.items) && s.ContainsSet(other)
}

// Union returns the union of sets A and B. The result contains all elements of A and all elements of B.
func Union[T comparable](A, B Set[T]) Set[T] {
    result := Set[T]{items: make(map[T]struct{}, len(A.items)+len(B.items))}
    for a := range A.items {
        result.items[a] = struct{}{}
    }
    for b := range B.items {
        result.items[b] = struct{}{}
    }
    return result
}

// Intersect returns the intersection of sets A and B. The result contains all elements of A which are also in B.
func Intersect[T comparable](A, B Set[T]) Set[T] {
    result := Set[T]{items: make(map[T]struct{})}
    for a := range A.items {
        if _, ok := B.items[a]; ok {
            result.items[a] = struct{}{}
        }
    }
    return result
}

// Difference returns the set difference of sets A and B. The result contains all elements of A which cannot be found
// in B.
func Difference[T comparable](A, B Set[T]) Set[T] {
    result := Set[T]{items: make(map[T]struct{})}
    for a := range A.items {
        if _, ok := B.items[a]; !ok {
            result.items[a] = struct{}{}
        }
    }
    return result
}

// SymmetricDiff returns the symmetric difference of sets A and B. The result contains all elements of A and B, except
// those elements which are found in both A and B.
func SymmetricDiff[T comparable](A, B Set[T]) Set[T] {
    result := Set[T]{items: make(map[T]struct{})}
    for a := range A.items {
        if _, ok := B.items[a]; !ok {
            result.items[a] = struct{}{}
        }
    }
    for b := range B.items {
        if _, ok := A.items[b]; !ok {
            result.items[b] = struct{}{}
        }
    }
    return result
}
