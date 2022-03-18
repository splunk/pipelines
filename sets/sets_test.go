package sets_test

import (
	"fmt"
	"github.com/splunk/go-genlib/sets"
	"testing"
)

type newTest[T comparable] struct {
	name string
	args []T
	want sets.Set[T]
}

func TestNew(t *testing.T) {
	t.Parallel()
	t.Run("[int]", func(t *testing.T) {
		testNew(t, []newTest[int]{
			{
				name: "duplicate elements",
				args: []int{3, 4, 6, 4, 4, 4, 4, 4, 1, 2, 2},
				want: sets.New[int](3, 4, 6, 1, 2),
			},
		})
	})
	t.Run("[string]", func(t *testing.T) {
		testNew(t, []newTest[string]{
			{
				name: "duplicate elements",
				args: []string{"a", "xyc", "b", "zuw", "b", "a", "c"},
				want: sets.New[string]("a", "b", "c", "xyc", "zuw"),
			},
		})
	})
}

func testNew[T comparable](t *testing.T, tests []newTest[T]) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sets.New[T](tt.args...)
			if !got.Equals(tt.want) {
				t.Errorf("New failed; want: %v got: %v", tt.want, got)
			}
		})
	}
}

type containsTest[T comparable] struct {
	name  string
	set   sets.Set[T]
	elems []T
	want  []bool
}

func TestContains(t *testing.T) {
	t.Parallel()
	t.Run("[int]", func(t *testing.T) {
		testContains(t, []containsTest[int]{
			{
				name:  "empty set",
				set:   sets.New[int](),
				elems: []int{3, 5, 7},
				want:  []bool{false, false, false},
			},
			{
				name:  "singleton set",
				set:   sets.New[int](5),
				elems: []int{1, 3, 5},
				want:  []bool{false, false, true},
			},
			{
				name:  "set",
				set:   sets.New[int](1, 3, 5),
				elems: []int{1, 2, 3, 4, 5},
				want:  []bool{true, false, true, false, true},
			},
		})
	})
	t.Run("[struct]", func(t *testing.T) {
		type Struct struct {
			str string
			x   int
		}
		testContains(t, []containsTest[Struct]{
			{
				name:  "set",
				set:   sets.New[Struct](Struct{"x", 1}, Struct{"y", 2}, Struct{"z", 3}),
				elems: []Struct{{"x", 1}, {"w", 3}, {"z", 3}},
				want:  []bool{true, false, true},
			},
		})
	})
}

func testContains[T comparable](t *testing.T, tests []containsTest[T]) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, elem := range tt.elems {
				got := tt.set.Contains(elem)
				if got != tt.want[i] {
					t.Errorf("%v contains %v; want: %t; got %t", tt.set, elem, got, tt.want[i])
				}
			}
		})
	}
}

type addTest[T comparable] struct {
	name  string
	set   sets.Set[T]
	elems []T
	want  sets.Set[T]
}

func TestAdd(t *testing.T) {
	t.Parallel()
	t.Run("[string]", func(t *testing.T) {
		testAdd[string](t, []addTest[string]{
			{
				name:  "add to empty",
				set:   sets.New[string](),
				elems: []string{"a", "b", "c"},
				want:  sets.New[string]("a", "b", "c"),
			},
			{
				name:  "add to existing",
				set:   sets.New[string]("a", "b"),
				elems: []string{"a", "b", "c", "d"},
				want:  sets.New[string]("a", "b", "c", "d"),
			},
		})
	})
}

func testAdd[T comparable](t *testing.T, tests []addTest[T]) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, elem := range tt.elems {
				tt.set.Add(elem)
			}
			if !tt.set.Equals(tt.want) {
				t.Errorf("add failed; want: %v, got: %v", tt.want, tt.set)
			}
		})
	}
}

type equalsTest[T comparable] struct {
	name string
	setA sets.Set[T]
	setB sets.Set[T]
	want bool
}

func TestEquals(t *testing.T) {
	t.Parallel()
	t.Run("[int]", func(t *testing.T) {
		testEquals[int](t, []equalsTest[int]{
			{
				name: "empty equals empty",
				setA: sets.New[int](),
				setB: sets.New[int](),
				want: true,
			},
			{
				name: "empty not equals full",
				setA: sets.New[int](),
				setB: sets.New[int](1, 2),
				want: false,
			},
			{
				name: "full equals full",
				setA: sets.New[int](1, 2, 6, 7),
				setB: sets.New[int](7, 2, 1, 6),
				want: true,
			},
			{
				name: "not equals subset",
				setA: sets.New[int](1, 2, 3),
				setB: sets.New[int](1, 2),
				want: false,
			},
		})
	})
}

func testEquals[T comparable](t *testing.T, tests []equalsTest[T]) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAB := tt.setA.Equals(tt.setB)
			if !gotAB == tt.want {
				t.Errorf("A equals B failed; want: %v, got: %v", tt.want, gotAB)
			}
			gotBA := tt.setB.Equals(tt.setA)
			if !gotBA == tt.want {
				t.Errorf("A equals B failed; want: %v, got: %v", tt.want, gotBA)
			}
		})
	}
}

type containsSetTest[T comparable] struct {
	name string
	setA sets.Set[T]
	setB sets.Set[T]
	want bool
}

func TestContainsSet(t *testing.T) {
	t.Parallel()
	t.Run("[byte]", func(t *testing.T) {
		testContainsSet(t, []containsSetTest[byte]{
			{
				name: "empty contains empty",
				setA: sets.New[byte](),
				setB: sets.New[byte](),
				want: true,
			},
			{
				name: "full contains empty",
				setA: sets.New[byte](5, 8, 2),
				setB: sets.New[byte](),
				want: true,
			},
			{
				name: "empty not contains full",
				setA: sets.New[byte](),
				setB: sets.New[byte](255, 3, 4),
				want: false,
			},
			{
				name: "full contains subset",
				setA: sets.New[byte](255, 8, 3, 4, 6, 1),
				setB: sets.New[byte](255, 3, 4),
				want: true,
			},
			{
				name: "subset not contains full",
				setA: sets.New[byte](255, 3, 4),
				setB: sets.New[byte](255, 8, 3, 4, 6, 1),
				want: false,
			},
		})
	})
}

func testContainsSet[T comparable](t *testing.T, tests []containsSetTest[T]) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.setA.ContainsSet(tt.setB)
			if tt.want != got {
				t.Errorf("A ContainsSet B failed; want: %v, got: %v", tt.want, got)
			}
		})
	}
}

type binaryOpTest[T comparable] struct {
	name          string
	setA          sets.Set[T]
	setB          sets.Set[T]
	union         sets.Set[T]
	intersection  sets.Set[T]
	difference    sets.Set[T]
	symmetricDiff sets.Set[T]
}

func TestBinaryOps(t *testing.T) {
	t.Parallel()
	t.Run("[int]", func(t *testing.T) {
		testBinaryOps[int](t, []binaryOpTest[int]{
			{
				name:          "empty full",
				setA:          sets.New[int](),
				setB:          sets.New[int](3, 7, 4, 2, 1),
				union:         sets.New[int](3, 7, 4, 2, 1),
				intersection:  sets.New[int](),
				difference:    sets.New[int](),
				symmetricDiff: sets.New[int](3, 7, 4, 2, 1),
			},
			{
				name:          "subset",
				setA:          sets.New[int](1, 4, 7, 100, 130),
				setB:          sets.New[int](1, 7, 130),
				union:         sets.New[int](1, 4, 7, 100, 130),
				intersection:  sets.New[int](1, 7, 130),
				difference:    sets.New[int](4, 100),
				symmetricDiff: sets.New[int](4, 100),
			},
			{
				name:          "disjoint",
				setA:          sets.New[int](1, 2, 3, 4, 5),
				setB:          sets.New[int](6, 7, 8, 9, 10),
				union:         sets.New[int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
				intersection:  sets.New[int](),
				difference:    sets.New[int](1, 2, 3, 4, 5),
				symmetricDiff: sets.New[int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			},
			{
				name:          "overlapping",
				setA:          sets.New[int](1, 2, 3, 4, 5, 6, 7),
				setB:          sets.New[int](5, 6, 7, 8, 9, 10),
				union:         sets.New[int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
				intersection:  sets.New[int](5, 6, 7),
				difference:    sets.New[int](1, 2, 3, 4),
				symmetricDiff: sets.New[int](1, 2, 3, 4, 8, 9, 10),
			},
		})
	})
}

func testBinaryOps[T comparable](t *testing.T, tests []binaryOpTest[T]) {
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s union", tt.name), func(t *testing.T) {
			gotAB := sets.Union(tt.setA, tt.setB)
			if !gotAB.Equals(tt.union) {
				t.Errorf("A union B failed; want: %v, got: %v", tt.union, gotAB)
			}
			gotBA := sets.Union(tt.setB, tt.setA)
			if !gotBA.Equals(tt.union) {
				t.Errorf("B union A failed; want: %v, got: %v", tt.union, gotBA)
			}
		})
		t.Run(fmt.Sprintf("%s intersect", tt.name), func(t *testing.T) {
			gotAB := sets.Intersect(tt.setA, tt.setB)
			if !gotAB.Equals(tt.intersection) {
				t.Errorf("A intersect B failed; want: %v, got: %v", tt.intersection, gotAB)
			}
			gotBA := sets.Intersect(tt.setB, tt.setA)
			if !gotBA.Equals(tt.intersection) {
				t.Errorf("B intersect A failed; want: %v, got: %v", tt.intersection, gotBA)
			}
		})
		t.Run(fmt.Sprintf("%s difference", tt.name), func(t *testing.T) {
			gotAB := sets.Difference(tt.setA, tt.setB)
			if !gotAB.Equals(tt.difference) {
				t.Errorf("A difference B failed; want: %v, got: %v", tt.difference, gotAB)
			}
		})
		t.Run(fmt.Sprintf("%s symmetric difference", tt.name), func(t *testing.T) {
			gotAB := sets.SymmetricDiff(tt.setA, tt.setB)
			if !gotAB.Equals(tt.symmetricDiff) {
				t.Errorf("A symmetric difference B failed; want: %v, got: %v", tt.symmetricDiff, gotAB)
			}
			gotBA := sets.SymmetricDiff(tt.setB, tt.setA)
			if !gotBA.Equals(tt.symmetricDiff) {
				t.Errorf("B symmetric difference A failed; want: %v, got: %v", tt.symmetricDiff, gotBA)
			}
		})
	}
}
