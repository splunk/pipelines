package genlib

type Number interface {
	int | int8 | int16 | int32 | int64 | float32 | float64 | byte
}

func Min[T Number](x, y T) T {
	if x < y {
		return x
	}
	return y
}

func Max[T Number](x, y T) T {
	if x > y {
		return x
	}
	return y
}
