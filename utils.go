package logicdb

func IF[T any](check bool, a, b T) T {
	if check {
		return a
	}

	return b
}
