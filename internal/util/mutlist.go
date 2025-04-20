package util

type MutList[T any] struct {
	items []T
}

func NewMutList[T any]() *MutList[T] {
	l := make([]T, 0, 10)
	return &MutList[T]{items: l}
}

func (l *MutList[T]) Add(item T) {
	l.items = append(l.items, item)
}

func (l *MutList[T]) Len() int {
	return len(l.items)
}

func (l *MutList[T]) Items() []T {
	list := make([]T, len(l.items))
	copy(list, l.items)
	return list
}

