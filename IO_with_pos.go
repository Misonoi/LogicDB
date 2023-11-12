package logicdb

import (
	"io"
)

type ReaderWithPos[T io.Reader] struct {
	reader T
	pos    uint64
}

func NewReaderWithPos[T io.Reader](reader T) *ReaderWithPos[T] {
	return &ReaderWithPos[T]{
		reader: reader,
		pos:    0,
	}
}

func (r *ReaderWithPos[T]) Read(p []byte) (int, error) {
	num, err := r.Read(p)

	r.pos += uint64(num)

	if err != nil {
		return 0, err
	}

	return num, err
}

type WriterWithPos[T io.Writer] struct {
	writer T
	pos    uint64
}

func NewWriterPos[T io.Writer](writer T) *WriterWithPos[T] {
	return &WriterWithPos[T]{
		writer: writer,
		pos:    0,
	}
}

func (w *WriterWithPos[T]) Write(p []byte) (int, error) {
	num, err := w.Write(p)

	w.pos += uint64(num)

	if err != nil {
		return 0, err
	}

	return num, err
}
