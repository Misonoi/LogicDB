package logicdb

import (
	"fmt"
	"io"
)

type ReadSeeker interface {
	io.Reader
	io.Seeker
}

type WriteSeeker interface {
	io.Writer
	io.Seeker
}

type ReaderWithPos[T ReadSeeker] struct {
	reader T
	pos    uint64
}

func NewReaderWithPos[T ReadSeeker](reader T) *ReaderWithPos[T] {
	return &ReaderWithPos[T]{
		reader: reader,
		pos:    0,
	}
}

func (r *ReaderWithPos[T]) Read(p []byte) (int, error) {
	num, err := r.reader.Read(p)

	r.pos += uint64(num)

	if err != nil {
		return 0, err
	}

	return num, err
}

func (r *ReaderWithPos[T]) Seek(offset int64, whence int) (int64, error) {
	currentPos := int64(r.pos)

	var newPos int64

	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = currentPos + offset
	case io.SeekEnd:
		size, err := r.reader.Seek(0, io.SeekEnd)

		if err != nil {
			return 0, err
		}

		newPos = size + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	newPos, err := r.reader.Seek(newPos, io.SeekStart)

	if err != nil {
		return 0, err
	}

	r.pos = uint64(newPos)

	return newPos, nil
}

type WriterWithPos[T WriteSeeker] struct {
	writer T
	pos    uint64
}

func NewWriterWithPos[T WriteSeeker](writer T) *WriterWithPos[T] {
	return &WriterWithPos[T]{
		writer: writer,
		pos:    0,
	}
}

func (w *WriterWithPos[T]) Write(p []byte) (int, error) {
	num, err := w.writer.Write(p)

	w.pos += uint64(num)

	if err != nil {
		return 0, err
	}

	return num, err
}

func (w *WriterWithPos[T]) Seek(offset int64, whence int) (int64, error) {
	currentPos := int64(w.pos)

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = currentPos + offset
	case io.SeekEnd:
		size, err := w.writer.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		newPos = size + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}

	newPos, err := w.writer.Seek(newPos, io.SeekStart)

	if err != nil {
		return 0, err
	}

	w.pos = uint64(newPos)

	return newPos, nil
}
