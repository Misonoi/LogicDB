package logicdb

import (
	"fmt"
	"io"
)

type ReadSeeker interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

type WriteSeeker interface {
	io.Writer
	io.Seeker
	io.WriterAt
}

type ReaderWithPos[T ReadSeeker] struct {
	reader T
	Pos    uint64
}

func NewReaderWithPos[T ReadSeeker](reader T) *ReaderWithPos[T] {
	return &ReaderWithPos[T]{
		reader: reader,
		Pos:    0,
	}
}

func (r *ReaderWithPos[T]) Read(p []byte) (int, error) {
	num, err := r.reader.Read(p)

	r.Pos += uint64(num)

	if err != nil {
		return 0, err
	}

	return num, err
}

func (r *ReaderWithPos[T]) ReadAt(p []byte, offset int64) (int, error) {
	return r.reader.ReadAt(p, offset)
}

func (r *ReaderWithPos[T]) Seek(offset int64, whence int) (int64, error) {
	currentPos := int64(r.Pos)

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

	r.Pos = uint64(newPos)

	return newPos, nil
}

type WriterWithPos[T WriteSeeker] struct {
	writer T
	Pos    uint64
}

func NewWriterWithPos[T WriteSeeker](writer T) *WriterWithPos[T] {
	return &WriterWithPos[T]{
		writer: writer,
		Pos:    0,
	}
}

func (w *WriterWithPos[T]) Write(p []byte) (int, error) {
	num, err := w.writer.Write(p)

	w.Pos += uint64(num)

	if err != nil {
		return 0, err
	}

	return num, err
}

func (w *WriterWithPos[T]) WriteAt(p []byte, offset int64) (int, error) {
	return w.writer.WriteAt(p, offset)
}

func (w *WriterWithPos[T]) Seek(offset int64, whence int) (int64, error) {
	currentPos := int64(w.Pos)

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

	w.Pos = uint64(newPos)

	return newPos, nil
}
