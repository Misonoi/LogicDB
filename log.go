package logicdb

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"sort"
)

// LogWriter which defines the behavior of log writer.
type LogWriter interface {
	// WriteRecord write a record into the log file.
	WriteRecord(r []byte) (err error)
	// Flush sync data to disk.
	Flush() (err error)
}

// LogReader which defines the behavior of log reader.
type LogReader interface {
	// ReadRecord read a record from log file.
	ReadRecord(dst []byte) (uint64, err error)
}

const dir = "./log/"

// RecodeWriter which write data to the logger.
type RecodeWriter struct {
	writeAt uint64
	file    *os.File
}

func (w *RecodeWriter) Flush() (err error) {
	return w.file.Sync()
}

// nextFile create a new file io stream
func (w *RecodeWriter) nextFile() (err error) {
	AllocGen()
	w.writeAt = GenBuf
	w.file, err = os.Create(fmt.Sprintf("%s%d.log", dir, w.writeAt))
	return
}

// WriteRecord write a Record
func (w *RecodeWriter) WriteRecord(r []byte) (err error) {
	err = w.nextFile()
	if err != nil {
		return
	}
	_, err = w.file.Write(r)
	return
}

// NewRecodeWriter new a writer
func NewRecodeWriter() *RecodeWriter {
	return &RecodeWriter{
		writeAt: 0,
		file:    nil,
	}
}

// RecodeReader read a recode
type RecodeReader struct {
	offset int
	file   *os.File
}

// next change to the next file io steam
func (r *RecodeReader) next() (err error) {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return err
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name() < list[j].Name() })
	if r.offset >= len(list) {
		return errors.New("no next data")
	}
	fileName := list[r.offset].Name()
	r.file, err = os.Open(dir + fileName)
	r.offset++
	return err
}

// ReadRecord read a Record
func (r *RecodeReader) ReadRecord(dst []byte) (uint64, error) {
	err := r.next()
	if err != nil {
		return 0, err
	}
	stat, _ := r.file.Stat()
	fileSize := stat.Size()
	if fileSize > int64(len(dst)) {
		return 0, errors.New("slice to short")
	}
	l, err := r.file.Read(dst)
	return uint64(l), err
}

func NewRecodeReader() *RecodeReader {
	return &RecodeReader{
		offset: 0,
		file:   nil,
	}
}
