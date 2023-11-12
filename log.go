package marker

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
