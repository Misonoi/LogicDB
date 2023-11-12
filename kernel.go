package logicdb

// Kernel which defines the behavior of database.
type Kernel interface {
	// Open a database witch path
	Open(path string) (err error)
	Get(key []byte) (value []byte, err error)
	Set(key []byte, value []byte) (err error)
	Remove(key []byte) (value []byte, err error)
}
