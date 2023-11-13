package logicdb

// Kernel which defines the behavior of database.
type Kernel interface {
	// Open a database witch path
	Open(path string) error
	Get(key []byte) ([]byte, error)
	Set(key []byte, value []byte) error
	RemoveWithGet(key []byte) ([]byte, bool, error)
	Remove(key []byte) (bool, error)
	Contains(key []byte) (bool, error)
}
