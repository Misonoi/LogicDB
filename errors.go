package marker

import "fmt"

type ErrKeyNotFound struct {
	key []byte
}

func (e *ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key: %v is not found", e.key)
}

type Result struct {
	err error
}
