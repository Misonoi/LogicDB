package logicdb

import "fmt"

type KeyNotFoundErr struct {
	key []byte
}

func (e *KeyNotFoundErr) Error() string {
	return fmt.Sprintf("key: %v is not found", e.key)
}

type Result struct {
	err error
}
