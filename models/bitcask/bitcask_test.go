package bitcask

import (
	"crypto/rand"
	"errors"
	"github.com/stretchr/testify/assert"
	"logicdb"
	"testing"
)

func newKernel() (*BKernel, error) {
	kernel := KernelWithConfig(NewBitCaskConfigWithDefault())
	err := kernel.Open("")

	return kernel, err
}

func TestBitCaskKernel_Open(t *testing.T) {
	_, err := newKernel()

	assert.Equal(t, err, nil)
}

func TestBitCask_Set(t *testing.T) {
	kernel, err := newKernel()
	kernel.bitCask.Set([]byte("yuuka"), []byte("my wife"))
	kernel.bitCask.Set([]byte("???"), []byte("test2"))

	assert.Equal(t, err, nil)
}

func TestBitCask_Get(t *testing.T) {
	kernel, err := newKernel()
	assert.Equal(t, err, nil)

	kernel.bitCask.Set([]byte("yuuka"), []byte("my wife"))

	res, err := kernel.bitCask.Get([]byte("yuuka"))
	assert.Equal(t, string(res), "my wife")

	kernel.bitCask.Set([]byte("yuuka"), []byte("my wife!"))

	res, err = kernel.bitCask.Get([]byte("yuuka"))
	assert.Equal(t, string(res), "my wife!")

	_, err = kernel.bitCask.Get([]byte("no"))

	var s = logicdb.WrapKeyNotFoundErr([]byte("no"))

	assert.Equal(t, errors.As(err, &s), true)
}

func TestBitCask_Remove(t *testing.T) {
	kernel, err := newKernel()
	assert.Equal(t, err, nil)

	kernel.bitCask.Set([]byte("yuuka"), []byte("my wife"))

	res, err := kernel.bitCask.Get([]byte("yuuka"))
	assert.Equal(t, string(res), "my wife")

	ok, _ := kernel.bitCask.Remove([]byte("yuuka"))
	assert.Equal(t, ok, true)

	_, err = kernel.bitCask.Get([]byte("yuuka"))

	var s = logicdb.WrapKeyNotFoundErr([]byte("no"))

	assert.Equal(t, errors.As(err, &s), true)
}

func TestBitCask_RemoveWithGet(t *testing.T) {
	kernel, err := newKernel()
	assert.Equal(t, err, nil)

	kernel.bitCask.Set([]byte("yuuka"), []byte("my wife"))

	res, err := kernel.bitCask.Get([]byte("yuuka"))
	assert.Equal(t, string(res), "my wife")

	res, ok, _ := kernel.bitCask.RemoveWithGet([]byte("yuuka"))
	assert.Equal(t, ok, true)
	assert.Equal(t, string(res), "my wife")

	_, err = kernel.bitCask.Get([]byte("yuuka"))

	var s = logicdb.WrapKeyNotFoundErr([]byte("no"))

	assert.Equal(t, errors.As(err, &s), true)
}

func TestSwitch(t *testing.T) {
	kernel, err := newKernel()
	assert.Equal(t, err, nil)

	key := make([]byte, 1)
	value := make([]byte, 1024*1024*1024-1)

	rand.Read(key)
	rand.Read(value)

	kernel.Set(key, value)

	rand.Read(key)
	rand.Read(value)

	kernel.Set(key, value)

	rand.Read(key)
	rand.Read(value)

	kernel.Set(key, value)
	//kernel.Set([]byte{2}, []byte{1})
}
