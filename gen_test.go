package logicdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
)

func TestGen(t *testing.T) {
	var wg sync.WaitGroup

	fmt.Printf("%v", GEN_BUF)

	numGoroutines := 10

	expectedFinalValue := GEN_BUF + int64(numGoroutines)

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			AllocGen()
			wg.Done()
		}()
	}

	wg.Wait()

	assert.Equal(t, expectedFinalValue, atomic.LoadInt64(&GEN_BUF))
}
