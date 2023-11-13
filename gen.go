package logicdb

import (
	"sync/atomic"
	"time"
)

var (
	GEN_BUF int64 = 0
)

// initialize the gen_buf with current timestamp.
func init() {
	GEN_BUF = time.Now().Unix()
}

// AllocGen new gen.
func AllocGen() {
	atomic.CompareAndSwapInt64(&GEN_BUF, GEN_BUF, GEN_BUF+1)
}
