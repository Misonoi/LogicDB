package logicdb

import (
	"sync/atomic"
	"time"
)

var (
	GenBuf uint64 = 0
)

// initialize the gen_buf with current timestamp.
func init() {
	GenBuf = uint64(time.Now().Unix())
}

// AllocGen new gen.
func AllocGen() {
	atomic.CompareAndSwapUint64(&GenBuf, GenBuf, GenBuf+1)
}
