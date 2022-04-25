package addindex

import (
	"flag"
	"sync"
)

var (
	ck = flag.Bool("check", false, "check test")
)

// 该文件，主要是为了 测试、校验、去重 等等.
// should restart where need check.
var (
	mtx   sync.Mutex
	check = make(map[string]bool, 100*_mb)
)

func DoCheckSame(key string) bool {
	if false == *ck {
		return true
	}
	mtx.Lock()
	exist := check[key]
	if !exist {
		check[key] = true
	}
	mtx.Unlock()
	return exist
}
