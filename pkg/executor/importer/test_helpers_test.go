// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer_test

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func skipIfCannotListen(t *testing.T) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("skip test because listen on tcp is not permitted: %v", err)
		return
	}
	_ = l.Close()

	socket := filepath.Join(t.TempDir(), "sock")
	ul, err := net.Listen("unix", socket)
	if err != nil {
		t.Skipf("skip test because listen on unix socket is not permitted: %v", err)
		return
	}
	_ = ul.Close()
}

func importerFailpointsEnabled() bool {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return false
	}
	dir := filepath.Dir(file)
	_, err := os.Stat(filepath.Join(dir, "_curpkg_.go"))
	return err == nil
}
