// Copyright 2022 PingCAP, Inc.
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

package sessionutil_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/testkit"
	su "github.com/pingcap/tidb/util/sessionutil"
	requires "github.com/stretchr/testify/require"
)

func TestSessionCommit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	s := testkit.NewTestKit(t, store).Session()
	se := su.NewSession(s)
	se.Begin()
	se.Commit()
}

func TestSessionRollback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	s := testkit.NewTestKit(t, store).Session()
	se := su.NewSession(s)
	se.Begin()
	se.Rollback()
}

func TestSessionReset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	s := testkit.NewTestKit(t, store).Session()
	se := su.NewSession(s)
	se.Begin()
	se.Reset()
}

func TestSessionGetTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	s := testkit.NewTestKit(t, store).Session()
	se := su.NewSession(s)
	se.Begin()
	_, err := se.GetTxn()
	requires.NoError(t, err)
}

func TestSession(t *testing.T) {
	store := testkit.CreateMockStore(t)
	s := testkit.NewTestKit(t, store).Session()
	se := su.NewSession(s)
	se.Begin()
	s1 := se.Session()
	requires.Equal(t, s1, s)
}

func TestRunInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	s := testkit.NewTestKit(t, store).Session()
	se := su.NewSession(s)
	su.RunInTxn(se, func(se *su.Session) error {
		_, err := se.Execute(context.Background(), "use test", "test")
		return err
	})
}
