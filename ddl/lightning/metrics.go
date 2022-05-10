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
package lightning

import (
	"fmt"
	"strings"
	"time"
)

// Caculate size:
// 1. raw size of kv before encode
// 2. size of kv after encode.
// 3. rate for  raw/size.

const (
	// Key:   tablePrefix{tableID}_indexPrefixSep{indexID}_indexedColumnsValue
	prefix_size = 1 + 8 + 1 + 1 + 8 + 1
)

type KVSize struct {
	rawSize int64
	size    int64
}

type statKVSize struct {
	// kvSizes []KVSize
	encodeSize int64
	count      int64
}

type TimeNode struct {
	start, end time.Time
}

type statTime struct {
	preprare       TimeNode
	localWrite     []TimeNode
	importAndReset []TimeNode
	finish         TimeNode
}

type AddIndexState int

const (
	ISPrepare AddIndexState = iota
	ISWrite
	ISImportAndReset
	ISFinish
)

func (v AddIndexState) String() string {
	switch v {
	case ISPrepare:
		return "Prepare Engines"
	case ISWrite:
		return "Write to LocalKV"
	case ISImportAndReset:
		return "Overload disk,import and reset engine"
	case ISFinish:
		return "Import to TiKV"
	default:
		return fmt.Sprintf("unknow state '%d'", v)
	}
}

var (
	sn StatNode
)

type StatNode struct {
	Name    string
	szInc   statKVSize
	szTotal statKVSize
	st      statTime
	err     error
	state   AddIndexState
}

func (sn *StatNode) reset(name string) {
	var s StatNode
	s.Name = name
	*sn = s
}

func (sn *StatNode) inc(count, size int) {
	sn.szInc.encodeSize += int64(size)
	sn.szInc.count += int64(count)
	sn.szTotal.encodeSize += int64(size)
	sn.szTotal.count += int64(count)
}

func (sn *StatNode) preprareCost(start time.Time) {
	sn.st.preprare.start = start
	sn.st.preprare.end = time.Now()
	sn.state = ISPrepare
}

func (sn *StatNode) writeCost(start time.Time) {
	sn.st.localWrite = append(sn.st.localWrite, TimeNode{start, time.Now()})
	sn.state = ISWrite
}

func (sn *StatNode) importAndReset(start time.Time) {
	sn.st.importAndReset = append(sn.st.importAndReset, TimeNode{start, time.Now()})
	sn.state = ISImportAndReset
}

func (sn *StatNode) finishCost(start time.Time) {
	sn.st.finish.start = start
	sn.st.finish.end = time.Now()
	sn.state = ISFinish
}

func (sn *StatNode) setErr(err error) {
	sn.err = err
}

func (sn *StatNode) cost() time.Duration {
	start := sn.st.preprare.start
	switch sn.state {
	case ISPrepare:
		return sn.st.preprare.end.Sub(start)
	case ISWrite:
		last := len(sn.st.localWrite) - 1
		return sn.st.localWrite[last].end.Sub(start)
	case ISFinish:
		return sn.st.finish.end.Sub(start)
	}
	return 0
}

func size2str(sz int64) string {
	sub := "B"
	var p int64
	switch {
	case sz < _kb:
		p = 0
	case sz < _mb:
		p = sz % _kb % 1000
		sz /= _kb
		sub = "KB"
	case sz < _gb:
		p = sz % _mb % 1000
		sz /= _mb
		sub = "MB"
	default:
		p = sz % _gb % 1000
		sz /= _gb
		sub = "GB"
	}
	return fmt.Sprintf("%d.%03d %s", sz, p, sub)
}

func (sn *StatNode) SizeInfo() string {
	ss := sn.szTotal
	raw := ss.encodeSize - ss.count*prefix_size
	return fmt.Sprintf("%d kvs;raw %s;size %s.", ss.count, size2str(raw), size2str(ss.encodeSize))
}

func (sn *StatNode) writeInfo() string {
	lw := sn.st.localWrite
	n := len(lw)
	if n == 0 {
		return fmt.Sprintf("write : there is no kv to write")
	}

	var diff time.Duration
	for _, st := range sn.st.localWrite {
		diff = st.end.Sub(st.start)
	}
	avg := time.Duration(int64(diff) / int64(n))
	return fmt.Sprintf("write %d times,cost %v,avg %v.", n, diff.String(), avg.String())
}

func (sn *StatNode) ImportResetInfo() string {
	lw := sn.st.importAndReset
	n := len(lw)
	if n == 0 {
		return fmt.Sprintf("")
	}

	var diff time.Duration
	for _, st := range sn.st.localWrite {
		diff = diff + st.end.Sub(st.start)
	}
	avg := time.Duration(int64(diff) / int64(n))
	return fmt.Sprintf("import-reset cost %v run %d times,avg %v.", diff.String(), n, avg.String())
}

func (sn *StatNode) Report() string {

	var sb strings.Builder
	sb.WriteString("Reporter of Add Index ")
	sb.WriteString(sn.Name)
	sb.WriteString(", cost ")
	sb.WriteString(sn.cost().String())
	sb.WriteByte(',')
	if sn.err != nil {
		sb.WriteString("execute failed : ")
		sb.WriteString(sn.err.Error())
		sb.WriteByte('\n')
		return sb.String()
	}
	sb.WriteString("execute success : \n\t")

	st := &sn.st
	// prepare
	sb.WriteString(ISPrepare.String())
	sb.WriteByte(' ')
	sb.WriteString(st.preprare.end.Sub(st.preprare.start).String())
	sb.WriteByte(';')
	// local write
	sb.WriteString(ISWrite.String())
	sb.WriteByte(' ')
	sb.WriteString(sn.writeInfo())
	sb.WriteByte(';')
	// import and reset;
	if len(sn.st.importAndReset) > 0 {
		sb.WriteString(ISImportAndReset.String())
		sb.WriteByte(' ')
		sb.WriteString(sn.ImportResetInfo())
		sb.WriteByte(';')
	}

	// import
	sb.WriteString(ISFinish.String())
	sb.WriteByte(' ')
	sb.WriteString(st.finish.end.Sub(st.finish.start).String())
	sb.WriteString(";\n\t")
	// size info
	sb.WriteString(sn.SizeInfo())
	sb.WriteByte('\n')
	return sb.String()
}
