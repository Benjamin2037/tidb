package addindex

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	filter_line        = `------->handleRange=physicalTableID_62_`
	repeatKey   string = `74800000000000003e5f72800000000032ee40000000`
	//
	fmtstr = `[2022/01/01 11:32:42.635 +08:00] [INFO] [common.go:35] ["------->handleRange=physicalTableID_62_[%d.next,%d](%s -> %s); %s -> %s."]`
)

type Range struct {
	prefix     string
	start, end int
	sk, ek     string
}

type mm struct {
	max, min string
}

type Record struct {
	Range
	mm
}

const (
	line_prefix   = "------->handleRange="
	rangeSplit    = ".next,"
	rangeKeySplit = " -> "
	rangeEnd      = "); "
	lineEnd       = ".\"]"
)

func parseRecord(line string) (r Record) {
	s := strings.Index(line, line_prefix) + len(line_prefix)
	e := s + strings.Index(line[s:], "[")
	r.prefix = line[s:e]
	//
	s = e + 1
	e = s + strings.Index(line[s:], rangeSplit)
	r.start, _ = strconv.Atoi(line[s:e])
	//
	s = e + len(rangeSplit)
	e = s + strings.Index(line[s:], "]")
	r.end, _ = strconv.Atoi(line[s:e])
	//
	s = e + 2
	e = s + strings.Index(line[s:], rangeKeySplit)
	r.sk = line[s:e]
	//
	s = e + len(rangeKeySplit)
	e = s + strings.Index(line[s:], rangeEnd)
	r.ek = line[s:e]
	//
	s = e + len(rangeEnd)
	e = s + strings.Index(line[s:], rangeKeySplit)
	r.min = line[s:e]
	//
	s = e + len(rangeKeySplit)
	e = s + strings.Index(line[s:], lineEnd)
	r.max = line[s:e]
	return
}

func TestHex2int(t *testing.T) {
	a := 0x32ecff
	b := 0x32ee40
	// a=3337792(32ee40);b=3337471(32ecff);
	// 3337471.next,3337792
	t.Logf("a=%d(%x);b=%d(%x);", a, a, b, b)
}

func TestParseRecord(t *testing.T) {
	linedata := `[2022/01/01 11:32:42.635 +08:00] [INFO] [common.go:35] ["------->handleRange=physicalTableID_62_[3337471.next,3337792](74800000000000003e5f72800000000032ecff00 -> 74800000000000003e5f72800000000032ee40); 74800000000000003e5f72800000000032ed00000000 -> 74800000000000003e5f72800000000032edff000000."]`
	r := parseRecord(linedata)
	assert.Equal(t, 3337471, r.start)
	assert.Equal(t, 3337792, r.end)
	assert.Equal(t, "74800000000000003e5f72800000000032ecff00", r.sk)
	assert.Equal(t, "74800000000000003e5f72800000000032ee40", r.ek)

	assert.Equal(t, "74800000000000003e5f72800000000032ed00000000", r.min)
	assert.Equal(t, "74800000000000003e5f72800000000032edff000000", r.max)

	assert.Less(t, strings.Compare(r.min, r.max), 0)
	assert.Less(t, strings.Compare(r.sk, r.ek), 0)

	assert.Less(t, strings.Compare(r.sk, r.min), 0)
	assert.Less(t, strings.Compare(r.max, r.ek), 0)
}

type pair struct {
	a int
	b string
}

const (
	key = "paris"
)

func run(ctx context.Context) {
	ps := ctx.Value(key).([]pair)
	select {
	case <-ctx.Done():
		fmt.Printf("run finish:%+v.\n", ps)
	}
}

func TestContext(t *testing.T) {
	pairs := []pair{{1, "1"}, {3, "3"}, {2, "2"}}
	ctx, cancel := context.WithCancel(context.TODO())
	pc := context.WithValue(ctx, key, pairs)
	go run(pc)
	time.Sleep(time.Second)
	cancel()
	time.Sleep(time.Second)
}

func TestDef(t *testing.T) {
	func() (v int) {
		var iv = 10
		defer func() {
			t.Logf("v=%d;iv=%d", v, iv)
		}()
		return iv
	}()
}
