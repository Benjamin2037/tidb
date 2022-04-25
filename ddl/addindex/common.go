package addindex

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	lightcfg "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	prefix_str = "------->"
	_kb        = 1024
	_mb        = 1024 * _kb
	_gb        = 1024 * _mb
	flush_size = 8 * _mb
)

var (
	limit = int64(1024)
	tblId = time.Now().Unix()
	//
	IndexDDLLightning = flag.Bool("ddl-mode", true, "index ddl use sst mode")
	sortkv            = flag.String("sortkv", "/tmp", "temp file for sort kv")
	dq                = flag.Int64("disk-quota", math.MaxInt64, "quota for sortkv; 1 GB is '1073741824', default is MaxInt64(1<<63 - 1).")
	diskQuota         int64
	//
	cluster ClusterInfo
)

func LogInfo(format string, a ...interface{}) {
	logutil.BgLogger().Info(prefix_str + fmt.Sprintf(format, a...))
}

func LogDebug(format string, a ...interface{}) {
	logutil.BgLogger().Debug(prefix_str + fmt.Sprintf(format, a...))
}

func LogError(format string, a ...interface{}) {
	logutil.BgLogger().Error(prefix_str + fmt.Sprintf(format, a...))
}

func LogFatal(format string, a ...interface{}) {
	logutil.BgLogger().Fatal(prefix_str + fmt.Sprintf(format, a...))
}

// this log is for test; delete after finish or convert to other log level.
func LogTest(format string, a ...interface{}) {
	logutil.BgLogger().Error(prefix_str + "JUST FOR TEST " + fmt.Sprintf(format, a...))
}

type ExecPool interface {
	Get() (sqlexec.RestrictedSQLExecutor, error)
	Put(sqlexec.RestrictedSQLExecutor)
}

type DDLInfo struct {
	Schema  string
	Table   *model.TableInfo
	StartTs uint64
	Unique  bool
	Exec    ExecPool
}

// pdaddr; tidb-host/status
type ClusterInfo struct {
	PdAddr string
	// TidbHost string - 127.0.0.1
	Port   uint
	Status uint
}

type glue_ struct{}

func (_ glue_) OwnsSQLExecutor() bool {
	return false
}
func (_ glue_) GetSQLExecutor() glue.SQLExecutor {
	return nil
}
func (_ glue_) GetDB() (*sql.DB, error) {
	return nil, nil
}
func (_ glue_) GetParser() *parser.Parser {
	return nil
}
func (_ glue_) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}
func (_ glue_) GetSession(context.Context) (checkpoints.Session, error) {
	return nil, nil
}
func (_ glue_) OpenCheckpointsDB(context.Context, *lightcfg.Config) (checkpoints.DB, error) {
	return nil, nil
}
func (_ glue_) Record(string, uint64) {

}

func generateLightningConfig(info ClusterInfo, unique bool) *lightcfg.Config {
	cfg := lightcfg.NewConfig()
	cfg.DefaultVarsForImporterAndLocalBackend()
	name, err := ioutil.TempDir(*sortkv, "lightning")
	if err != nil {
		logutil.BgLogger().Warn(fmt.Sprintf("TempDir err:%s.", err.Error()))
		name = "/tmp/lightning"
	}
	os.Remove(name)
	LogDebug("./ %s.", name)
	cfg.Checkpoint.Enable = false
	cfg.TikvImporter.SortedKVDir = name
	cfg.TikvImporter.RangeConcurrency = 32
	cfg.TikvImporter.EngineMemCacheSize = 512 * _mb
	cfg.TikvImporter.LocalWriterMemCacheSize = 128 * _mb
	cfg.TiDB.PdAddr = info.PdAddr
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = int(info.Status)
	return cfg
}

func createLocalBackend(ctx context.Context, info ClusterInfo, unique bool) (backend.Backend, error) {
	cfg := generateLightningConfig(info, unique)
	tls, err := cfg.ToTLS()
	if err != nil {
		return backend.Backend{}, err
	}
	var g glue_
	return local.NewLocalBackend(ctx, tls, cfg, &g, int(limit), nil)
}
