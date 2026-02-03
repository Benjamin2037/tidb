# RFC: TiDB Built-in SQL Diagnostics

## Summary

Today TiDB diagnostics rely mainly on external tools (perf/iosnoop/iotop/vmstat/sar/... ), monitoring systems (Prometheus/Grafana), log files, HTTP APIs, and TiDB system tables. The fragmented toolchain and complex access paths raise the entry bar for operating a TiDB cluster, increase operational cost, make it hard to discover issues early, and slow down troubleshooting, diagnosis, and recovery.

This proposal adds built-in diagnostics in TiDB and exposes the data via system tables, so users can query diagnostics with SQL.

## Motivation

This proposal addresses the following pain points in TiDB diagnostics:

- Toolchains are fragmented. Users need to switch between tools, and some Linux distributions do not ship these tools or ship different versions.
- Diagnostic access paths are inconsistent: SQL, HTTP, exported metrics, or logging into each node to inspect logs.
- There are many TiDB cluster components; comparing and correlating metrics across components is inefficient.
- TiDB has no centralized log management, so filtering, searching, analyzing, and aggregating logs for the whole cluster is hard.
- System tables only contain the current node state and do not reflect the whole cluster, such as SLOW_QUERY, PROCESSLIST, STATEMENTS_SUMMARY.

By providing multi-dimensional cluster-level system tables and a cluster diagnosis rule framework, the solution improves cluster-wide info queries, state inspection, log search, one-click inspection, and fault diagnosis. It also provides a base for future anomaly alerting.

## Detailed Design

### System Overview

The implementation has four layers:

- L1: On each node, implement data collection modules, including TiDB/TiKV/PD metrics, hardware info, OS network IO and disk IO, CPU usage, and memory usage.
- L2: Call the collection modules and expose data through external service interfaces (HTTP API or gRPC service) so TiDB can fetch node data.
- L3: TiDB pulls data from nodes, aggregates it, and exposes it through system tables.
- L4: The diagnosis framework queries system tables to get cluster state and produces diagnosis results based on rules.

Data flow from collection to diagnosis:

```
+-L1--------------+             +-L3-----+
| +-------------+ |             |        |
| |   Metrics   | |             |        |
| +-------------+ |             |        |
| +-------------+ |             |        |
| |   Disk IO   | +---L2:gRPC-->+        |
| +-------------+ |             |        |
| +-------------+ |             |  TiDB  |
| |  Network IO | |             |        |
| +-------------+ |             |        |
| +-------------+ |             |        |
| |   Hardware  | +---L2:HTTP-->+        |
| +-------------+ |             |        |
| +-------------+ |             |        |
| | System Info | |             |        |
| +-------------+ |             |        |
+-----------------+             +---+----+
                                    | 
                   +---infoschema---+ 
                   |                  
                   v                  
+-L4---------------+---------------------+
|                                        |
|          Diagnosis Framework           |
|                                        |
| +---------+ +---------+  +---------+   |
| | rule1   | |  rule2  |  |  rule3  |   |
| +---------+ +---------+  +---------+   |
+----------------------------------------+
```

### System Information Collection

All three components (TiDB, TiKV, PD) need system information collection modules. TiDB and PD share Go implementation, while TiKV uses Rust.

#### Node Hardware Info

Each node should collect:

- CPU info: physical cores, logical cores, NUMA info, CPU frequency, CPU vendor, L1/L2/L3 cache sizes
- Network interface info: device name, enabled or not, vendor, model, bandwidth, driver version, queue count (optional)
- Disk info: disk name, capacity, usage, partitions, mounts
- USB device list
- Memory info

#### Node System Info

Each node should collect:

- CPU usage, 1/5/15 minute load
- Memory: Total/Free/Available/Buffers/Cached/Active/Inactive/Swap
- Disk IO:
    - tps: transfers per second for the device
    - rrqm/s: read requests merged per second
    - wrqm/s: write requests merged per second
    - r/s: reads per second
    - w/s: writes per second
    - rsec/s: sectors read per second
    - wsec/s: sectors written per second
    - avgrq-sz: average request size in sectors
    - avgqu-sz: average request queue length
    - await: average time per IO request (units depend on source)
    - svctm: average service time per IO operation (ms)
    - %util: fraction of time spent on IO during the interval
- Network IO
    - IFACE: LAN interface
    - rxpck/s: packets received per second
    - txpck/s: packets sent per second
    - rxbyt/s: bytes received per second
    - txbyt/s: bytes sent per second
    - rxcmp/s: compressed packets received per second
    - txcmp/s: compressed packets sent per second
    - rxmcst/s: multicast packets received per second
- Common system settings: sysctl -a

#### Node Configuration Info

Each node has its effective runtime configuration. No extra steps are needed to collect it.

#### Node Log Info

Logs from TiDB/TiKV/PD are stored on each node. Because the cluster does not deploy a separate log collection component, log search has the following issues:

- Logs are distributed across nodes; users must log in to each node and search by keyword.
- Logs rotate daily, so multiple files must be searched on a single node.
- There is no easy way to merge logs from many nodes in time order.

This proposal considers two approaches:

- Introduce a third-party log collector to gather logs from all nodes.
    - Pros: centralized log management; long retention; easy search; multi-component logs can be merged by time.
    - Cons: higher operational complexity; hard to integrate with TiDB SQL; full-log collection consumes disk and network IO.
- Each node provides a log service. TiDB pushes down predicates in log search SQL to the log service on each node and merges the filtered results.
    - Pros: no third-party component; predicate pushdown returns only filtered logs; easy to integrate with TiDB SQL and reuse SQL engine filtering and aggregation.
    - Cons: deleted node logs cannot be searched.

Based on the analysis, this proposal chooses the second approach: each node provides a log search interface, and TiDB pushes down log predicates. The log search interface semantics: search local log files with predicates and return matching results.

- `start_time`: log search start time (unix timestamp in ms). If not provided, default is 0.
- `end_time`: log search end time (unix timestamp in ms). If not provided, default is `int64::MAX`.
- `pattern`: keyword filter, e.g. SELECT * FROM cluster_log WHERE pattern LIKE "%gc%".
- `level`: log level: DEBUG/INFO/WARN/WARNING/TRACE/CRITICAL/ERROR.
- `limit`: result count limit. If not provided, default to 64k to avoid large network transfers.

#### Node Performance Sampling Data

When a TiDB cluster has performance bottlenecks, users need fast diagnosis. Flame Graph, invented by Brendan Gregg, provides a global view of time distribution by listing all possible call stacks from bottom to top. Other formats only show a single stack or non-hierarchical time distribution.

Today TiKV and TiDB use different approaches and both depend on external tools.

- TiKV flame graph

    ```
    perf record -F 99 -p proc_pid -g -- sleep 60
    perf script > out.perf
    /opt/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
    /opt/FlameGraph/flamegraph.pl out.folded > cpu.svg
    ```

- TiDB flame graph

    ```
    curl http://127.0.0.1:10080/debug/pprof/profile > cpu.pprof
    go tool pprof -svg cpu.svn cpu.pprof
    ```

Two key problems:

- Production environments may not include the external tools (perf/flamegraph.pl/go).
- TiKV and TiDB do not share a unified method.

To solve this, the proposal integrates flame graph sampling into TiDB. SQL triggers sampling and returns the flame graph result, reducing external dependencies and improving efficiency. Each node collects sampling data and exposes it, with a unified output format. The output format is the ProtoBuf format defined by `pprof`.

Sampling data sources:

- TiDB/PD: use Go runtime built-in profiling APIs
- TiKV: use the `pprof-rs` library

#### Node Monitoring Metrics

Monitoring metrics are defined inside each component. TiDB/TiKV/PD provide a `/metrics` HTTP API, and Prometheus pulls metrics periodically (default 15s). Grafana visualizes the metrics.

Metrics are time series data and are essential for diagnosis. To query metrics with SQL inside TiDB, there are two options:

- Query Prometheus server with Prometheus client and PromQL.
    - Pros: existing solution; only need to register Prometheus server address in TiDB.
    - Cons: stronger dependency on Prometheus; harder to remove Prometheus later.
- Store recent metrics (initially 1 day) in PD and query from PD.
    - Pros: no dependency on Prometheus; helps future removal of Prometheus components.
    - Cons: needs time series storage and query engine; high implementation cost.

This proposal prefers option 2. To reduce scope, the feature is split into phases (phase 3 is optional):

1. Add `remote-metrics-storage` in PD config. It initially points to Prometheus. PD acts as a proxy, forwarding queries to Prometheus. Considerations:
    - PD can later implement its own query interface without changing TiDB.
    - Users can use their own monitoring service and still query metrics and diagnostics via SQL.
2. Extract Prometheus time series storage and query modules and embed them in PD.
3. Implement native time series storage and query inside PD (similar to CockroachDB).

##### PD Performance Analysis

PD mainly serves cluster scheduling and TSO:

1. TSO increments a single atomic variable in leader memory.
2. Scheduling operators and steps are stored in memory and updated by region heartbeat.

Therefore, in most cases the performance impact of adding monitoring on PD is negligible.

### System Information Access

TiDB/TiKV/PD already expose some system information via HTTP API, and PD mainly provides services via HTTP. This proposal reuses existing logic and uses HTTP API for some data (such as configuration).

TiKV plans to remove HTTP API entirely. Except for reusing existing endpoints, no new HTTP APIs will be added. Log search, hardware info, and system info will be defined as gRPC services, implemented by each component and registered on startup.

#### gRPC Service Definition

```proto
// Diagnostics service for TiDB cluster components.
service Diagnostics {
	// Searchs log in the target node
	rpc search_log(SearchLogRequest) returns (SearchLogResponse) {};
	// Retrieves server info in the target node
	rpc server_info(ServerInfoRequest) returns (ServerInfoResponse) {};
}

enum LogLevel {
	Debug = 0;
	Info = 1;
	Warn = 2;
	Trace = 3;
	Critical = 4;
	Error = 5;
}

message SearchLogRequest {
	int64 start_time = 1;
	int64 end_time = 2;
	LogLevel level = 3;
	string pattern = 4;
	int64 limit = 5;
}

message SearchLogResponse {
	repeated LogMessage messages = 1;
}

message LogMessage {
	int64 time = 1;
	LogLevel level = 2;
	string message = 3;
}

enum ServerInfoType {
	All = 0;
	HardwareInfo = 1;
	SystemInfo = 2;
	LoadInfo = 3;
}

message ServerInfoRequest {
	ServerInfoType tp = 1;
}

message ServerInfoItem {
	// cpu, memory, disk, network ...
	string tp = 1;
	// eg. network: lo1/eth0, cpu: core1/core2, disk: sda1/sda2 
	string name = 2;
	string key = 3;
	string value = 4;
}

message ServerInfoResponse {
	repeated ServerInfoItem items = 1;
}
```

#### Reusable HTTP APIs

TiDB/TiKV/PD already provide some HTTP APIs. This proposal does not migrate them to gRPC yet; migration can be done later. All HTTP APIs return JSON. APIs that may be used include:

- Get configuration
    - PD: /pd/api/v1/config
    - TiDB/TiKV: /config
- Profiling APIs: TiDB/PD include all below; TiKV only provides CPU profiling initially
    - CPU: /debug/pprof/profile
    - Memory: /debug/pprof/heap
    - Allocs: /debug/pprof/allocs
    - Mutex: /debug/pprof/mutex
    - Block: /debug/pprof/block

### Cluster Information System Tables

Each TiDB instance can access other nodes via HTTP APIs or gRPC services to build a cluster Global View. The proposal adds system tables to expose cluster information for:

- End users: query cluster info with SQL to troubleshoot issues.
- Ops systems: integrate TiDB into operational tooling across environments.
- Ecosystem tools: use SQL to build features. For example, `sqltop` can read cluster `statements_summary` to get SQL samples for the whole cluster.

#### Cluster Topology System Table

To provide a Global View, TiDB needs a topology table with each node's HTTP API and gRPC service address. This makes it easy to build endpoints and fetch data.

Expected SQL result:

```
mysql> use information_schema;
Database changed

mysql> desc CLUSTER_INFO;
+----------------+---------------------+------+------+---------+-------+
| Field          | Type                | Null | Key  | Default | Extra |
+----------------+---------------------+------+------+---------+-------+
| TYPE           | varchar(64)         | YES  |      | NULL    |       |
| ADDRESS        | varchar(64)         | YES  |      | NULL    |       |
| STATUS_ADDRESS | varchar(64)         | YES  |      | NULL    |       |
| VERSION        | varchar(64)         | YES  |      | NULL    |       |
| GIT_HASH       | varchar(64)         | YES  |      | NULL    |       |
+----------------+---------------------+------+------+---------+-------+
5 rows in set (0.00 sec)

mysql> select TYPE, ADDRESS, STATUS_ADDRESS,VERSION from CLUSTER_INFO;
+------+-----------------+-----------------+-----------------------------------------------+
| TYPE | ADDRESS         | STATUS_ADDRESS  | VERSION                                       |
+------+-----------------+-----------------+-----------------------------------------------+
| tidb | 127.0.0.1:4000  | 127.0.0.1:10080 | 5.7.25-TiDB-v4.0.0-alpha-793-g79eef48a3-dirty |
| pd   | 127.0.0.1:2379  | 127.0.0.1:2379  | 4.0.0-alpha                                   |
| tikv | 127.0.0.1:20160 | 127.0.0.1:20180 | 4.0.0-alpha                                   |
+------+-----------------+-----------------+-----------------------------------------------+
3 rows in set (0.00 sec)
```

#### Metrics System Tables

Metrics change over time and each metric can have multiple expressions for different dimensions. To provide a flexible system table framework, this proposal maps expressions to tables in the `metrics_schema` database. Expressions can be associated with tables in several ways:

- Define in config file

    ```
    # tidb.toml
    [metrics_schema]
    qps = `sum(rate(tidb_server_query_total[$STEP])) by (result)`
    memory_usage = `process_resident_memory_bytes{job="tidb"}`
    goroutines = `rate(go_gc_duration_seconds_sum{job="tidb"}[$STEP])`
    ```

- HTTP API injection

    ```
    curl -XPOST http://host:port/metrics_schema?name=distsql_duration&expr=`histogram_quantile(0.999, 
    sum(rate(tidb_distsql_handle_query_duration_seconds_bucket[$STEP])) by (le, type))`
    ```

- Special SQL command

    ```
    mysql> admin metrics_schema add parse_duration `histogram_quantile(0.95, sum(rate(tidb_session_parse_duration_seconds_bucket[$STEP])) by (le, sql_type))`
    ```

- Load from file

    ```
    mysql> admin metrics_schema load external_metrics.txt
    #external_metrics.txt
    execution_duration = `histogram_quantile(0.95, sum(rate(tidb_session_execute_duration_seconds_bucket[$STEP])) by (le, sql_type))`
    pd_client_cmd_ops = `sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{type!="tso"}[$STEP])) by (type)`
    ```

After adding these tables, they appear in `metrics_schema`:

```
mysql> use metrics_schema;
Database changed

mysql> show tables;
+-------------------------------------+
| Tables_in_metrics_schema            |
+-------------------------------------+
| qps                                 |
| memory_usage                        |
| goroutines                          |
| distsql_duration                    |
| parse_duration                      |
| execution_duration                  |
| pd_client_cmd_ops                   |
+-------------------------------------+
7 rows in set (0.00 sec)
```

The table schema depends on the PromQL result. Example expression: `sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{type!="tso"}[1m]offset 0)) by (type)` produces:

| Element | Value |
|---------|-------|
| {type="update_gc_safe_point"} | 0 |
| {type="wait"} | 2.910521666666667 |
| {type="get_all_stores"} | 0 |
| {type="get_prev_region"} | 0 |
| {type="get_region"} | 0 |
| {type="get_region_byid"} | 0 |
| {type="scan_regions"} | 0 |
| {type="tso_async_wait"} | 2.910521666666667 |
| {type="get_operator"} | 0 |
| {type="get_store"} | 0 |
| {type="scatter_region"} | 0 |

Mapped table schema and query results:

```
mysql> desc pd_client_cmd_ops;
+------------+-------------+------+-----+-------------------+-------+
| Field      | Type        | Null | Key | Default           | Extra |
+------------+-------------+------+-----+-------------------+-------+
| address    | varchar(32) | YES  |     | NULL              |       |
| type       | varchar(32) | YES  |     | NULL              |       |
| value      | float       | YES  |     | NULL              |       |
| interval   | int         | YES  |     | 60                |       |
| start_time | int         | YES  |     | CURRENT_TIMESTAMP |       |
| end_time   | int         | YES  |     |                   |       |
| end_time   | int         | YES  |     |                   |       |
| step       | int         | YES  |     |                   |       |
+------------+-------------+------+-----+-------------------+-------+
3 rows in set (0.02 sec)

mysql> select address, type, value from pd_client_cmd_ops;
+------------------+----------------------+---------+
| address          | type                 | value   |
+------------------+----------------------+---------+
| 172.16.5.33:2379 | update_gc_safe_point |       0 |
| 172.16.5.33:2379 | wait                 | 2.91052 |
| 172.16.5.33:2379 | get_all_stores       |       0 |
| 172.16.5.33:2379 | get_prev_region      |       0 |
| 172.16.5.33:2379 | get_region           |       0 |
| 172.16.5.33:2379 | get_region_byid      |       0 |
| 172.16.5.33:2379 | scan_regions         |       0 |
| 172.16.5.33:2379 | tso_async_wait       | 2.91052 |
| 172.16.5.33:2379 | get_operator         |       0 |
| 172.16.5.33:2379 | get_store            |       0 |
| 172.16.5.33:2379 | scatter_region       |       0 |
+------------------+----------------------+---------+
11 rows in set (0.00 sec)

mysql> select address, type, value from pd_client_cmd_ops where start_time='2019-11-14 10:00:00' and end_time='2019-11-14 10:05:00';
+------------------+----------------------+---------+
| address          | type                 | value   |
+------------------+----------------------+---------+
| 172.16.5.33:2379 | update_gc_safe_point |       0 |
| 172.16.5.33:2379 | wait                 | 0.82052 |
| 172.16.5.33:2379 | get_all_stores       |       0 |
| 172.16.5.33:2379 | get_prev_region      |       0 |
| 172.16.5.33:2379 | get_region           |       0 |
| 172.16.5.33:2379 | get_region_byid      |       0 |
| 172.16.5.33:2379 | scan_regions         |       0 |
| 172.16.5.33:2379 | tso_async_wait       | 0.82052 |
| 172.16.5.33:2379 | get_operator         |       0 |
| 172.16.5.33:2379 | get_store            |       0 |
| 172.16.5.33:2379 | scatter_region       |       0 |
+------------------+----------------------+---------+
11 rows in set (0.00 sec)
```

For PromQL expressions with multiple labels, the table will contain multiple columns. Users can filter and aggregate with SQL to get the desired results.

#### Node Profiling System Tables

Fetch profiling data from each node via `/debug/pprof/profile`, aggregate it, and expose it through SQL. Because SQL cannot return SVG, we need a text output format.

Key properties of flame graphs:

- Provide a global view
- Show all call paths
- Present a hierarchical structure

This proposal focuses on those properties rather than the graphical format. The final plan: aggregate samples and output all call paths in a tree, one line per path.

How the solution maps to the properties:

- Global view: show each aggregated result in a separate column for easy filtering and sorting.
- All call paths: output all paths and label each subtree so users can filter a subtree.
- Hierarchical structure: display stacks as a tree and record stack depth in a separate column.

Profiling tables to implement:

| Table | Description |
|------|-----|
| tidb_profile_cpu | TiDB CPU flame graph |
| tikv_profile_cpu | TiKV CPU flame graph |
| tidb_profile_block | TiDB blocking flame graph |
| tidb_profile_memory | TiDB memory object flame graph |
| tidb_profile_allocs | memory allocation flame graph |
| tidb_profile_mutex | lock contention flame graph |
| tidb_profile_goroutines | goroutines in the system; used to find leaks or blocking |

#### Global Memory Tables

`slow_query`/`statements_summary`/`processlist` currently contain only single-node data. This proposal adds cluster-level tables so any TiDB instance can view the entire cluster:

| Table | Description |
|------|-----|
| cluster_slow_query | slow_query from all TiDB nodes |
| cluster_statements_summary | statements_summary from all TiDB nodes |
| cluster_processlist | processlist from all TiDB nodes |

#### Configuration for All Nodes

For a large cluster, fetching config from each node via HTTP API is inefficient. This proposal provides a cluster-level config table for easier filtering and aggregation.

Expected output:

```
mysql> use information_schema;
Database changed

mysql> select * from cluster_config where `key` like 'log%';
+------+-----------------+-----------------------------+---------------+
| TYPE | ADDRESS         | KEY                         | VALUE         |
+------+-----------------+-----------------------------+---------------+
| pd   | 127.0.0.1:2379  | log-file                    |               |
| pd   | 127.0.0.1:2379  | log-level                   |               |
| pd   | 127.0.0.1:2379  | log.development             | false         |
| pd   | 127.0.0.1:2379  | log.disable-caller          | false         |
| pd   | 127.0.0.1:2379  | log.disable-error-verbose   | true          |
| pd   | 127.0.0.1:2379  | log.disable-stacktrace      | false         |
| pd   | 127.0.0.1:2379  | log.disable-timestamp       | false         |
| pd   | 127.0.0.1:2379  | log.file.filename           |               |
| pd   | 127.0.0.1:2379  | log.file.log-rotate         | true          |
| pd   | 127.0.0.1:2379  | log.file.max-backups        | 0             |
| pd   | 127.0.0.1:2379  | log.file.max-days           | 0             |
| pd   | 127.0.0.1:2379  | log.file.max-size           | 0             |
| pd   | 127.0.0.1:2379  | log.format                  | text          |
| pd   | 127.0.0.1:2379  | log.level                   |               |
| pd   | 127.0.0.1:2379  | log.sampling                | <nil>         |
| tidb | 127.0.0.1:4000  | log.disable-error-stack     | <nil>         |
| tidb | 127.0.0.1:4000  | log.disable-timestamp       | <nil>         |
| tidb | 127.0.0.1:4000  | log.enable-error-stack      | <nil>         |
| tidb | 127.0.0.1:4000  | log.enable-timestamp        | <nil>         |
| tidb | 127.0.0.1:4000  | log.expensive-threshold     | 10000         |
| tidb | 127.0.0.1:4000  | log.file.filename           |               |
| tidb | 127.0.0.1:4000  | log.file.max-backups        | 0             |
| tidb | 127.0.0.1:4000  | log.file.max-days           | 0             |
| tidb | 127.0.0.1:4000  | log.file.max-size           | 300           |
| tidb | 127.0.0.1:4000  | log.format                  | text          |
| tidb | 127.0.0.1:4000  | log.level                   | info          |
| tidb | 127.0.0.1:4000  | log.query-log-max-len       | 4096          |
| tidb | 127.0.0.1:4000  | log.record-plan-in-slow-log | 1             |
| tidb | 127.0.0.1:4000  | log.slow-query-file         | tidb-slow.log |
| tidb | 127.0.0.1:4000  | log.slow-threshold          | 300           |
| tikv | 127.0.0.1:20160 | log-file                    |               |
| tikv | 127.0.0.1:20160 | log-level                   | info          |
| tikv | 127.0.0.1:20160 | log-rotation-timespan       | 1d            |
+------+-----------------+-----------------------------+---------------+
33 rows in set (0.00 sec)

mysql> select * from cluster_config where type='tikv' and `key` like 'raftdb.wal%';
+------+-----------------+---------------------------+--------+
| TYPE | ADDRESS         | KEY                       | VALUE  |
+------+-----------------+---------------------------+--------+
| tikv | 127.0.0.1:20160 | raftdb.wal-bytes-per-sync | 512KiB |
| tikv | 127.0.0.1:20160 | raftdb.wal-dir            |        |
| tikv | 127.0.0.1:20160 | raftdb.wal-recovery-mode  | 2      |
| tikv | 127.0.0.1:20160 | raftdb.wal-size-limit     | 0KiB   |
| tikv | 127.0.0.1:20160 | raftdb.wal-ttl-seconds    | 0      |
+------+-----------------+---------------------------+--------+
5 rows in set (0.01 sec)
```

#### Node Hardware/System/Load Tables

Based on the gRPC service, each `ServerInfoItem` has a name and key-value pairs. When presenting to users, add node type and node address.

```
mysql> use information_schema;
Database changed

mysql> select * from cluster_hardware
+------+-----------------+----------+----------+-------------+--------+
| TYPE | ADDRESS         | HW_TYPE  | HW_NAME  | KEY         | VALUE  |
+------+-----------------+----------+----------+-------------+--------+
| tikv | 127.0.0.1:20160 | cpu      | cpu-1    | frequency   | 3.3GHz |
| tikv | 127.0.0.1:20160 | cpu      | cpu-2    | frequency   | 3.6GHz |
| tikv | 127.0.0.1:20160 | cpu      | cpu-1    | core        | 40     |
| tikv | 127.0.0.1:20160 | cpu      | cpu-2    | core        | 48     |
| tikv | 127.0.0.1:20160 | cpu      | cpu-1    | vcore       | 80     |
| tikv | 127.0.0.1:20160 | cpu      | cpu-2    | vcore       | 96     |
| tikv | 127.0.0.1:20160 | network  | memory   | capacity    | 256GB  |
| tikv | 127.0.0.1:20160 | network  | lo0      | bandwidth   | 10000M |
| tikv | 127.0.0.1:20160 | network  | eth0     | bandwidth   | 1000M  |
| tikv | 127.0.0.1:20160 | disk     | /dev/sda | capacity    | 4096GB |
+------+-----------------+----------+----------+-------------+--------+
10 rows in set (0.01 sec)

mysql> select * from cluster_systeminfo
+------+-----------------+----------+--------------+--------+
| TYPE | ADDRESS         | MODULE   | KEY          | VALUE  |
+------+-----------------+----------+--------------+--------+
| tikv | 127.0.0.1:20160 | sysctl   | ktrace.state | 0      |
| tikv | 127.0.0.1:20160 | sysctl   | hw.byteorder | 1234   |
| ...                                                       |
+------+-----------------+----------+--------------+--------+
20 rows in set (0.01 sec)

mysql> select * from cluster_load
+------+-----------------+----------+-------------+--------+
| TYPE | ADDRESS         | MODULE   | KEY         | VALUE  |
+------+-----------------+----------+-------------+--------+
| tikv | 127.0.0.1:20160 | network  | rsec/s      | 1000Kb |
| ...                                                      |
+------+-----------------+----------+-------------+--------+
100 rows in set (0.01 sec)
```

#### Cluster Log Table

Today log search requires logging into multiple machines and there is no easy way to merge results by time. This proposal adds a `cluster_log` table for end-to-end log search. Implementation: use the gRPC Diagnostics Service `search_log` API, push down predicates to each node, then merge results by time.

Expected output:

```
mysql> use information_schema;
Database changed

mysql> desc cluster_log;
+---------+-------------+------+------+---------+-------+
| Field   | Type        | Null | Key  | Default | Extra |
+---------+-------------+------+------+---------+-------+
| type    | varchar(16) | YES  |      | NULL    |       |
| address | varchar(32) | YES  |      | NULL    |       |
| time    | varchar(32) | YES  |      | NULL    |       |
| level   | varchar(8)  | YES  |      | NULL    |       |
| message | text        | YES  |      | NULL    |       |
+---------+-------------+------+------+---------+-------+
5 rows in set (0.00 sec)

mysql> select * from cluster_log where content like '%412134239937495042%'; -- query end-to-end logs for TSO 412134239937495042
+------+--------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| TYPE | ADDRESS                | LEVEL | CONTENT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
+------+------------------------+-------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:501.60574ms txnStartTS:412134239937495042 region_id:180 store_addr:10.9.82.29:20160 kv_process_ms:416 scan_total_write:340807 scan_processed_write:340806 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                             |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:698.095216ms txnStartTS:412134239937495042 region_id:88 store_addr:10.9.1.128:20160 kv_process_ms:583 scan_total_write:491123 scan_processed_write:491122 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                             |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.529574387s txnStartTS:412134239937495042 region_id:112 store_addr:10.9.1.128:20160 kv_process_ms:945 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.55722114s txnStartTS:412134239937495042 region_id:100 store_addr:10.9.82.29:20160 kv_process_ms:1000 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.608597018s txnStartTS:412134239937495042 region_id:96 store_addr:10.9.137.171:20160 kv_process_ms:1048 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                          |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.614233631s txnStartTS:412134239937495042 region_id:92 store_addr:10.9.137.171:20160 kv_process_ms:1000 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                          |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.67587146s txnStartTS:412134239937495042 region_id:116 store_addr:10.9.137.171:20160 kv_process_ms:950 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.693188495s txnStartTS:412134239937495042 region_id:108 store_addr:10.9.1.128:20160 kv_process_ms:949 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.693383633s txnStartTS:412134239937495042 region_id:120 store_addr:10.9.1.128:20160 kv_process_ms:951 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.731990066s txnStartTS:412134239937495042 region_id:128 store_addr:10.9.82.29:20160 kv_process_ms:1035 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.744524732s txnStartTS:412134239937495042 region_id:104 store_addr:10.9.137.171:20160 kv_process_ms:1030 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                         |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.786915459s txnStartTS:412134239937495042 region_id:132 store_addr:10.9.82.29:20160 kv_process_ms:1014 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.786978732s txnStartTS:412134239937495042 region_id:124 store_addr:10.9.82.29:20160 kv_process_ms:1002 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tikv | 10.9.82.29:20180       | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=17] [block_read_count=1810] [block_read_byte=114945337] [scan_first_range="Some(start: 74800000000000002B5F728000000000130A96 end: 74800000000000002B5F728000000000196372)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.001s] [peer_id=ipv4:10.9.120.251:47968] [region_id=100] |
| tikv | 10.9.82.29:20180       | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=19] [block_read_count=1793] [block_read_byte=96014381] [scan_first_range="Some(start: 74800000000000002B5F728000000000393526 end: 74800000000000002B5F7280000000003F97A6)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.002s] [peer_id=ipv4:10.9.120.251:47994] [region_id=124]  |
| tikv | 10.9.82.29:20180       | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=17] [block_read_count=1811] [block_read_byte=96620574] [scan_first_range="Some(start: 74800000000000002B5F72800000000045F083 end: 74800000000000002B5F7280000000004C51E4)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.014s] [peer_id=ipv4:10.9.120.251:47998] [region_id=132]  |
| tikv | 10.9.137.171:20180     | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=17] [block_read_count=1779] [block_read_byte=95095959] [scan_first_range="Some(start: 74800000000000002B5F7280000000004C51E4 end: 74800000000000002B5F72800000000052B456)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=2ms] [total_process_time=1.025s] [peer_id=ipv4:10.9.120.251:34926] [region_id=136]  |
| tikv | 10.9.137.171:20180     | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=15] [block_read_count=1793] [block_read_byte=114024055] [scan_first_range="Some(start: 74800000000000002B5F728000000000196372 end: 74800000000000002B5F7280000000001FC628)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=2ms] [total_process_time=1.03s] [peer_id=ipv4:10.9.120.251:34954] [region_id=104]  |
| tikv | 10.9.82.29:20180       | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831930] [internal_delete_skipped_count=0] [block_cache_hit_count=18] [block_read_count=1796] [block_read_byte=96116255] [scan_first_range="Some(start: 74800000000000002B5F7280000000003F97A6 end: 74800000000000002B5F72800000000045F083)"] [scan_ranges=1] [scan_iter_processed=831930] [scan_iter_ops=831932] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.035s] [peer_id=ipv4:10.9.120.251:47996] [region_id=128]  |
| tikv | 10.9.137.171:20180     | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=15] [block_read_count=1792] [block_read_byte=113958562] [scan_first_range="Some(start: 74800000000000002B5F7280000000000CB1BA end: 74800000000000002B5F728000000000130A96)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.048s] [peer_id=ipv4:10.9.120.251:34924] [region_id=96]  |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.841528722s txnStartTS:412134239937495042 region_id:140 store_addr:10.9.137.171:20160 kv_process_ms:991 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                          |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.410650751s txnStartTS:412134239937495042 region_id:144 store_addr:10.9.82.29:20160 kv_process_ms:1000 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.930478221s txnStartTS:412134239937495042 region_id:136 store_addr:10.9.137.171:20160 kv_process_ms:1025 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                         |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.26929792s txnStartTS:412134239937495042 region_id:148 store_addr:10.9.82.29:20160 kv_process_ms:901 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                             |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.116672983s txnStartTS:412134239937495042 region_id:152 store_addr:10.9.82.29:20160 kv_process_ms:828 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.642668083s txnStartTS:412134239937495042 region_id:156 store_addr:10.9.1.128:20160 kv_process_ms:888 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.537375971s txnStartTS:412134239937495042 region_id:168 store_addr:10.9.137.171:20160 kv_process_ms:728 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                          |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.602765417s txnStartTS:412134239937495042 region_id:164 store_addr:10.9.82.29:20160 kv_process_ms:871 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.583965975s txnStartTS:412134239937495042 region_id:172 store_addr:10.9.1.128:20160 kv_process_ms:933 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.712528952s txnStartTS:412134239937495042 region_id:160 store_addr:10.9.1.128:20160 kv_process_ms:959 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.664343044s txnStartTS:412134239937495042 region_id:220 store_addr:10.9.1.128:20160 kv_process_ms:976 scan_total_write:865647 scan_processed_write:865646 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.713342373s txnStartTS:412134239937495042 region_id:176 store_addr:10.9.1.128:20160 kv_process_ms:950 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
+------+--------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
31 rows in set (0.01 sec) 

mysql> select * from cluster_log where type='pd' and content like '%scheduler%'; -- query PD scheduling logs

mysql> select * from cluster_log where type='tidb' and content like '%ddl%'; -- query TiDB DDL logs
```

### Cluster Diagnosis

In current cluster topology, components are distributed and data sources and formats are heterogeneous. Programmatic diagnosis is hard, so humans diagnose issues manually. With the system tables from earlier layers, each TiDB node has a stable cluster Global View. On top of that, a diagnosis framework can detect existing and potential issues by applying diagnosis rules.

**Diagnosis rule definition**: a rule reads data from system tables and detects anomalies to find issues.

Rules fall into three levels:

- Find potential issues: for example, detect low disk capacity by comparing disk capacity vs usage.
- Find existing issues: for example, detect saturated Coprocessor thread pools by inspecting load.
- Provide remediation suggestions: for example, recommend disk replacement if IO latency is high.

This proposal focuses on the diagnosis framework and a subset of rules. More rules should be added over time to form an expert system and reduce operational effort. This document does not cover each specific rule and focuses on the framework.

#### Diagnosis Framework Design

The framework must handle multiple user scenarios, including:

- Users stay on a fixed TiDB version and do not upgrade frequently.
- Users define custom diagnosis rules.
- Rules can be loaded without restarting the cluster.
- The framework can integrate with existing operations systems.
- Users may disable some diagnosis, for example when the system is heterogeneous.
- ...

We need a diagnosis system with hot-loadable rules. Candidate approaches:

- Golang plugin: define rules as plugins and load them into TiDB.
    - Pros: Go is easy to use and has low learning curve.
    - Cons: version management is error-prone; plugins must be built with the same version as TiDB.
- Embedded Lua: load Lua scripts at runtime or startup. Scripts read system tables and apply rules.
    - Pros: simple syntax; easy integration.
    - Cons: introduces another scripting language.
- Shell script: use shell scripts to define rules and run SQL.
    - Pros: easy to write, load, and execute; no TiDB intrusion; only requires a MySQL client to run SQL.
    - Cons: requires a host with a MySQL client installed.

This proposal chooses the third option for now: implement rules as shell scripts. It keeps TiDB untouched and leaves room for better options in the future.
