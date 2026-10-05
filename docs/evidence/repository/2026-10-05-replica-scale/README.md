# Archive service replica diagnostic

Measured on `krick` on 2026-10-05, starting from `29a22c2d` with the benchmark
and archive retry-manifest correction in the working tree. `sources.sha256`
records the exact changed runtime and launcher sources used. The complete
`ArchiveServiceIT` passed 25 tests; this benchmark passed one test with zero
failures, errors or skips. The retry regression first failed in both tests.

```sh
PROTOMOLT_REPLICA_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*RepositoryScaleBenchmarkIT' --console=plain
```

One, two and four separate JVMs serve authenticated TCP/gRPC requests against the
same PostgreSQL 18 and LocalStack 3.8 containers. Each has a 256 MiB heap limit.
`fixed_sql` configures runtime pool maxima totaling eight (8/4/2 per child).
`added_sql` configures a maximum of four per child (4/8/16 total). Actual open
connections were not counted; Flyway also opens separate startup connections.
The environment file's connection-budget wording refers to these maxima. These are SQL capacity
comparisons, not fixed aggregate CPU, memory or provider-connection budgets.

Eight workers use independent entries in one archive. Each cycle performs an
expected-version update, current read, retained original-version read and
same-content retry; reads and retries rotate to another replica when available.
Every byte sequence, version increment and retry manifest is checked. A separate
contended window sends eight different updates against one expected version:
exactly one must succeed and seven must return ABORTED. Every replica then reads
the winner and the original retained version. Seed writes and post-contention
verification reads are outside the timed request set.

The raw files contain 2,496 requests and 48 windows, including warmup sample -1.
Excluding warmups leaves 1,872 attempts: 1,728 successful mixed operations and
144 contended attempts (18 successful, 126 ABORTED). `summary.csv` uses total
attempts divided by summed window wall time; latency quantiles use nearest rank.
Contention throughput includes rejected attempts, not only successful writes.
The mixed workload combines operation types; it is not an individual RPC rate.

Mixed throughput ranged from 151 to 167 requests/second. This run did not show a
throughput improvement from additional replicas. It does not establish whether
SQL, provider I/O, shared archive statistics, service execution or the load driver
was limiting. All six configurations passed the correctness checks.

Configurations ran in fixed order, with one warmup and three measured windows;
retained history grows between windows and the shared database accumulates prior
configurations. The host was not reserved exclusively. `environment.txt` records
initial host load. LocalStack timing is not production-storage timing. Memory
samples are Linux post-window RSS and process-lifetime high-water readings, not
per-window peaks. SQL pool acquisition wait, SQL lock-wait duration, provider
call time/connections and service CPU saturation are not measured here.

Before claiming scale-out performance, add those measurements, vary offered load,
interleave repeated topology runs, control aggregate CPU/memory/provider budgets,
and repeat on representative storage. This archive diagnostic does not qualify
the new typed document publication path or replace recovery/lifecycle tests.
