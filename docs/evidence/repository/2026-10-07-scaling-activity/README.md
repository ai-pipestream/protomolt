# Backend activity and worker CPU diagnostic

The fixed-total 384 MB budget workload passed in 3 minutes 34 seconds:
16 clients, 64 KiB payloads, 32 iterations per client, journaled publication,
tracing disabled, 12 windows across 1/2/4 JVMs. Production behavior is unchanged.
Sol reviewed the sampler and interpretation limits.

The sampler captures client-backend states and wait events, plus cumulative
worker CPU counters and RSS. In t00, 615 observations reported WALWrite and 223
reported WalSync. The corresponding counts were 985/203 in t05 and 825/212 in t10.
These observations do not measure wait duration. Idle/ClientRead usually indicates
an idle pool connection. No wait event does not prove CPU saturation.
The lock CSV covers client backends only.

Worker CPU deltas between the first and last samples sum to 22.42 seconds in t00
(1 JVM), 31.67 in t05 (2 JVMs, 8 SQL connections each), and 49.79 in t10 (4 JVMs,
8 connections each). These intervals differ from the primary operation timers.
RSS sums include shared mappings repeatedly. Blank measurements mean unavailable.

WAL/write pressure and pool admission warrant investigation. These results do not
quantify WAL latency, prove disk saturation or justify weaker durability.
Sampling adds overhead. The host remains shared, and aggregate heap and worker
resources vary by topology. The raw archive retains all samples. The workload
command matches the preceding fixed-budget checkpoint.
