# Fixed payload budget in the replica benchmark

At production commit 253afba1f, the journaled 16-client, 64 KiB workload exhausted
its 128,000,000-byte per-worker payload budget during warmup. The stack identifies
DocumentPublicationRegistration.admitInitial. That phase reserves the maximum
preparation, modes and command encoding allowances: 16 + 1 + 1 MiB per active
call. This is admission refusal, not a completed throughput measurement. The
failed output and raw files are retained here.

The test-only nativeBenchmarkBudgetBytes setting now accepts an explicit total
budget, divided equally across the one/two/four-worker configurations. Zero keeps
the previous 128,000,000 bytes per worker. Child configuration reports and parent
assertions check the actual applied capacity. No production budget or reservation
rule changed.

The follow-up uses 384,000,000 total bytes, 16 clients, 16 total read slots, 64 total
read handles, 65,536 payload characters and 32 iterations per client. JDBC tracing
is disabled. Each measured window includes 256 reads, 192 successful publications,
64 expected admission rejections and 256 exact receipt replays. Its inclusive
window time includes replay; operation latency excludes the following replay.

The mirrored twelve-window order compares eight total SQL connections with eight
connections per worker. Each worker still has a 512 MiB heap limit and independent
upload/maintenance/session resources. All workers share unisolated PostgreSQL and
RustFS containers, whose data and caches change during the run. This is a fixed
closed-loop workload comparison, not open-loop latency or a maximum-capacity test.
Sol reviewed these controls and limitations. All twelve windows passed in
3 minutes 38 seconds, including exact outcomes, replay and resource cleanup.

## Results

With eight SQL connections per replica, one worker completed 73.38–75.53
iterations/second, two completed 87.03–89.32, and four completed 77.86–80.20.
With eight SQL connections in total, two workers completed 65.44–68.35 and four
completed 62.21–62.78. These rates count the primary mixed iterations; successful
writes and rejected writes additionally perform receipt replay inside the window.

Successful-publication p95 latency was 332.71–359.15 ms for one worker,
283.59–297.39 ms for two workers with eight connections each, and 348.30–348.61 ms
for four workers with eight each. Restricting four workers to two connections each
raised publication p95 to 393.04–427.86 ms.

This workload benefits from two workers with a larger total pool but does not
improve further at four. Pool allocation matters; the results do not identify a
single causal bottleneck. Repeated independent runs and CPU/I/O/SQL wait analysis
remain necessary before a deployment-capacity claim. The raw archive includes
database statements, wait samples, worker RSS and operation timings.
window-summary.csv contains nearest-rank p50/p95 values from each window's
measured primary operations, excluding their subsequent receipt replays.
