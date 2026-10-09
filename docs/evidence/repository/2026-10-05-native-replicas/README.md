# Native repository process qualification

`nativeReplicaTest` runs independent standard JVMs with the production JAR graph
against one PostgreSQL 18 and one pinned RustFS container. It is a small
correctness workload, separate from the assessment failure/expiry suite.

One seed JVM creates the namespace, drive, backend and policy. One, two and four
writer processes then initialize and wait at a parent-controlled ready/go barrier.
Every writer submits two valid typed creates and one invalid create, for exactly
14 successful publications and seven retained rejections. Every write uses real
codec output, provider transfers and ProtoMolt validation. Same-runtime retries
must return the exact original result or rejection without another schema lookup.

After each topology exits, a fresh JVM reads all published historical documents
using retained definitions. It checks content equality, metadata ownership,
mutation revision and command digest. It independently observes every original
terminal result/rejection and verifies that invalid creates have neither normal
documents nor published revisions. Runtime and reader shutdown must drain their
pins and memory budgets. Failure cleanup signals all child processes before
attempting every bounded reap and reports accumulated cleanup failures.

This fixture uses a trusted caller with process authority. It does not establish
scoped authentication, concurrent mixed read/write traffic, execution retry on a
second live replica, or a public typed publication endpoint. Existing provider
version tests remain separate; this workload checks archived content and command
identity without overwriting provider keys during its read phase.

Operation durations written inside the disposable fixture are diagnostics, not
qualified performance samples. They include no warmup/window protocol or SQL,
provider, lock and RSS telemetry. Do not derive scaling or capacity from this run.
Competing same-revision writers, mixed traffic, fixed versus added SQL budgets
and interleaved measured windows remain the next performance acceptance work.

```sh
./gradlew :protomolt-repo-container:nativeReplicaTest --console=plain
```

The source hashes and compressed JUnit result preserve this local checkpoint.
They are not hosted CI, merge or deployment evidence.

The final local run passed with no failures, errors or skips after Sol review and
readiness/cleanup refinements. Cleanup preserves an original failure and attaches
its own aggregated failures as suppressed, rather than hiding the cause. The
initial attempt failed to compile a probe because an import was missing; that
attempt is not counted as a successful provider run.
