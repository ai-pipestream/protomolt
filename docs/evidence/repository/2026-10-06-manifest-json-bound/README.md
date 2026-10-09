# Bounded archive manifest JSON reads

Bounded library and transport reads now reserve one call-wide manifest JSON
allowance before SQL. It remains owned through parsing, provider reads and response
assembly, including cancellation and shutdown. The configured cap is independent
of response size; public options and the launcher default it to the request cap.

Exact version reads return size and text from one SQL statement, suppressing JSON
above the remaining allowance. Version lists cap the entire selected page in SQL;
entry lists decrement the same call-wide allowance across exact current-version
queries. A missing row remains distinct from a present oversized manifest. These
are additive Java APIs; protobuf names, fields, tags and Any URLs are unchanged.

Local validation: 50 tests passed with none skipped in 29 seconds:

```sh
./gradlew :protomolt-repo-engine:test --tests '*ArchiveGetAdmissionTest' \
  :protomolt-repo-service:test --tests '*RedisArchiveLifecycleIT' \
  --tests '*BoundedArchiveTransportIT' --tests '*BoundedArchiveOptionsTest' \
  --tests '*RepoBoundedArchiveMainTest' --tests '*BoundedArchiveHostIT' \
  --tests '*BoundedArchiveEmbeddingIT' --console=plain
```

After adding explicit continuation-page assertions, the 16 RedisArchiveLifecycleIT
cases passed again in 10 seconds. Production code was unchanged for that rerun.
Attached XML records that last lifecycle run and the four admission unit tests.

Real PostgreSQL/Redis cases cover exact scalar byte boundaries, page aggregate
refusal, cumulative entry-list refusal, historical bytes, missing versions, newest
first ordering and continuation to the older version. Authenticated Netty tests
refuse oversized manifests before provider GET, release their budgets, and still
permit a manifest-free listing. Existing held-provider cancellation, close/drain,
embedding and startup regressions pass. Sol reviewed the implementation without
a blocker.

The allowance bounds serialized UTF-8 JSON transferred through JDBC, not measured
Java heap, PostgreSQL materialization, or unrelated entry/archive metadata. A list
of entries may parse earlier manifests before refusing a later one; only version
pages suppress all over-limit JSON together. The managed credential still has
process authority. These results do not establish scoped-user authorization, hosted
CI, merge, deployment, or completion of the wider repository goal.
