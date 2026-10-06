# Repository environment parsing

Repository service environment parsing now refuses malformed limits, integer
overflow, invalid port ranges, negative configured bounds, nonpositive intervals
and pool sizes, and unknown boolean spellings. An omitted setting retains its
documented default. The supported boolean pairs remain true/false, 1/0, yes/no
and on/off. HTTP off and explicit zero settings keep their documented meanings.

Present blank defaulted service text settings, including the storage selector and
Redis URI, are refused. Present blank JDBC URL and username are also refused instead
of selecting the local development database. An explicitly empty database password
is preserved. Direct Java constructors retain their existing default conventions.
Environment files using blank values to request defaults must omit those settings.

Errors name the numeric, flag or storage-selector setting without echoing supplied
values or attaching a parse exception containing them. Both config records expose
an environment-map overload so hosts and tests can use an explicit snapshot.

```sh
./gradlew :protomolt-repo-container:test --tests '*LedgerConfigTest' \
 :protomolt-repo-service:test --tests '*RepoServiceConfigTest' \
 --tests '*ManagedStoragePolicyTest' --tests '*BoundedArchiveOptionsTest' \
 --tests '*BoundedArchiveHostIT' --tests '*BoundedArchiveEmbeddingIT' --console=plain
```

Result: 108 tests, zero failures/errors/skips, 13 seconds. The host and embedding
cases use real PostgreSQL and Redis. Local log:
`/tmp/protomolt-strict-environment-qualified.log`. `git diff --check` passed.

This is a setup prerequisite, not standalone archive activation. The reviewed
[next launch slice](../../../design/repository-bounded-ingress.md#next-standalone-slice-explicit-archive-bootstrap)
requires explicit local account/drive bootstrap before starting the archive-only
listener. Process launch/restart, bootstrap failure and shutdown tests remain open.
