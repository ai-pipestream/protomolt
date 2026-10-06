# Graceful successor and late predecessor PUT

Validated on 2026-10-06 against PostgreSQL 18 and LocalStack 3.8 using real
repository adapters. Sol reviewed the final test delta without a blocking finding.

Command:

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessor*IT' --tests '*RepositorySuccessor*IT' --tests '*DocumentSelectedTransferIT' --tests '*DelayedS3PutGatewayIT' --console=plain
```

Result: 48 tests, zero failures, errors or skips; build successful in 47 seconds.
The compressed XML files contain the individual results. This is local validation,
not hosted CI, a merge or a deployment.

`DocumentSuccessorLatePutIT` captures one original signed PUT through a test-owned
HTTP proxy while retaining the SDK endpoint and registered backend identity. The
real SDK times out. The old host drains, both exact V90/V91 records exist, and its
SDK closes before forwarding. After natural lease expiry, a second manager
installs and executes the successor through V92/V93/V94. SQL-driven cleanup records
ABSENT before the old PUT arrives. The real backend then accepts that PUT. Another
cleanup pass removes its version and retains the tombstone. The old object stays
unverified, without a provider-version binding or revision reference; the new
publication's exact bytes and receipt survive.

The command uses admin authority, opaque admission and one CORE part. Hosts are
assembled directly from the journaled manager components. This does not qualify
abrupt process death, automatic managed-service recovery, additional scoped or
typed authorization, arbitrary SDK framing/retries, or horizontal scaling and
latency. RustFS performance qualification remains separate. No production code or
protobuf contract changed in this checkpoint.
