# Claim loss and forced writer exit

Local PostgreSQL/LocalStack production-JAR qualification on 2026-10-05, based on
`3d5f736c` plus this checkpoint. Sol reviewed the changes without a blocker.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The gate passed, including two additions:

- Restore under a live execution claim, wait for its real database-clock expiry,
  and verify the operation owner remains live. Resume fails with the exact claim
  fence before assessment-reader callbacks or read pins. Transfer the expired
  claim to a new epoch/token and repeat: the old handle still fails and the
  operation stays pending. All reservations drain on close.
- Start a separate writer JVM with only a public operation UUID and normal test
  database/provider/runtime configuration. It stages real provider data, commits
  the assessment, loses the JDBC acknowledgment, then calls `Runtime.halt(86)`
  before cleanup. The harness requires that exit code and reaps it before starting
  a fresh reader JVM. The reader obtains the original live claim, owner and command
  from shared SQL, without a command/token handoff file, and reconciles the sealed
  assessment through the production restoration handle. It verifies the durable
  rejection's original assessment, manifest and retention deadline, exact retry,
  no additional assessment reads on retry, and resource cleanup.

The new crash target does not use the handoff files from the older independent
restart fixtures; the harness removes that environment variable for both new
processes. Test-only privileged SQL reads recover private capabilities after
confirmed writer death. This is not a production bootstrap API, automatic
failover, successor-owner execution, or an in-flight provider-write race proof.
LocalStack results establish correctness here, not RustFS performance.
