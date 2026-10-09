# Terminated-host reader discovery

Code checkpoint: `c9b4b6bed` on `agent/host-reader-composition`.

```sh
./gradlew :protomolt-repo-container:test --tests '*ReaderHostDiscoveryIT' --tests '*ReaderExternalQuiescenceIT' --max-workers=2 --console=plain
```

6 tests passed with no failures, errors or skips; 13-second build. The discovery
case uses real PostgreSQL and verifies the exit of a managed child process.
It checks exact receipt binding, mismatched execution and receipt rejection even
on empty pages, page limits, PostgreSQL UUID ordering, all lifecycle states,
local/foreign exclusion, external quiescence, repeat traversal, and a
failed-registration tombstone inserted behind an exhausted cursor. Existing
external-quiescence cases also passed.

Sol reviewed the discovery primitive and found no blocker. The suggested
tombstone case was incorporated before this final run. These checks cover
scheduling and SQL lifecycle. Automatic recovery and remote termination
verification remain separate work.
