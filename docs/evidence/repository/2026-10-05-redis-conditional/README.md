# Redis conditional byte operations

Base: `2cf4ffe798b970cedc32ec9a6dbb9ec546f55948`. October 5, 2026 local,
October 6 UTC. Standalone `redis:7-alpine` test containers.

The initial four real-Redis tests failed because conditional operations inherited
the unsupported SPI defaults (`red.xml`, `red.log.gz`). The implementation adds
atomic Lua comparison and replacement, plus an authoritative bounded read, using
the unchanged SPI limit of 9 MiB. Configured object limits still constrain writes.

Validation command:

```sh
./gradlew :protomolt-repo-blob-redis:check :protomolt-repo-blob-cache:check --console=plain
```

Final result: 45 Redis tests and 2 cache tests passed, none skipped, with runtime
dependency gates passing. Build completed in 8 seconds; an intermediate test
compilation error for the discovered handle's checked close exception was fixed
before this run. `TEST-*.xml`, `green.log.gz` and `source.sha256` retain the evidence.

The conditional cases exercise absent and matching conditions, two independent
client pools racing, exact byte boundaries, missing/malformed objects, TTL and
metadata preservation on conflict, provider capability negotiation, and content
ETag ABA. The acknowledgment case deliberately throws after the real adapter
returns; it is caller-acknowledgment uncertainty, not a dropped Redis network
response. Retry conflicts remain conflicts; a fresh adapter reads actual bytes.

Sol reviewed the implementation and requested the authoritative-read capability
declaration, now covered by a real discovered-provider round trip. No archival
activation is included. Ordinary PUT/COPY still replace objects; ETags describe
body content rather than mutation epochs. Syntactically valid but corrupted stored
ETags/bytes require higher-level integrity checking. This does not qualify an
immutable key policy, late-write cleanup, failover, or managed archival durability.
