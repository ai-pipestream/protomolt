# Redis create-only byte policy

Base `526d2c266d1feaea1ab36f00d066b15843f2c365`. October 5, 2026 local,
October 6 UTC. Real standalone `redis:7-alpine` containers.

The red baseline added only the policy enum/configuration field, without behavior.
Three tests failed on replacement, layout aliasing and accepted expiry (`red.xml`).
The implementation now isolates physical keys and backend identity, requires zero
TTL, and refuses replacement via byte/stream PUT, conditional replacement or COPY.
The full conditional-write capability is omitted for create-only handles because
matching writes are unsupported. Default v2 behavior is unchanged.

```sh
./gradlew :protomolt-repo-blob-redis:check :protomolt-repo-blob-cache:check --console=plain
```

Final result: 53 Redis tests and 2 cache tests passed with no skips, failures or
errors. Runtime dependency checks passed. The build completed in 10 seconds;
`TEST-*.xml`, `green.log.gz` and `source.sha256` retain results and source hashes.

Tests include copy-versus-put races from separate pools, self-copy, malformed and
oversized copy objects, stream creation/collision, layout coexistence, provider
identity/capabilities, and caller acknowledgment failure after actual Redis success.
A fresh adapter reconciles the stored bytes. The fault is not a dropped network
response. Another case explicitly proves that reclamation permits a subsequent
late write; no tombstone, fencing or archival lifecycle guarantee is inferred.

Sol reviewed design and code and requested additional copy/stream cases, included
here. The first implementation run caught an invalid backend schema identifier;
the implementation now uses the existing schema grammar, `redis/v3`, with the
disjoint `v3-create-only` physical prefix. No public protobuf or service activation
changes are included.
