# Public bounded archive embedding

`BoundedArchiveOptions` exposes primitive limits. `buildBoundedArchive` creates the
shared budget and uses provider discovery; the public bounded Netty method requires
an operator token. The default factory and standalone entry point are unchanged.
Invalid limits fail before opening database or provider resources.

An external-package test uses public composition methods with real PostgreSQL and
Redis. It provisions a drive and archive, writes locally, retries and reads history
through authenticated Netty, then reads retained bytes after host restart. Options
and existing configuration tests cover invalid sizes, concurrency, budget and storage
qualification. Sol reviewed the API and tests with no blocker.

```sh
./gradlew :protomolt-repo-service:test \
 --tests '*BoundedArchiveEmbeddingIT' \
 --tests '*BoundedArchiveOptionsTest' \
 --tests '*BoundedArchiveTransportIT' \
 --tests '*BoundedArchiveHostIT' \
 --tests '*RepoServiceConfigTest' --console=plain
```

Result: 31 tests, zero failures/errors/skips, 14 seconds. Local log:
`/tmp/protomolt-bounded-embedding.log`. `git diff --check` passed.

The minimum budget covers one maximum transport write, not maximum concurrency at
maximum size. Read memory, decoded heap and prior network buffers are excluded.
Redis remains running during host restart; no new crash-durability evidence is
provided here. Standalone environment activation, HTTP admission and full repository
parity remain open. This checkpoint does not establish publication or deployment.
