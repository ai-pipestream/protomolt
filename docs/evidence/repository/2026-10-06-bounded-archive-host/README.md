# Internal bounded archive host

The internal profile uses Redis with PostgreSQL, managed archive recovery and a
shared unary-put budget. Redis requires create-only writes, zero TTL and a finite
object limit within 9 MiB. Identity, handle and reclaimer use the same selected
configuration. The default managed profile still rejects Redis.

Host tests verify saved bytes, retry identity, historical bytes after host restart,
size and capacity rejection, absence of S3 selection, invalid configuration, and
disabled document/HTTP/gRPC surfaces. A delayed real Redis PUT demonstrates that
shutdown timeout preserves the provider and reservation. Further writes fail;
after the accepted write completes, shutdown closes the provider once.

```sh
./gradlew :protomolt-repo-service:test \
 --tests '*BoundedArchiveHostIT' \
 --tests '*RedisServiceCompositionIT' \
 --tests '*RedisArchiveLifecycleIT' \
 --tests '*ManagedArchiveHostIT' --console=plain
```

Result: 18 tests, zero failures/errors/skips, 26 seconds. Local log:
`/tmp/protomolt-bounded-host-qualified.log`. Sol found no blocker in the identity,
access or lifetime review. `git diff --check` passed. An initial test compilation
failed on a response accessor and was corrected. This is local verification only.

Public configuration, aggregate allocation before transport decoding, early stream
rejection, full repository parity and deployment durability remain open. Redis
was running throughout the host restart test; that test proves no crash durability.
The put budget excludes read memory and parsed-object overhead. Startup fault and
backend identity-conflict tests remain open before public activation.
