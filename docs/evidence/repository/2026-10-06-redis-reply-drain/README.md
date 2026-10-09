# Drain while a real Redis write acknowledgment is delayed

The fixture uses PostgreSQL, real Redis, the production Redis provider and an
authenticated Netty archive RPC. A test-only RESP2 TCP proxy forwards real requests
and responses, holding only the actual `:1` reply from the conditional-write EVAL.
It neither replaces the provider nor synthesizes success.

While held, SQL records exactly one STAGING upload for the unique fixture account.
An independent direct Redis read returns the exact bytes, proving the write executed
before the acknowledgment reached the client. The host's 100 ms close reports an
archive-RPC drain timeout, retains the pending call and its reserved bytes, and
refuses new requests. The proxy releases the response promptly below Jedis's normal
two-second socket timeout. The original RPC must succeed with the same physical
object UUID, the SQL upload becomes LIVE, and a later close releases reservations.

Sol reviewed the gate, identity checks and teardown. The first run reached the
intended boundary but failed on a mistaken post-publication VERIFIED assertion:
successful publication promotes the object to LIVE. It also exposed the pool's
idle connection reset during shutdown. The fixture now permits that reset only
after explicit successful-call cleanup, at an idle first-byte boundary; partial
frames and forwarding failures during the active test still fail it. Socket errors
caused by closing the proxy's sockets during final teardown are expected; other
recorded failures remain visible. Socket-close errors are
collected while the remaining sockets and workers are cleaned up.

The corrected focused test passed in nine seconds, before the final saved-manifest
UUID comparison was added. Log:
`/tmp/protomolt-redis-reply-gate-qualified.log`.

The final affected regression passed all 21 tests with zero failures, errors or
skips in 24 seconds, including the final saved-manifest UUID assertion:

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*BoundedArchiveRedisReplyIT' --tests '*BoundedArchiveTransportIT' \
  --tests '*BoundedArchiveHostIT' --tests '*RepoBoundedArchiveMainTest' --console=plain
```

Log: `/tmp/protomolt-redis-reply-gate-recovered.log`. The preceding attempt
(`/tmp/protomolt-redis-reply-gate-regression.log`) was aborted after repeated live
thread dumps established a stalled Testcontainers Docker image-list request before
container startup. Independent Docker image-list calls succeeded. Only that test
worker was terminated; Docker and other services were untouched. Its replacement
had a 180-second overall process deadline and completed normally. The aborted run
is not counted as qualification.

This qualifies a short real-provider acknowledgment delay with an embedded shutdown
deadline. It does not establish successful writes after a client socket timeout,
ten-second Redis stalls, forced-kill durability or throughput. The separate
production-process test covers SIGTERM during a SQL commit wait beyond ten seconds.
