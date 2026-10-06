# Archive read construction admission

The optional `ArchiveGetAdmission` library gate bounds selected GetEntry response
construction before provider reads. It includes the full metadata envelope, payload
and protobuf framing. Its shared reservation lasts through verification and
construction; returned protobuf retention is the caller's responsibility.

Validation:

```sh
./gradlew :protomolt-repo-engine:test :protomolt-repo-service:test \
  --tests '*ArchiveServiceIT' --tests '*RedisArchiveLifecycleIT' --console=plain
```

126 tests passed, none skipped, in 33 seconds. Unit cases exercise exact framing at
payload varint boundaries, aggregate/metadata limits, invalid stored sizes, shared
capacity and close/drain. Real PostgreSQL/Redis fixtures test both local calls and
in-process gRPC: two individually allowed renditions exceed the aggregate limit
without any provider GET; a selected historical subset succeeds. Delaying the
acknowledgment of an actual Redis GET retains the read pin and budget through gate
closure until successful return or injected byte corruption fails checksum checks.

Sol identified a legacy fallback bypass during review. Gated reads now refuse any
selected rendition without a storage identity before provider I/O. A real legacy
write/read control proves old constructors still work; the same entry under the
gate refuses with FAILED_PRECONDITION and performs zero raw or bounded GETs. Sol's
final review found no remaining blocker for this library slice.

This does not activate the gate in the managed host. Header-time transport response
reservations, host options and lifecycle wiring, transport cancellation acceptance,
metadata/list bounds and caller-retained results remain separate work. Existing
process-authority requirements are unchanged; this is not scoped-user authorization
qualification, deployment, or hosted CI evidence.
