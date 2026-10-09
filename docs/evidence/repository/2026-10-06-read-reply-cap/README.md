# Archive read-only response limits

The bounded archive Netty host now reserves and enforces its configured response
limit for all seven read-only ArchiveService RPCs. The existing GetEntry engine gate
still checks aggregate construction before provider I/O. Metadata, manifest and list
responses are checked at send time, after construction. Their SQL allocation,
parsing, local return values and decoded heap remain outside this qualification.
Mutation acknowledgments are not capped at send time: hiding an already committed
result requires a separate precommit response-bound design.

Local validation: 29 tests passed, none skipped, in 23 seconds:

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*BoundedArchiveTransportIT' \
  --tests '*UnaryRequestAdmissionTest' \
  --tests '*BoundedArchiveOptionsTest' --console=plain
```

The new PostgreSQL/Redis/authenticated Netty case creates a real entry and verifies
that oversized manifest and entry-list responses return RESOURCE_EXHAUSTED. Each
refusal releases its budget; a subsequent empty archive-list response succeeds.
No provider GET occurs and the original single provider write remains unchanged.
The existing transport lifecycle tests cover cancellation, held provider completion,
shutdown, retry and successful local/remote reads. The synthetic protobuf echo
service tests the final message boundary independently; it is not a provider mock.

An initial test compile used the wire method name for the Java SPI; corrected to
ArchiveRepository.getManifest before execution. Both subsequent focused runs passed.
The final run includes the defensive refusal of negative protobuf serialized sizes.
Sol reviewed the read-only method inventory and scope without a blocker. These are
local results, not hosted CI, merge or deployment evidence.
