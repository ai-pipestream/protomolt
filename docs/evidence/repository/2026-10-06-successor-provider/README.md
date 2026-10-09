# Graceful successor publication through a real provider

`JournaledSuccessorPublicationProbe` runs in the production-JAR storage harness
with PostgreSQL and versioned LocalStack S3. It updates an existing writable
document as a scoped caller. Process authority is used only for private handoff,
installation, preparation bootstrap and drain; activation and execution check the
scoped caller's current rights.

The original manager uploads real bytes and reads a retained source, then an
injected schema resolver exception interrupts it before publication. The test
reads the exact verified provider versions back and checks their hashes. It loads
the saved preparation under the live claim and retains that borrowed record through
handoff under a separate byte reservation.

The original manager drains its session, upload and read work. SQL read pins and
locally retained handles must be released before V91. Actual claim and owner leases
expire naturally; the fixture does not update their timestamps. A fresh manager
performs V92 reservation, V93 installation and V94 activation, then publishes the
same command with runtime validation.

Assertions check distinct successor attempt/physical identities and keys, exact
generation-two selection, current revision references, one committed operation
revision, receipt/current mutation-revision equality, and the retained Any
descriptor digest. Exact predecessor attempt/object/cleanup snapshots and direct
provider version reads remain unchanged. Receipt replay performs no provider or
resolver work. Local read handles, SQL pins and payload reservations are released.

Run with:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The final aggregate passed with no failures or skips, including the required
`JOURNALED_SUCCESSOR_PUBLICATION_OK` marker and the existing storage/restart checks.
`storage-runtime.xml.gz` records the complete harness result. Sol reviewed the
probe and the corrected cleanup assertions without finding a remaining blocker.

The saved red run reached successor publication but its test incorrectly assumed
mutation revisions increment by one per document. V8 uses a shared sequence. The
corrected assertions bind the receipt to the current revision and require one
operation commit. No production revision behavior was changed. Sol also required
explicit SQL pin-release assertions and preservation of the provider on failed
drain; both are included in the final probe.

This test proves graceful local handoff, not process death or a delayed remote PUT
after local drain. The first provider handle remains open but drained while its
successor runs. Automatic public-host recovery, pre-owner recovery, abrupt-death
recovery, late-effect reclamation and RustFS performance remain separate work.
