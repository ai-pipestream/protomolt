package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.repo.container.archive.*;
import com.google.protobuf.UnknownFieldSet;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ArchiveMutationLedgerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final ArchiveMutationLedger.LogicalOutcome NOOP =
            new ArchiveMutationLedger.LogicalOutcome(false, 0, 0, Set.of());

    @Test void concurrentRetriesExecuteOnceAndAreScopedToThePrincipal() throws Exception {
        try (var database = database(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(database.entityManagerFactory());
            var mutations = new ArchiveMutationLedger(tx);
            var command = command(address());
            var calls = new AtomicInteger();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var first = executor.submit(() -> mutations.admit("caller", command, 0, (em, entry) -> {
                calls.incrementAndGet();
                entered.countDown();
                await(release);
                return NOOP;
            }));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            var second = executor.submit(() -> mutations.admit("caller", command, 0, (em, entry) -> {
                calls.incrementAndGet();
                return NOOP;
            }));
            release.countDown();
            var receipt = first.get(10, TimeUnit.SECONDS);
            assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(receipt);
            assertThat(calls.get()).isEqualTo(1);
            assertThat(receipt.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_COMPLETED);
            assertThat(mutations.find("caller", command.address().getAccountId(), command.operationId())).contains(receipt);
            assertThat(mutations.find("other", command.address().getAccountId(), command.operationId())).isEmpty();
            mutations.admit("other", command, 0, (em, entry) -> { calls.incrementAndGet(); return NOOP; });
            assertThat(calls.get()).isEqualTo(2);
            var changed = new ArchiveMutationCommand(command.request().toBuilder()
                    .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(address())).build());
            assertThatThrownBy(() -> mutations.admit("caller", changed, 0, (em, entry) -> {
                throw new AssertionError("Conflicting command must not execute");
            })).isInstanceOf(ArchiveMutationLedger.OperationConflictException.class);
        }
    }

    @Test void recordedNoopDoesNotResampleARecreatedEntryAndRowsAreImmutable() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var mutations = new ArchiveMutationLedger(tx);
            var command = command(address());
            var receipt = mutations.admit("caller", command, 0, (em, entry) -> NOOP);
            var saved = save(tx, command.address());
            assertThat(mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                throw new AssertionError("Replay must not touch the recreated entry");
            })).isEqualTo(receipt);
            assertThat(new ArchiveLedger(tx).findEntry(saved.entryUuid)).isPresent();
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                em.createNativeQuery("DELETE FROM archive_mutations WHERE operation_id=:id")
                        .setParameter("id", command.operationId()).executeUpdate();
            })).hasStackTraceContaining("admission is immutable");
        }
    }

    @Test void callbackFailureRollsBackBothMutationAndReceiptAndAllowsRetry() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var mutations = new ArchiveMutationLedger(tx);
            var command = command(address());
            var saved = save(tx, command.address());
            var failure = new IllegalStateException("Injected failure after logical change");
            assertThatThrownBy(() -> mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                em.remove(entry);
                em.flush();
                throw failure;
            })).isSameAs(failure);
            assertThat(new ArchiveLedger(tx).findEntry(saved.entryUuid)).isPresent();
            assertThat(mutations.find("caller", command.address().getAccountId(), command.operationId())).isEmpty();
            var receipt = mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                em.remove(entry);
                return new ArchiveMutationLedger.LogicalOutcome(true, 0, 0, Set.of());
            });
            assertThat(receipt.getEntryDeleted()).isTrue();
            assertThat(new ArchiveLedger(tx).findEntry(saved.entryUuid)).isEmpty();
        }
    }

    @Test void staleRevisionAndInvalidOutcomeCannotCommitLogicalChanges() {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var mutations = new ArchiveMutationLedger(tx);
            var command = command(address());
            var saved = save(tx, command.address());
            assertThatThrownBy(() -> mutations.admit("caller", command, 0, (em, entry) -> {
                throw new AssertionError("Stale revision must not execute");
            })).isInstanceOf(ArchiveLedger.VersionConflictException.class);
            assertThatThrownBy(() -> mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                em.remove(entry);
                return new ArchiveMutationLedger.LogicalOutcome(true, -1, 0, Set.of());
            })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("outcome");
            assertThat(new ArchiveLedger(tx).findEntry(saved.entryUuid)).isPresent();
            assertThat(mutations.find("caller", command.address().getAccountId(), command.operationId())).isEmpty();
        }
    }

    @Test void commandValidationRejectsInvalidNestedAndUnknownFields() {
        var command = command(address());
        var unknown = UnknownFieldSet.newBuilder().addField(12345,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        assertThatThrownBy(() -> new ArchiveMutationCommand(command.request().toBuilder()
                .setDeleteEntry(command.request().getDeleteEntry().toBuilder().setUnknownFields(unknown)).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown");
        assertThatThrownBy(() -> new ArchiveMutationCommand(command.request().toBuilder().clearMutation().build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ArchiveMutationCommand(command.request().toBuilder()
                .setPruneVersions(PruneVersionsRequest.newBuilder().setAddress(command.address()).setKeepLatest(0)).build()))
                .isInstanceOf(IllegalArgumentException.class);
        var otherId = new ArchiveMutationCommand(command.request().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        assertThat(otherId.canonical()).isEqualTo(command.canonical());
        assertThat(otherId.sha256()).isEqualTo(command.sha256());
    }

    @Test void cancellationDuringLogicalChangeRollsBackAdmission() throws Exception {
        try (var database = database(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(database.entityManagerFactory());
            var mutations = new ArchiveMutationLedger(tx);
            var command = command(address());
            var saved = save(tx, command.address());
            executor.submit(() -> {
                try {
                    assertThatThrownBy(() -> mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                        em.remove(entry);
                        Thread.currentThread().interrupt();
                        return new ArchiveMutationLedger.LogicalOutcome(true, 0, 0, Set.of());
                    })).isInstanceOf(java.util.concurrent.CancellationException.class);
                } finally { Thread.interrupted(); }
            }).get(10, TimeUnit.SECONDS);
            assertThat(new ArchiveLedger(tx).findEntry(saved.entryUuid)).isPresent();
            assertThat(mutations.find("caller", command.address().getAccountId(), command.operationId())).isEmpty();
        }
    }

    @Test void targetsMustBePublishedInScopeAndUnpinnedAndCommitWithTheReceipt() throws Exception {
        try (var database = database()) {
            var tx = new Tx(database.entityManagerFactory());
            var mutations = new ArchiveMutationLedger(tx);
            var address = address();
            var saved = save(tx, address);
            var admission = stage(tx, address);
            UUID object = admission.upload().objectId();
            var command = command(address);
            var outcome = new ArchiveMutationLedger.LogicalOutcome(true, 1, 0, Set.of(object));
            assertThatThrownBy(() -> mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                em.remove(entry); return outcome;
            })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not published");
            var abandoned = stage(tx, address);
            UUID abandonedId = abandoned.upload().objectId();
            tx.inTransaction(em -> {
                em.createNativeQuery("UPDATE archive_object_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE object_id=:id")
                        .setParameter("id", abandonedId).executeUpdate();
            });
            var cleanup = new ArchiveCleanupLedger(tx);
            var claim = cleanup.claim(abandonedId, Instant.now().plusSeconds(60)).orElseThrow();
            assertThat(cleanup.succeeded(abandonedId, claim.token())).isTrue();
            assertThatThrownBy(() -> mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                em.remove(entry);
                return new ArchiveMutationLedger.LogicalOutcome(true, 1, 0, Set.of(abandonedId));
            })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not referenced before");
            var foreign = stage(tx, address());
            assertThatThrownBy(() -> mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                em.remove(entry);
                return new ArchiveMutationLedger.LogicalOutcome(true, 1, 0, Set.of(foreign.upload().objectId()));
            })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("scope");
            var uploads = new ArchiveUploadLedger(tx);
            uploads.verify(object, admission.upload().leaseToken(), 7, "a".repeat(64), null, null);
            assertThatThrownBy(() -> mutations.admit("caller", command, saved.mutationRevision, (em, entry) -> {
                em.remove(entry); return outcome;
            })).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not published");
            // Publish through the actual archive ledger, not a fabricated LIVE row.
            var manifest = VersionManifest.newBuilder().setAddress(address).setVersion(1)
                    .setRootChecksum("a".repeat(64)).setTotalBytes(7)
                    .addRenditions(RenditionManifestEntry.newBuilder()
                            .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                            .setState(RenditionState.RENDITION_STATE_PRESENT).setSizeBytes(7)
                            .setSha256("a".repeat(64)).setObjectKey(admission.binding().location().objectKey())
                            .setStorageObjectId(object.toString())).build();
            var version = new ArchiveVersionRecord();
            version.entryUuid = saved.entryUuid;
            version.version = 1;
            version.manifest = com.google.protobuf.util.JsonFormat.printer().print(manifest);
            version.rootChecksum = manifest.getRootChecksum();
            version.totalBytes = 7;
            version.createdAt = Instant.now();
            var archive = new ArchiveLedger(tx);
            archive.commitSave(saved, 1, version, 0, ArchiveLedger.StatsDelta.none(),
                    java.util.Map.of(object, admission.upload().leaseToken()));
            long revision = archive.findEntry(saved.entryUuid).orElseThrow().mutationRevision;
            assertThatThrownBy(() -> mutations.admit("caller", command, revision, (em, entry) -> outcome))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retained references");
            assertThat(mutations.find("caller", address.getAccountId(), command.operationId())).isEmpty();
            var receipt = mutations.admit("caller", command, revision, (em, entry) -> {
                em.remove(entry); return outcome;
            });
            assertThat(receipt.getObjectsTargeted()).isEqualTo(1);
            assertThat(receipt.getObjectsPending()).isEqualTo(1);
            assertThat(receipt.getObjectsConfirmedAbsent()).isZero();
            assertThat(receipt.getState()).isEqualTo(ArchiveMutationState.ARCHIVE_MUTATION_STATE_ADMITTED);
            assertThat(archive.findEntry(saved.entryUuid)).isEmpty();
            assertThat(mutations.find("caller", address.getAccountId(), command.operationId())).contains(receipt);
            long targets = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM archive_mutation_targets WHERE operation_id=:id")
                    .setParameter("id", command.operationId()).getSingleResult()).longValue());
            assertThat(targets).isEqualTo(1);
            save(tx, address);
            tx.inTransaction(em -> { em.persist(version); });
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                em.createNativeQuery("INSERT INTO archive_version_object_refs(entry_uuid,version,object_id) VALUES (:entry,1,:id)")
                        .setParameter("entry", saved.entryUuid).setParameter("id", object).executeUpdate();
            })).hasStackTraceContaining("admitted mutation target");
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                em.createNativeQuery("DELETE FROM archive_mutation_targets WHERE operation_id=:id")
                        .setParameter("id", command.operationId()).executeUpdate();
            })).hasStackTraceContaining("admission is immutable");
        }
    }

    private static ArchiveUploadLedger.Admission stage(Tx tx, EntryAddress address) {
        String generation = "mutation-" + UUID.randomUUID();
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of("https://storage.example", "us-east-1", true), generation));
        return new ArchiveUploadLedger(tx).begin(new ArchiveObjectLedger.Location(ArchiveIds.entryUuid(address),
                address.getAccountId(), address.getArchive(), generation, "bucket", UUID.randomUUID().toString()),
                7, "text/plain", java.time.Duration.ofMinutes(1));
    }

    private static EntryAddress address() {
        return EntryAddress.newBuilder().setAccountId("account").setArchive("records")
                .setEntryId(UUID.randomUUID().toString()).build();
    }

    private static ArchiveMutationCommand command(EntryAddress address) {
        return new ArchiveMutationCommand(ArchiveMutationRequest.newBuilder().setOperationId(UUID.randomUUID().toString())
                .setDeleteEntry(DeleteEntryRequest.newBuilder().setAddress(address)).build());
    }

    private static ArchiveEntryRecord save(Tx tx, EntryAddress address) {
        var entry = new ArchiveEntryRecord();
        entry.entryUuid = ArchiveIds.entryUuid(address);
        entry.entryId = address.getEntryId();
        entry.accountId = address.getAccountId();
        entry.archive = address.getArchive();
        entry.currentVersion = 1;
        entry.createdAt = Instant.now();
        entry.updatedAt = entry.createdAt;
        return tx.inTransaction(em -> { em.persist(entry); em.flush(); em.refresh(entry); return entry; });
    }

    private static LedgerDatabase database() {
        return new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("Timed out waiting for concurrent request"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); }
    }
}
