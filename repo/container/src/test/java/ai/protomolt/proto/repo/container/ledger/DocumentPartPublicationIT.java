package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real SQL plans with synthetic measured-byte identities; no provider I/O is claimed. */
@Testcontainers
class DocumentPartPublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static final String SHA = "ab".repeat(32);
    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    enum Mismatch { NODE, ACCOUNT, REVISION, SOURCES, ADDRESS, VERSION, OMITTED, EXTRA, ORDER,
        KEY, HASH, SIZE, SUBKEY, CORE_VERSION, CORE_ETAG, TOTAL_SIZE, ROOT, EMPTY_WITH_KEY, DUPLICATE_TOMBSTONE, UNKNOWN_STATE,
        PENDING_STATUS, PENDING_PURGE, UNKNOWN_FIELD }

    @Test void exactOrderedCandidatePassesWithoutPublishingAnything() {
        var fixture = fixture(true);
        validate(fixture, 7L, fixture.plan.sources());
        assertThat(new DocumentLedger(tx).findByNodeId(fixture.row.nodeId)).isEmpty();
        assertThat(new DocumentPartAttemptLedger(tx).find(fixture.attempt.id()).orElseThrow().state()).isEqualTo("VERIFIED");
    }

    @Test void unverifiedAttemptIsRefused() {
        var fixture = fixture(false);
        assertThatThrownBy(() -> validate(fixture, 7L, fixture.plan.sources()))
                .isInstanceOf(DocumentPartAttemptLedger.FenceException.class).hasMessageContaining("not fully verified");
    }

    @ParameterizedTest @EnumSource(Mismatch.class)
    void publicationRefusesCandidateDrift(Mismatch mismatch) {
        var f = fixture(true);
        var row = f.row;
        var manifest = row.readManifest().toBuilder();
        Long revision = 7L;
        var sources = f.plan.sources();
        switch (mismatch) {
            case NODE -> row.nodeId = UUID.randomUUID();
            case ACCOUNT -> row.accountId = "other";
            case REVISION -> revision = 8L;
            case SOURCES -> sources = Map.of();
            case ADDRESS -> manifest.getAddressBuilder().setDocId("another-document");
            case VERSION -> manifest.setDocVersion(0);
            case OMITTED -> manifest.removeParts(2);
            case EXTRA -> manifest.addParts(manifest.getParts(2).toBuilder().setSubKey("extra"));
            case ORDER -> { var second = manifest.getParts(1); manifest.setParts(1, manifest.getParts(2)); manifest.setParts(2, second); }
            case KEY -> manifest.getPartsBuilder(1).setObjectKey("documents/another-key");
            case HASH -> manifest.getPartsBuilder(1).setSha256("cd".repeat(32));
            case SIZE -> manifest.getPartsBuilder(1).setSizeBytes(4);
            case SUBKEY -> manifest.getPartsBuilder(1).setSubKey("another-slot");
            case CORE_VERSION -> row.versionId = "other-version";
            case CORE_ETAG -> row.etag = "other-etag";
            case TOTAL_SIZE -> row.sizeBytes = 10L;
            case ROOT -> row.checksum = "cd".repeat(32);
            case EMPTY_WITH_KEY -> manifest.addParts(PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS)
                    .setState(PartState.PART_STATE_EMPTY).setObjectKey("documents/hidden-key"));
            case DUPLICATE_TOMBSTONE -> manifest.addParts(manifest.getParts(1).toBuilder().setState(PartState.PART_STATE_DELETED).setDeletedReason("RTBF"));
            case UNKNOWN_STATE -> manifest.getPartsBuilder(1).setStateValue(999);
            case PENDING_STATUS -> row.status = DocumentStatus.PENDING_PURGE;
            case PENDING_PURGE -> row.pendingPurgeId = UUID.randomUUID();
            case UNKNOWN_FIELD -> { /* Inject persisted JSON below, after normal serialization. */ }
        }
        row.writeManifest(manifest.build());
        if (mismatch == Mismatch.UNKNOWN_FIELD)
            row.partManifest = row.partManifest.substring(0, row.partManifest.length() - 1) + ",\"unrecognizedIdentity\":\"must-not-disappear\"}";
        Long expected = revision;
        var expectedSources = sources;
        assertThatThrownBy(() -> validate(f, expected, expectedSources)).isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
        assertThat(new DocumentLedger(tx).findByNodeId(f.attempt.location().nodeId())).isEmpty();
    }

    @Test void emptyAndHistoricalDeletedSlotsAreNotLiveObjects() {
        var f = fixture(true);
        var manifest = f.row.readManifest().toBuilder()
                .addParts(PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS).setState(PartState.PART_STATE_EMPTY))
                .addParts(PartManifestEntry.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).setState(PartState.PART_STATE_DELETED)
                        .setObjectKey("historical/parsed").setSha256(SHA).setSizeBytes(12).setDeletedReason("RTBF"));
        f.row.writeManifest(manifest.build());
        validate(f, 7L, f.plan.sources());
    }

    private static void validate(Fixture fixture, Long revision, Map<UUID, Long> sources) {
        tx.inTransaction(em -> {
            DocumentPartAttemptLedger.requirePublishable(em, fixture.attempt.id(), fixture.attempt.token(), fixture.row, revision, sources);
        });
    }
    private record Fixture(DocumentPartAttemptLedger.Plan plan, DocumentPartAttemptLedger.Attempt attempt, DocumentRecord row) {}
    private static Fixture fixture(boolean verify) {
        String generation = "publication-" + UUID.randomUUID();
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "https://storage.example")), generation));
        UUID node = UUID.randomUUID(), id = UUID.randomUUID();
        var location = new DocumentPartAttemptLedger.Location(node, "account", generation, "container");
        String prefix = "documents/account/" + node + "/attempts/" + id + "/";
        var objects = List.of(
                new DocumentPartAttemptLedger.PlannedObject(DocumentPart.DOCUMENT_PART_CORE, "", prefix + "core", 3, SHA, "application/protobuf"),
                new DocumentPartAttemptLedger.PlannedObject(DocumentPart.DOCUMENT_PART_CHUNKS, "z", prefix + "z", 3, SHA, "application/protobuf"),
                new DocumentPartAttemptLedger.PlannedObject(DocumentPart.DOCUMENT_PART_CHUNKS, "a", prefix + "a", 3, SHA, "application/protobuf"));
        var plan = new DocumentPartAttemptLedger.Plan(id, location, 7, Map.of(node, 7L), objects);
        var ledger = new DocumentPartAttemptLedger(tx);
        var attempt = ledger.begin(plan, Duration.ofMinutes(5));
        if (verify) for (var object : objects) ledger.verify(id, attempt.token(), object.objectKey(), 3, SHA, "v1", "etag");
        var row = new DocumentRecord();
        row.nodeId = node; row.accountId = "account"; row.docId = "doc"; row.graphId = "intake:account"; row.graphAddressId = "source";
        row.versionId = "v1"; row.etag = "etag"; row.sizeBytes = 9L;
        var manifest = DocumentManifest.newBuilder().setDocVersion(1).setAddress(NodeAddress.newBuilder()
                .setAccountId(row.accountId).setDocId(row.docId).setGraphId(row.graphId).setGraphAddressId(row.graphAddressId));
        for (var object : objects) manifest.addParts(PartManifestEntry.newBuilder().setPart(object.part()).setSubKey(object.subKey())
                .setObjectKey(object.objectKey()).setSizeBytes(object.size()).setSha256(object.sha256()).setState(PartState.PART_STATE_PRESENT));
        row.writeManifest(manifest.build());
        row.checksum = DocumentPartCodec.rootChecksumFromManifest(manifest.build());
        return new Fixture(plan, attempt, row);
    }
}
