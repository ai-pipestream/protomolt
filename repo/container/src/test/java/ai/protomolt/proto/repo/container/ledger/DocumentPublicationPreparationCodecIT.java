package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL source snapshots and restored admission; decoding itself performs no SQL or provider I/O. */
@Testcontainers
class DocumentPublicationPreparationCodecIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final Duration LEASE = Duration.ofMinutes(1).plusNanos(123);

    @Test void roundTripPreservesExactPreparedIdentitiesIntoSql() {
        try (var c = context(POSTGRES)) {
            var original = input(c);
            var bytes = DocumentPublicationPreparationCodec.encode(original);
            var decoded = decode(bytes, original);
            assertThat(decoded.command().intent()).isEqualTo(original.command().intent());
            assertThat(decoded.placements()).isEqualTo(original.placements());
            assertThat(decoded.seeds().ownerNonce()).isEqualTo(original.seeds().ownerNonce());
            assertThat(decoded.seeds().attempts()).isEqualTo(original.seeds().attempts());
            assertThat(decoded.seeds().uploadTokens()).isEqualTo(original.seeds().uploadTokens());
            assertThat(decoded.lease()).isEqualTo(LEASE);
            assertThat(decoded.predecessorGeneration()).isZero();
            assertThat(decoded.prepare().members()).isEqualTo(original.prepare().members());
            assertThat(new RepositoryOperationLedger(c.tx()).find(original.key())).isEmpty();
            var owner = new RepositoryOperationLedger(c.tx()).admit(decoded.key(), decoded.command(), decoded.seeds().ownerNonce(), decoded.lease())
                    .owner().orElseThrow();
            var attempts = new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(
                    new RepositoryCaller("principal", true), owner, decoded.prepare());
            assertThat(attempts).hasSize(1);
            assertThat(attempts.getFirst().id()).isEqualTo(original.seeds().attempts().get("member-0"));
            assertThat(attempts.getFirst().token()).isEqualTo(original.seeds().uploadTokens().get("member-0"));
            assertThat(decoded.toString()).isEqualTo("DocumentPublicationPreparationRecord[private]");
        }
    }

    @Test void preservesNullableAndRichSnapshotsWithoutResolvingCurrentConfiguration() {
        try (var c = context(POSTGRES)) {
            var original = input(c); var entry = original.placements().entrySet().iterator().next(); var d = entry.getValue().drive();
            var location = new LinkedHashMap<String, String>(); location.put("z", "last"); location.put("a", "first");
            var drive = new DocumentDriveSnapshot(d.id(), d.account(), "named π", d.type(), d.provider(), d.namespace(), d.prefix(),
                    "region", "credential-reference", "{\"title\":\"雪\"}", "{\"setting\":true}", d.status());
            var placement = new DocumentUploadPlan.Placement(drive, "native-test", new ManagedBackendLedger.Profile(
                    new BackendIdentity(d.provider(), d.provider()+"/v1", location), "native-test"));
            var value = new DocumentPublicationPreparationRecord(original.key(), original.command(), original.seeds(), Map.of(d.id(), placement), LEASE, 7);
            var decoded = decode(DocumentPublicationPreparationCodec.encode(value), value);
            assertThat(decoded.placements()).isEqualTo(value.placements());
            assertThat(decoded.predecessorGeneration()).isEqualTo(7);
            var reordered = new DocumentUploadPlan.Placement(drive, "native-test", new ManagedBackendLedger.Profile(
                    new BackendIdentity(d.provider(), d.provider()+"/v1", new java.util.TreeMap<>(location)), "native-test"));
            assertThat(DocumentPublicationPreparationCodec.placementDigest(Map.of(d.id(), reordered)))
                    .isEqualTo(DocumentPublicationPreparationCodec.placementDigest(value.placements()));
            assertThat(DocumentPublicationPreparationCodec.placementDigest(original.placements()))
                    .isNotEqualTo(DocumentPublicationPreparationCodec.placementDigest(value.placements()));
            assertThat(DocumentPublicationPreparationCodec.encode(new DocumentPublicationPreparationRecord(value.key(), value.command(),
                    value.seeds(), Map.of(d.id(), reordered), LEASE, 7))).isEqualTo(DocumentPublicationPreparationCodec.encode(value));
            assertThat(new RepositoryOperationLedger(c.tx()).find(value.key())).isEmpty();
        }
    }

    @Test void rejectsWrongJournalScopeDigestVersionAndMalformedEnvelope() {
        try (var c = context(POSTGRES)) {
            var value = input(c); var bytes = DocumentPublicationPreparationCodec.encode(value);
            var wrongKey = new RepositoryOperationLedger.Key(value.key().account(), "another", value.key().operationId());
            assertThatThrownBy(() -> DocumentPublicationPreparationCodec.decode(bytes, wrongKey, value.command().sha256()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> DocumentPublicationPreparationCodec.decode(bytes, value.key(), "0".repeat(64)))
                    .isInstanceOf(IllegalArgumentException.class);
            for (int end : new int[]{0, 1, 7, 16, bytes.size()/2, bytes.size()-1})
                assertThatThrownBy(() -> decode(bytes.substring(0, end), value)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> decode(bytes.concat(ByteString.copyFromUtf8("extra")), value)).hasMessageContaining("Trailing");
            var version = bytes.toByteArray(); java.nio.ByteBuffer.wrap(version).putInt(4, 99);
            assertThatThrownBy(() -> decode(ByteString.copyFrom(version), value)).hasMessageContaining("Unsupported");
            var badLength = bytes.toByteArray(); java.nio.ByteBuffer.wrap(badLength).putInt(8, Integer.MAX_VALUE);
            assertThatThrownBy(() -> decode(ByteString.copyFrom(badLength), value)).hasMessageContaining("length");
            var badUtf8 = bytes.toByteArray(); badUtf8[12] = (byte) 0xff;
            assertThatThrownBy(() -> decode(ByteString.copyFrom(badUtf8), value)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void rejectsDuplicateIdentityEntriesAndOversizedCountsBeforeAllocation() throws Exception {
        try (var c = context(POSTGRES)) {
            var value = input(c); byte[] bytes = DocumentPublicationPreparationCodec.encode(value).toByteArray();
            // Locate the documented v1 identity list after the bounded command and lease header.
            var raw = new ByteArrayInputStream(bytes); var in = new DataInputStream(raw);
            in.skipNBytes(8); skipBlock(in); skipBlock(in); in.skipNBytes(16);
            skipBlock(in); in.skipNBytes(4); skipBlock(in); skipBlock(in); in.skipNBytes(8+4+8+16);
            int countOffset = bytes.length-raw.available(); assertThat(in.readInt()).isEqualTo(1);
            int entryStart = bytes.length-raw.available(); skipBlock(in); in.skipNBytes(16); int entryEnd = bytes.length-raw.available();
            var duplicate = new java.io.ByteArrayOutputStream(); duplicate.write(bytes, 0, entryEnd);
            duplicate.write(bytes, entryStart, entryEnd-entryStart); duplicate.write(bytes, entryEnd, bytes.length-entryEnd);
            var encoded = duplicate.toByteArray(); java.nio.ByteBuffer.wrap(encoded).putInt(countOffset, 2);
            assertThatThrownBy(() -> decode(ByteString.copyFrom(encoded), value)).hasMessageContaining("Duplicate");
            java.nio.ByteBuffer.wrap(bytes).putInt(countOffset, Integer.MAX_VALUE);
            assertThatThrownBy(() -> decode(ByteString.copyFrom(bytes), value)).hasMessageContaining("collection");
        }
    }

    @Test void rejectsInvalidCoverageLeaseAndAliasedIdentitiesBeforeEncoding() {
        try (var c = context(POSTGRES)) {
            var value = input(c);
            assertThatThrownBy(() -> new DocumentPublicationPreparationRecord(value.key(), value.command(), value.seeds(), Map.of(), LEASE, 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new DocumentPublicationPreparationRecord(value.key(), value.command(), value.seeds(), value.placements(), Duration.ZERO, 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new DocumentPublicationPreparationRecord(value.key(), value.command(), value.seeds(), value.placements(), LEASE, Long.MAX_VALUE))
                    .isInstanceOf(IllegalArgumentException.class);
            var other = new RepositoryOperationLedger.Key(value.key().account(), "other", value.key().operationId());
            assertThatThrownBy(() -> new DocumentPublicationPreparationRecord(other, value.command(), value.seeds(), value.placements(), LEASE, 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void rejectsEquivalentButNoncanonicalMemberOrder() throws Exception {
        try (var c = context(POSTGRES)) {
            var value = input(c); byte[] bytes = DocumentPublicationPreparationCodec.encode(value).toByteArray();
            var raw = new ByteArrayInputStream(bytes); var in = new DataInputStream(raw);
            in.skipNBytes(8); skipBlock(in); skipBlock(in); in.skipNBytes(16);
            skipBlock(in); in.skipNBytes(4); skipBlock(in);
            int length = in.readInt(); int offset = bytes.length-raw.available();
            var reversed = value.command().intent().toBuilder().clearMembers()
                    .addAllMembers(value.command().intent().getMembersList().reversed()).build();
            assertThat(new DocumentPublicationCommand(reversed).sha256()).isEqualTo(value.command().sha256());
            assertThat(reversed.toByteArray()).hasSize(length);
            System.arraycopy(reversed.toByteArray(), 0, bytes, offset, length);
            assertThatThrownBy(() -> decode(ByteString.copyFrom(bytes), value)).hasMessageContaining("Noncanonical");
        }
    }

    @Test void aggregateAndTextBoundsApplyOnEncodeAndDecode() {
        try (var c = context(POSTGRES)) {
            var value = input(c);
            assertThatThrownBy(() -> decode(ByteString.copyFrom(new byte[DocumentPublicationPreparationCodec.MAX_BYTES+1]), value))
                    .hasMessageContaining("size");
            var template = value.placements().values().iterator().next(); var drive = template.drive();
            for (String metadata : new String[]{"x".repeat(1024*1024+1), "\uD800"}) {
                var invalid = new DocumentDriveSnapshot(drive.id(), drive.account(), drive.name(), drive.type(), drive.provider(),
                        drive.namespace(), drive.prefix(), drive.region(), drive.credentials(), metadata, drive.config(), drive.status());
                var record = new DocumentPublicationPreparationRecord(value.key(), value.command(), value.seeds(),
                        Map.of(drive.id(), new DocumentUploadPlan.Placement(invalid, template.generation(), template.profile())), LEASE, 0);
                assertThatThrownBy(() -> DocumentPublicationPreparationCodec.encode(record)).isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> DocumentPublicationPreparationCodec.placementDigest(record.placements()))
                        .isInstanceOf(IllegalArgumentException.class);
            }
            // Each snapshot is legal on its own; their combined encoding exceeds the envelope limit.
            var intent = value.command().intent().toBuilder().clearMembers();
            var placements = new HashMap<UUID, DocumentUploadPlan.Placement>();
            String metadata = "x".repeat(1024*1024);
            var member = value.command().intent().getMembers(0);
            for (int i = 0; i < 20; i++) {
                UUID id = UUID.randomUUID();
                intent.addMembers(member.toBuilder().setMemberId("large-"+i).setDriveId(id.toString())
                        .setDestination(member.getDestination().toBuilder()
                                .setAddress(member.getDestination().getAddress().toBuilder().setDocId("large-"+i))));
                var snapshot = new DocumentDriveSnapshot(id, drive.account(), drive.name(), drive.type(), drive.provider(),
                        drive.namespace(), drive.prefix(), drive.region(), drive.credentials(), metadata, drive.config(), drive.status());
                placements.put(id, new DocumentUploadPlan.Placement(snapshot, template.generation(), template.profile()));
            }
            var command = new DocumentPublicationCommand(intent.build());
            var oversized = new DocumentPublicationPreparationRecord(value.key(), command,
                    DocumentPublicationSeeds.mint(value.key(), command), placements, LEASE, 0);
            assertThatThrownBy(() -> DocumentPublicationPreparationCodec.encode(oversized)).hasMessageContaining("total bound");
            assertThatThrownBy(() -> DocumentPublicationPreparationCodec.placementDigest(placements)).hasMessageContaining("total bound");
        }
    }

    private static void skipBlock(DataInputStream in) throws Exception { int size = in.readInt(); if (size >= 0) in.skipNBytes(size); }
    private static DocumentPublicationPreparationRecord decode(ByteString bytes, DocumentPublicationPreparationRecord value) {
        return DocumentPublicationPreparationCodec.decode(bytes, value.key(), value.command().sha256());
    }
    private static DocumentPublicationPreparationRecord input(Context c) {
        var source = prepare(c, 2, true);
        var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
        UUID drive = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var placements = Map.of(drive, DocumentUploadPlan.Placement.sample(new DriveLedger(c.tx()).findById(drive).orElseThrow(), "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow()));
        return new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), placements, LEASE, 0);
    }
}
