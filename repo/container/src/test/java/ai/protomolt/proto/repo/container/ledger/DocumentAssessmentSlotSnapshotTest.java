package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentAssessmentSlotSnapshotTest {
    private static final UUID ID = new UUID(1, 2);
    private static final RepositoryOperationLedger.Key KEY = new RepositoryOperationLedger.Key("account", "principal", new UUID(3, 4));
    private static final Instant DEADLINE = Instant.parse("2026-10-05T00:00:00.123456Z");
    private static final DocumentAssessmentSlotSnapshot.Identity IDENTITY = new DocumentAssessmentSlotSnapshot.Identity(ID, KEY, 1, "ab".repeat(32), "cd".repeat(32), DEADLINE);
    private static final DocumentAssessmentSlots.Slot UPLOAD = new DocumentAssessmentSlots.Slot("a", 2, 1, new UUID(5, 6), "NEW_CONTENT", null, null);
    private static final DocumentAssessmentSlots.Slot REUSE = new DocumentAssessmentSlots.Slot("b", 5, 2, new UUID(7, 8), "REUSE", new UUID(9, 10), 31);

    @Test void deterministicOrderingAndOwnedLifetime() {
        var budget = new PayloadBudget(100000);
        var first = DocumentAssessmentSlotSnapshot.encode(IDENTITY, List.of(REUSE, UPLOAD), budget, () -> {});
        try (first; var second = DocumentAssessmentSlotSnapshot.encode(IDENTITY, List.of(UPLOAD, REUSE), budget, () -> {})) {
            assertThat(first.bytes()).isEqualTo(second.bytes());
            assertThat(first.sha256()).isEqualTo(second.sha256());
            assertThat(first.bytes().substring(0, 8).toByteArray()).containsExactly(0x50, 0x4d, 0x41, 0x53, 0, 0, 0, 1);
            assertThat(budget.reservedBytes()).isGreaterThan(first.bytes().size() * 2L);
        }
        assertThat(budget.reservedBytes()).isZero();
        first.close();
        assertThatThrownBy(first::bytes).hasMessageContaining("closed");
    }

    @Test void everyHeaderIdentityChangesDigest() {
        String original = digest(IDENTITY, List.of(UPLOAD, REUSE));
        var variants = List.of(
                new DocumentAssessmentSlotSnapshot.Identity(new UUID(11, 12), KEY, 1, IDENTITY.commandSha256(), IDENTITY.manifestSha256(), DEADLINE),
                new DocumentAssessmentSlotSnapshot.Identity(ID, new RepositoryOperationLedger.Key("other", KEY.principal(), KEY.operationId()), 1, IDENTITY.commandSha256(), IDENTITY.manifestSha256(), DEADLINE),
                new DocumentAssessmentSlotSnapshot.Identity(ID, new RepositoryOperationLedger.Key(KEY.account(), "other", KEY.operationId()), 1, IDENTITY.commandSha256(), IDENTITY.manifestSha256(), DEADLINE),
                new DocumentAssessmentSlotSnapshot.Identity(ID, new RepositoryOperationLedger.Key(KEY.account(), KEY.principal(), ID), 1, IDENTITY.commandSha256(), IDENTITY.manifestSha256(), DEADLINE),
                new DocumentAssessmentSlotSnapshot.Identity(ID, KEY, 2, IDENTITY.commandSha256(), IDENTITY.manifestSha256(), DEADLINE),
                new DocumentAssessmentSlotSnapshot.Identity(ID, KEY, 1, "ef".repeat(32), IDENTITY.manifestSha256(), DEADLINE),
                new DocumentAssessmentSlotSnapshot.Identity(ID, KEY, 1, IDENTITY.commandSha256(), "ef".repeat(32), DEADLINE),
                new DocumentAssessmentSlotSnapshot.Identity(ID, KEY, 1, IDENTITY.commandSha256(), IDENTITY.manifestSha256(), DEADLINE.plusNanos(1000)));
        for (var variant : variants) assertThat(digest(variant, List.of(UPLOAD, REUSE))).isNotEqualTo(original);
    }

    @Test void versionOneWireLayoutHasExplicitLengthsAndFullSourceOrdinal() {
        var budget = new PayloadBudget(100000);
        try (var encoded = DocumentAssessmentSlotSnapshot.encode(IDENTITY, List.of(REUSE), budget, () -> {})) {
            var wire = encoded.bytes().asReadOnlyByteBuffer();
            assertThat(wire.getInt()).isEqualTo(0x504d4153);
            assertThat(wire.getInt()).isEqualTo(1);
            assertThat(wire.getLong()).isEqualTo(1); assertThat(wire.getLong()).isEqualTo(2);
            assertThat(readText(wire)).isEqualTo("account"); assertThat(readText(wire)).isEqualTo("principal");
            assertThat(wire.getLong()).isEqualTo(3); assertThat(wire.getLong()).isEqualTo(4);
            assertThat(wire.getLong()).isEqualTo(1);
            byte[] digest = new byte[32]; wire.get(digest);
            assertThat(java.util.HexFormat.of().formatHex(digest)).isEqualTo("ab".repeat(32));
            wire.get(digest); assertThat(java.util.HexFormat.of().formatHex(digest)).isEqualTo("cd".repeat(32));
            assertThat(wire.getLong()).isEqualTo(DEADLINE.getEpochSecond()); assertThat(wire.getInt()).isEqualTo(123456000);
            assertThat(wire.getInt()).isEqualTo(1); assertThat(readText(wire)).isEqualTo("b");
            assertThat(wire.getInt()).isEqualTo(5); assertThat(wire.getLong()).isEqualTo(2);
            assertThat(wire.getLong()).isEqualTo(7); assertThat(wire.getLong()).isEqualTo(8);
            assertThat(wire.get()).isEqualTo((byte) 1);
            assertThat(wire.getLong()).isEqualTo(9); assertThat(wire.getLong()).isEqualTo(10);
            assertThat(wire.getInt()).isEqualTo(31); assertThat(wire.hasRemaining()).isFalse();
        }
    }

    @Test void everySlotIdentityChangesDigest() {
        String original = digest(IDENTITY, List.of(REUSE));
        var variants = List.of(
                new DocumentAssessmentSlots.Slot("c", 5, 2, REUSE.object(), "REUSE", REUSE.sourceRevision(), 31),
                new DocumentAssessmentSlots.Slot("b", 6, 2, REUSE.object(), "REUSE", REUSE.sourceRevision(), 31),
                new DocumentAssessmentSlots.Slot("b", 5, 3, REUSE.object(), "REUSE", REUSE.sourceRevision(), 31),
                new DocumentAssessmentSlots.Slot("b", 5, 2, ID, "REUSE", REUSE.sourceRevision(), 31),
                new DocumentAssessmentSlots.Slot("b", 5, 2, REUSE.object(), "REUSE", ID, 31),
                new DocumentAssessmentSlots.Slot("b", 5, 2, REUSE.object(), "REUSE", REUSE.sourceRevision(), 32),
                new DocumentAssessmentSlots.Slot("b", 5, 2, REUSE.object(), "NEW_CONTENT", null, null));
        for (var variant : variants) assertThat(digest(IDENTITY, List.of(variant))).isNotEqualTo(original);
    }

    @Test void rejectsDuplicatesMalformedAlternativesAndPrecision() {
        assertThatThrownBy(() -> digest(IDENTITY, List.of(UPLOAD, UPLOAD))).hasMessageContaining("Duplicate");
        assertThatThrownBy(() -> digest(IDENTITY, List.of())).hasMessageContaining("count");
        assertThatThrownBy(() -> digest(IDENTITY, List.of(new DocumentAssessmentSlots.Slot("a", 0, 1, ID, "NEW_CONTENT", ID, 0))))
                .hasMessageContaining("source binding");
        assertThatThrownBy(() -> new DocumentAssessmentSlotSnapshot.Identity(ID, KEY, 1, IDENTITY.commandSha256(), IDENTITY.manifestSha256(), DEADLINE.plusNanos(1)))
                .hasMessageContaining("microsecond");
        var malformed = new RepositoryOperationLedger.Key("bad\ud800", "principal", ID);
        assertThatThrownBy(() -> digest(new DocumentAssessmentSlotSnapshot.Identity(ID, malformed, 1, IDENTITY.commandSha256(), IDENTITY.manifestSha256(), DEADLINE), List.of(UPLOAD)))
                .hasMessageContaining("UTF-16");
    }

    @Test void boundsMaximumSetAndRefusesBudgetBeforeEncoding() {
        var slots = new ArrayList<DocumentAssessmentSlots.Slot>();
        for (int i = 0; i < 10000; i++) slots.add(new DocumentAssessmentSlots.Slot("a".repeat(128), i, 1, ID, "REUSE", ID, i));
        var budget = new PayloadBudget(10_000_000);
        try (var encoded = DocumentAssessmentSlotSnapshot.encode(IDENTITY, slots, budget, () -> {})) {
            assertThat(encoded.bytes().size()).isLessThanOrEqualTo(DocumentAssessmentSlotSnapshot.MAX_BYTES);
        }
        assertThat(budget.reservedBytes()).isZero();
        slots.add(UPLOAD);
        assertThatThrownBy(() -> DocumentAssessmentSlotSnapshot.encode(IDENTITY, slots, budget, () -> {})).hasMessageContaining("count");
        var tiny = new PayloadBudget(1);
        assertThatThrownBy(() -> DocumentAssessmentSlotSnapshot.encode(IDENTITY, List.of(UPLOAD), tiny, () -> {}))
                .isInstanceOf(PayloadBudget.CapacityExceededException.class);
        assertThat(tiny.reservedBytes()).isZero();
    }

    @Test void cancellationAfterAllocationReleasesReservation() {
        var budget = new PayloadBudget(100000);
        var cancelled = new CancellationException("stop");
        var observed = new AtomicInteger();
        assertThatThrownBy(() -> DocumentAssessmentSlotSnapshot.encode(IDENTITY, List.of(UPLOAD, REUSE), budget, () -> {
            if (budget.reservedBytes() > 0) { observed.incrementAndGet(); throw cancelled; }
        })).isSameAs(cancelled);
        assertThat(observed).hasValue(1);
        assertThat(budget.reservedBytes()).isZero();
    }
    private static String digest(DocumentAssessmentSlotSnapshot.Identity identity, List<DocumentAssessmentSlots.Slot> slots) {
        var budget = new PayloadBudget(100000);
        try (var encoded = DocumentAssessmentSlotSnapshot.encode(identity, slots, budget, () -> {})) { return encoded.sha256(); }
    }
    private static String readText(java.nio.ByteBuffer wire) {
        byte[] value = new byte[wire.getInt()]; wire.get(value);
        return new String(value, java.nio.charset.StandardCharsets.UTF_8);
    }
}
