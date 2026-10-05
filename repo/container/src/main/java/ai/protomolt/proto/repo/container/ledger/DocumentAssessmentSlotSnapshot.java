package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Canonical storage provenance, not observed validation evidence or authorization. */
final class DocumentAssessmentSlotSnapshot {
    static final String CODEC = "document-assessment-slots";
    static final int VERSION = 1;
    static final int MAX_BYTES = 4 * 1024 * 1024;
    private static final int MAGIC = 0x504d4153; // PMAS

    record Identity(UUID assessment, RepositoryOperationLedger.Key key, long generation,
                    String commandSha256, String manifestSha256, Instant retainUntil) {
        Identity {
            Objects.requireNonNull(assessment); Objects.requireNonNull(key); Objects.requireNonNull(retainUntil);
            if (generation < 1 || retainUntil.getNano() % 1000 != 0)
                throw new IllegalArgumentException("Snapshot requires positive generation and microsecond deadline");
            requireDigest(commandSha256); requireDigest(manifestSha256);
        }
    }

    /** Bytes are borrowed while open. The caller must reserve separately for JDBC copies. */
    static final class Encoded implements AutoCloseable {
        private ByteString bytes;
        private final String sha256;
        private final PayloadBudget.Lease lease;
        private Encoded(ByteString bytes, String sha256, PayloadBudget.Lease lease) {
            this.bytes = bytes; this.sha256 = sha256; this.lease = lease;
        }
        synchronized ByteString bytes() { requireOpen(); return bytes; }
        synchronized String sha256() { requireOpen(); return sha256; }
        private void requireOpen() { if (bytes == null) throw new IllegalStateException("Snapshot encoding is closed"); }
        @Override public synchronized void close() { bytes = null; lease.close(); }
    }

    private DocumentAssessmentSlotSnapshot() {}

    /** Exact re-encoding of retained rows permits comparison without parsing untrusted lengths. */
    static Encoded encode(Identity identity, List<DocumentAssessmentSlots.Slot> input, PayloadBudget budget, Runnable control) {
        Objects.requireNonNull(identity); Objects.requireNonNull(budget); Objects.requireNonNull(control);
        check(control);
        if (input.isEmpty() || input.size() > 10000) throw new IllegalArgumentException("Snapshot slot count must be 1..10000");
        var slots = new ArrayList<>(input);
        // Members are restricted to ASCII, so Java order is also unsigned UTF-8 order.
        slots.sort(Comparator.comparing(DocumentAssessmentSlots.Slot::member).thenComparingInt(DocumentAssessmentSlots.Slot::ordinal));
        int size = 8 + 16 + textSize(identity.key().account()) + textSize(identity.key().principal())
                + 16 + 8 + 32 + 32 + 8 + 4 + 4;
        DocumentAssessmentSlots.Slot previous = null;
        for (var slot : slots) {
            check(control); validate(slot);
            if (previous != null && previous.member().equals(slot.member()) && previous.ordinal() == slot.ordinal())
                throw new IllegalArgumentException("Duplicate snapshot candidate slot");
            size += 4 + slot.member().length() + 4 + 8 + 16 + 1 + (slot.sourceRevision() == null ? 0 : 20);
            previous = slot;
        }
        if (size > MAX_BYTES) throw new IllegalArgumentException("Snapshot exceeds byte bound");
        // One exact encoding array, its immutable copy, and bounded UTF-8 text scratch.
        var lease = budget.reserve(2L * size + 800);
        try {
            var out = ByteBuffer.allocate(size);
            out.putInt(MAGIC).putInt(VERSION); uuid(out, identity.assessment());
            text(out, identity.key().account()); text(out, identity.key().principal()); uuid(out, identity.key().operationId());
            out.putLong(identity.generation()); out.put(HexFormat.of().parseHex(identity.commandSha256()));
            out.put(HexFormat.of().parseHex(identity.manifestSha256()));
            out.putLong(identity.retainUntil().getEpochSecond()).putInt(identity.retainUntil().getNano()).putInt(slots.size());
            for (var slot : slots) {
                check(control); text(out, slot.member()); out.putInt(slot.ordinal()).putLong(slot.selection()); uuid(out, slot.object());
                out.put((byte) (slot.sourceRevision() == null ? 0 : 1));
                if (slot.sourceRevision() != null) { uuid(out, slot.sourceRevision()); out.putInt(slot.sourceOrdinal()); }
            }
            if (out.hasRemaining()) throw new IllegalStateException("Snapshot size differs from encoding");
            var bytes = ByteString.copyFrom(out.array());
            var digest = MessageDigest.getInstance("SHA-256"); digest.update(bytes.asReadOnlyByteBuffer());
            check(control);
            return new Encoded(bytes, HexFormat.of().formatHex(digest.digest()), lease);
        } catch (NoSuchAlgorithmException failure) {
            lease.close(); throw new IllegalStateException("SHA-256 is required", failure);
        } catch (RuntimeException | Error failure) { lease.close(); throw failure; }
    }

    private static void validate(DocumentAssessmentSlots.Slot slot) {
        if (slot.member() == null || !slot.member().matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
                || slot.ordinal() < 0 || slot.ordinal() >= 10000 || slot.selection() < 1 || slot.object() == null)
            throw new IllegalArgumentException("Invalid snapshot candidate slot");
        boolean upload = "NEW_CONTENT".equals(slot.declaration()) && slot.sourceRevision() == null && slot.sourceOrdinal() == null;
        boolean reuse = "REUSE".equals(slot.declaration()) && slot.sourceRevision() != null
                && slot.sourceOrdinal() != null && slot.sourceOrdinal() >= 0;
        if (!upload && !reuse) throw new IllegalArgumentException("Invalid snapshot source binding");
    }
    private static int textSize(String text) {
        if (text == null || text.isEmpty() || text.length() > 200) throw new IllegalArgumentException("Invalid snapshot text");
        int bytes = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i == text.length() || !Character.isLowSurrogate(text.charAt(i))) throw new IllegalArgumentException("Malformed snapshot UTF-16");
                bytes += 4;
            } else if (Character.isLowSurrogate(c)) throw new IllegalArgumentException("Malformed snapshot UTF-16");
            else bytes += c < 128 ? 1 : c < 2048 ? 2 : 3;
        }
        return 4 + bytes;
    }
    private static void text(ByteBuffer out, String value) { byte[] bytes = value.getBytes(StandardCharsets.UTF_8); out.putInt(bytes.length).put(bytes); }
    private static void uuid(ByteBuffer out, UUID value) { out.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()); }
    private static void requireDigest(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Snapshot requires canonical SHA-256");
    }
    private static void check(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Snapshot encoding interrupted");
        control.run();
    }
}
