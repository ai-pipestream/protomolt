package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Private payload copies for one exact prepared plan and explicit member subset.
 * This grants no admission or provider authority. The coordinator must retain this
 * reservation until every started provider call and observation flusher drains.
 */
final class DocumentUploadPayloads implements AutoCloseable {
    record Key(String member, int revisionOrdinal) {
        Key {
            Objects.requireNonNull(member);
            if (revisionOrdinal < 0) throw new IllegalArgumentException("Negative revision ordinal");
        }
    }

    /** Body is borrowed by internal transfer workers; neither retain nor modify it. */
    record Entry(Key key, UUID attempt, DocumentUploadPlan.Upload upload, byte[] body) {}

    private final DocumentUploadPlan.Prepared plan;
    private final Set<String> members;
    private final PayloadBudget.Lease reservation;
    private List<Entry> entries;
    private boolean claimed;

    private DocumentUploadPayloads(DocumentUploadPlan.Prepared plan, Set<String> members,
            PayloadBudget.Lease reservation, List<Entry> entries) {
        this.plan = plan;
        this.members = members;
        this.reservation = reservation;
        this.entries = List.copyOf(entries);
    }

    static DocumentUploadPayloads prepare(DocumentUploadPlan.Prepared plan, Set<String> members,
            Map<Key, PartObject> supplied, PayloadBudget budget) {
        Objects.requireNonNull(plan); Objects.requireNonNull(budget);
        if (members.size() > plan.members().size() || supplied.size() > DocumentPublicationCommand.MAX_PARTS)
            throw new IllegalArgumentException("Payload selection exceeds command bounds");
        members = Set.copyOf(members);
        supplied = Map.copyOf(supplied);
        var expected = new HashMap<Key, DocumentUploadPlan.Upload>();
        var attempts = new HashMap<Key, UUID>();
        var ordered = new ArrayList<Key>();
        int selected = 0;
        for (var member : plan.members()) {
            String id = member.intent().getMemberId();
            if (!members.contains(id)) continue;
            selected++;
            if (member.attempt().isEmpty()) continue;
            var attempt = member.attempt().orElseThrow();
            for (var upload : attempt.uploads()) {
                var key = new Key(id, upload.revisionOrdinal());
                expected.put(key, upload);
                attempts.put(key, attempt.id());
                ordered.add(key);
            }
        }
        if (selected != members.size() || !expected.keySet().equals(supplied.keySet()))
            throw new IllegalArgumentException("Payload keys differ from selected command uploads");
        long bytes = 0;
        for (var key : ordered) {
            var part = supplied.get(key);
            var declaration = expected.get(key).object();
            if (part.part() != declaration.part() || !Objects.equals(part.subKey(), declaration.subKey())
                    || part.bytes() == null || part.bytes().length != declaration.size()
                    || !Objects.equals(part.sha256(), declaration.sha256()))
                throw new IllegalArgumentException("Payload slot or declaration differs from command");
            bytes = Math.addExact(bytes, part.bytes().length);
        }
        var reservation = budget.reserve(Math.multiplyExact(bytes, 2));
        boolean completed = false;
        try {
            var entries = new ArrayList<Entry>(ordered.size());
            for (var key : ordered) {
                byte[] body = supplied.get(key).bytes().clone();
                var upload = expected.get(key);
                if (!DocumentPartCodec.sha256Hex(body).equals(upload.object().sha256()))
                    throw new IllegalArgumentException("Private payload checksum differs from command");
                entries.add(new Entry(key, attempts.get(key), upload, body));
            }
            var result = new DocumentUploadPayloads(plan, members, reservation, entries);
            completed = true;
            return result;
        } finally {
            if (!completed) reservation.close();
        }
    }

    /** Requires this exact preparation, not another attempt for the same command. */
    synchronized Use claim(DocumentUploadPlan.Prepared expected, Set<String> expectedMembers) {
        if (entries == null) throw new IllegalStateException("Upload payloads are closed");
        if (claimed) throw new IllegalStateException("Upload payloads are already claimed");
        if (plan != expected) throw new IllegalArgumentException("Upload payloads belong to another prepared plan");
        if (!members.equals(expectedMembers)) throw new IllegalArgumentException("Upload payloads belong to another member subset");
        var use = new Use(this);
        claimed = true;
        return use;
    }

    @Override public synchronized void close() {
        if (claimed && entries != null) throw new IllegalStateException("Active transfer owns upload payloads");
        entries = null;
        reservation.close();
    }

    /** The coordinator alone closes this handle, after draining every worker. */
    static final class Use implements AutoCloseable {
        private final DocumentUploadPayloads owner;
        private Use(DocumentUploadPayloads owner) { this.owner = owner; }
        List<Entry> entries() {
            synchronized (owner) {
                if (owner.entries == null) throw new IllegalStateException("Upload payloads are closed");
                return owner.entries;
            }
        }
        View view() { return new View(entries()); }
        @Override public void close() {
            synchronized (owner) {
                owner.entries = null;
                owner.reservation.close();
            }
        }
    }

    /**
     * Trusted synchronous preparation only. Buffers are borrowed read-only views,
     * not owned copies; neither they nor decoded objects using them may escape the
     * callback without a separate reservation. Closing this view releases no bytes.
     */
    static final class View implements AutoCloseable {
        private Map<Key, byte[]> bodies;
        private boolean closed;
        private View(List<Entry> entries) {
            var bodies = new HashMap<Key, byte[]>();
            for (var entry : entries) bodies.put(entry.key(), entry.body());
            this.bodies = Map.copyOf(bodies);
        }
        synchronized Set<Key> keys() { requireOpen(); return bodies.keySet(); }
        synchronized java.nio.ByteBuffer bytes(Key key) {
            requireOpen();
            var body = bodies.get(Objects.requireNonNull(key));
            if (body == null) throw new IllegalArgumentException("Upload slot is absent from preparation");
            return java.nio.ByteBuffer.wrap(body).asReadOnlyBuffer();
        }
        private void requireOpen() { if (closed) throw new IllegalStateException("Upload preparation view is closed"); }
        @Override public synchronized void close() { closed = true; bodies = Map.of(); }
    }
}
