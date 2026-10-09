package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/** Owned serialized fragment copies for preparation that outlives borrowed upload/read bytes. */
final class DocumentPublicationFragments implements AutoCloseable {
    private final DocumentPublicationCommand command;
    private final PayloadBudget.Lease lease;
    private Map<String, Map<Integer, ByteString>> fragments;

    private DocumentPublicationFragments(DocumentPublicationCommand command, PayloadBudget.Lease lease,
            Map<String, Map<Integer, ByteString>> fragments) {
        this.command = command; this.lease = lease; this.fragments = Map.copyOf(fragments);
    }

    /**
     * Input maps and bytes must remain stable throughout capture and have their own
     * host reservation. Preflight the complete command before allocating payload copies.
     * This lease accounts only the copies here, not descriptors, proofs or decoded objects.
     * All private copies must match their declared hashes before capture returns.
     * Source pins remain the caller's responsibility. Schema, authorization and
     * physical-selection checks still belong to admission and commit.
     */
    static DocumentPublicationFragments capture(DocumentPublicationCommand command,
            Map<String, Map<Integer, ByteString>> supplied, PayloadBudget budget, Runnable control) {
        Objects.requireNonNull(command); Objects.requireNonNull(supplied); Objects.requireNonNull(budget);
        command.requireExecutionSupported();
        return captureChecked(command, supplied, budget, control);
    }

    /** Explicit historical path; references remain borrowed and grant no publication authority. */
    static DocumentPublicationFragments captureHistorical(DocumentPublicationCommand command,
            Map<String, Map<Integer, ByteString>> supplied,
            java.util.List<DocumentHistoricalReferenceAdmission.Prepared> historical,
            PayloadBudget budget, Runnable control) {
        Objects.requireNonNull(command); Objects.requireNonNull(supplied); Objects.requireNonNull(budget);
        active(control);
        var references = DocumentHistoricalReferenceAdmission.requireComplete(command, historical, control);
        var result = captureChecked(command, supplied, budget, control);
        try {
            // A Use may have closed during copying or hashing. Never expose that snapshot.
            DocumentHistoricalReferenceAdmission.requireComplete(command, references, control);
            return result;
        } catch (RuntimeException | Error failure) {
            result.close();
            throw failure;
        }
    }

    private static DocumentPublicationFragments captureChecked(DocumentPublicationCommand command,
            Map<String, Map<Integer, ByteString>> supplied, PayloadBudget budget, Runnable control) {
        active(control);
        var members = command.intent().getMembersList();
        if (supplied.size() != members.size()) throw new IllegalArgumentException("Fragment members differ from command");
        var expectedMembers = new HashSet<String>();
        var source = new HashMap<String, Map<Integer, ByteString>>();
        long total = 0;
        for (var member : members) {
            active(control);
            String id = member.getMemberId(); expectedMembers.add(id);
            var suppliedParts = supplied.get(id);
            if (suppliedParts == null || suppliedParts.size() > member.getPartsCount())
                throw new IllegalArgumentException("Fragment members or ordinals differ from command");
            var parts = Map.copyOf(suppliedParts);
            var expected = new HashSet<Integer>();
            for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                active(control);
                var part = member.getParts(ordinal);
                if (part.hasEmpty()) continue;
                expected.add(ordinal);
                var bytes = parts.get(ordinal);
                long size = switch (part.getContentCase()) {
                    case UPLOAD -> part.getUpload().getSizeBytes();
                    case REUSE -> part.getReuse().getObject().getSizeBytes();
                    case HISTORICAL_REUSE -> part.getHistoricalReuse().getObject().getSizeBytes();
                    default -> throw new IllegalArgumentException("Fragment has no declared payload");
                };
                if (bytes == null || bytes.size() != size)
                    throw new IllegalArgumentException("Fragment size or ordinal differs from command");
                total = Math.addExact(total, bytes.size());
            }
            if (!expected.equals(parts.keySet())) throw new IllegalArgumentException("Fragment ordinals differ from command");
            source.put(id, parts);
        }
        if (!expectedMembers.equals(supplied.keySet())) throw new IllegalArgumentException("Fragment members differ from command");
        active(control);
        final PayloadBudget.Lease lease;
        try { lease = budget.reserve(total); }
        catch (PayloadBudget.CapacityExceededException exhausted) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Publication fragment snapshot capacity exhausted");
        }
        boolean transferred = false;
        try {
            var copied = new HashMap<String, Map<Integer, ByteString>>();
            for (var member : members) {
                var parts = new HashMap<Integer, ByteString>();
                for (var entry : source.get(member.getMemberId()).entrySet()) {
                    active(control);
                    // The fresh array is exclusively owned here and never exposed or mutated.
                    // Avoid flattening a rope and then allocating a second copy of it.
                    var copy = com.google.protobuf.UnsafeByteOperations.unsafeWrap(entry.getValue().toByteArray());
                    var declaration = member.getParts(entry.getKey());
                    String digest = switch (declaration.getContentCase()) {
                        case UPLOAD -> declaration.getUpload().getSha256();
                        case REUSE -> declaration.getReuse().getObject().getSha256();
                        case HISTORICAL_REUSE -> declaration.getHistoricalReuse().getObject().getSha256();
                        default -> throw new IllegalArgumentException("Fragment has no declared payload");
                    };
                    if (!DocumentCommandContent.sha256(copy, () -> active(control)).equals(digest))
                        throw new IllegalArgumentException("Fragment hash differs from command declaration");
                    parts.put(entry.getKey(), copy);
                }
                copied.put(member.getMemberId(), Map.copyOf(parts));
            }
            active(control);
            var result = new DocumentPublicationFragments(command, lease, copied);
            transferred = true;
            return result;
        } finally {
            if (!transferred) lease.close();
        }
    }

    /** Borrowed immutable snapshots: close only after every candidate/proof consumer has finished. */
    synchronized Map<String, Map<Integer, ByteString>> fragments() { requireOpen(); return fragments; }
    synchronized DocumentPublicationCommand command() { requireOpen(); return command; }
    private void requireOpen() { if (fragments == null) throw new IllegalStateException("Publication fragments are closed"); }
    @Override public synchronized void close() {
        if (fragments == null) return;
        fragments = null; lease.close();
    }
    private static void active(Runnable control) {
        Objects.requireNonNull(control);
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Publication fragment capture interrupted");
        control.run();
    }
}
