package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Borrowed complete inputs for synchronous candidate capture, with protected retained reads. */
final class DocumentPublicationInputs implements AutoCloseable {
    private final DocumentReadLedger.PinnedPlan.Use use;
    private final List<DocumentRetainedReader.Batch> batches;
    private Map<String, Map<Integer, ByteString>> fragments;

    private DocumentPublicationInputs(DocumentReadLedger.PinnedPlan.Use use,
            List<DocumentRetainedReader.Batch> batches, Map<String, Map<Integer, ByteString>> fragments) {
        this.use = use; this.batches = new ArrayList<>(batches); this.fragments = Map.copyOf(fragments);
    }

    /**
     * The upload view must stay open throughout use. Returned bytes borrow both that
     * view and retained batches; copy them into an owned candidate before close.
     * This owner releases local uses only. The coordinator separately closes/drains
     * the pinned plan and releases its SQL pins, including after a failed capture.
     */
    static DocumentPublicationInputs capture(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner,
            DocumentUploadPayloads.View uploads,
            DocumentReadLedger.PinnedPlan pinned, DocumentRetainedReader reader, RepositoryReadControl control) {
        Objects.requireNonNull(command); Objects.requireNonNull(owner); Objects.requireNonNull(uploads); Objects.requireNonNull(reader);
        Objects.requireNonNull(control).check();
        var use = Objects.requireNonNull(pinned).use();
        var batches = new ArrayList<DocumentRetainedReader.Batch>();
        boolean transferred = false;
        Throwable primary = null;
        try {
            var plan = use.plan();
            if (!plan.command().operationId().equals(command.operationId())
                    || !plan.command().canonical().equals(command.canonical())
                    || !owner.key().operationId().equals(command.operationId())
                    || !owner.key().account().equals(command.intent().getAccountId())
                    || !plan.principal().equals(owner.key().principal()) || plan.generation() != owner.generation())
                throw new IllegalArgumentException("Retained read plan differs from publication command or owner");
            var expectedUploads = new HashSet<DocumentUploadPayloads.Key>();
            var expectedReuse = new HashMap<DocumentUploadPayloads.Key, ai.protomolt.proto.repo.v1.DocumentPublicationPart>();
            var fragments = new HashMap<String, Map<Integer, ByteString>>();
            for (var member : command.intent().getMembersList()) {
                control.check();
                var parts = new HashMap<Integer, ByteString>();
                for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                    var part = member.getParts(ordinal);
                    var key = new DocumentUploadPayloads.Key(member.getMemberId(), ordinal);
                    if (part.hasUpload()) expectedUploads.add(key);
                    else if (part.hasReuse()) expectedReuse.put(key, part);
                }
                fragments.put(member.getMemberId(), parts);
            }
            if (!uploads.keys().equals(expectedUploads))
                throw new IllegalArgumentException("Upload inputs differ from complete command");
            var byMember = new HashMap<String, List<DocumentRetainedReadPlan.Entry>>();
            var seen = new HashSet<DocumentUploadPayloads.Key>();
            for (var entry : plan.entries()) {
                control.check();
                var key = new DocumentUploadPayloads.Key(entry.memberId(), entry.revisionOrdinal());
                var part = expectedReuse.get(key);
                if (part == null || !seen.add(key) || !part.getSlot().equals(entry.destinationSlot())
                        || !part.getReuse().equals(entry.source()))
                    throw new IllegalArgumentException("Retained read entries differ from command reuse slots");
                byMember.computeIfAbsent(entry.memberId(), ignored -> new ArrayList<>()).add(entry);
            }
            if (!seen.equals(expectedReuse.keySet()))
                throw new IllegalArgumentException("Retained read entries omit command reuse slots");
            for (var key : expectedUploads) {
                control.check();
                fragments.get(key.member()).put(key.revisionOrdinal(), UnsafeByteOperations.unsafeWrap(uploads.bytes(key)));
            }
            for (var member : command.intent().getMembersList()) {
                var entries = byMember.getOrDefault(member.getMemberId(), List.of());
                if (entries.isEmpty()) continue;
                control.check();
                var batch = Objects.requireNonNull(reader.readRetained(pinned, member.getMemberId(), control));
                try { batches.add(batch); }
                catch (RuntimeException | Error failure) { batch.close(); throw failure; }
                var parts = batch.parts();
                if (parts.size() != entries.size()) throw new IllegalArgumentException("Retained batch has wrong part count");
                for (int i = 0; i < entries.size(); i++) {
                    control.check();
                    var entry = entries.get(i);
                    var part = parts.get(i);
                    var object = entry.source().getObject();
                    if (part == null || part.bytes() == null || part.part() != entry.destinationSlot().getPart()
                            || !Objects.equals(part.subKey(), entry.destinationSlot().getSubKey())
                            || part.bytes().length != object.getSizeBytes() || !Objects.equals(part.sha256(), object.getSha256()))
                        throw new IllegalArgumentException("Retained batch part differs from selected source");
                    fragments.get(member.getMemberId()).put(entry.revisionOrdinal(), UnsafeByteOperations.unsafeWrap(part.bytes()));
                }
            }
            fragments.replaceAll((member, parts) -> Map.copyOf(parts));
            control.check();
            var result = new DocumentPublicationInputs(use, batches, fragments);
            transferred = true;
            return result;
        } catch (RuntimeException | Error failure) {
            primary = failure;
            throw failure;
        } finally {
            if (!transferred) {
                try { closeResources(batches, use); }
                catch (RuntimeException | Error cleanup) {
                    if (primary == null) throw cleanup;
                    if (cleanup != primary) primary.addSuppressed(cleanup);
                }
            }
        }
    }

    synchronized Map<String, Map<Integer, ByteString>> fragments() {
        if (fragments == null) throw new IllegalStateException("Publication inputs are closed");
        return fragments;
    }
    @Override public synchronized void close() {
        if (fragments == null) return;
        fragments = null;
        try { closeResources(batches, use); }
        finally { batches.clear(); }
    }

    private static void closeResources(List<DocumentRetainedReader.Batch> batches, DocumentReadLedger.PinnedPlan.Use use) {
        try (use) {
            Throwable first = null;
            for (int i = batches.size() - 1; i >= 0; i--) {
                try { batches.get(i).close(); }
                catch (RuntimeException | Error failure) {
                    if (first == null) first = failure;
                    else if (failure != first) first.addSuppressed(failure);
                }
            }
            if (first instanceof RuntimeException failure) throw failure;
            if (first instanceof Error failure) throw failure;
        }
    }
}
