package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.container.ledger.DocumentPublicationLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentReadLedger;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Original-backend reads of an already-authorized, published document snapshot. */
public final class DocumentPartReader implements AutoCloseable,
        ai.protomolt.proto.repo.container.ledger.DocumentRetainedReader,
        ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRetainedReader,
        ai.protomolt.proto.repo.container.ledger.DocumentAssessmentReader,
        ai.protomolt.proto.repo.container.ledger.DocumentReadLifecycle.Reader {
    @FunctionalInterface
    public interface BackendResolver {
        /**
         * Nonblocking lookup of the exact original backend. Client creation and
         * remote discovery belong to host setup, outside the read. Never
         * substitute the current drive or close the borrowed client.
         */
        BlobStore resolve(String generation, ManagedBackendLedger.Profile profile);
    }
    private final BackendResolver backends;
    private final java.util.concurrent.Semaphore readSlots;
    private final int maxConcurrentReads;
    private final long maxLegacyReuseBytes;
    private final PayloadBudget payloadBudget;
    private final Object lifecycle = new Object();
    private boolean closed;
    private int operations;
    private int workers;

    @Override public void close() {
        synchronized (lifecycle) { closed = true; lifecycle.notifyAll(); }
    }

    /**
     * After close, wait for resolver/read operations and actual provider workers.
     * False means the borrowed backend must remain open. Returned batches retain
     * their payload leases independently; this barrier does not close caller batches.
     */
    public boolean awaitIdle(java.time.Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Drain timeout must not be negative");
        long remaining = timeout.toNanos();
        long started = System.nanoTime();
        synchronized (lifecycle) {
            if (!closed) throw new IllegalStateException("Close the reader before awaiting idle");
            while (operations != 0 || workers != 0) {
                if (remaining <= 0) return false;
                java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
                remaining = timeout.toNanos() - (System.nanoTime() - started);
            }
            return true;
        }
    }

    private void checkActive(RepositoryReadControl control) {
        control.check();
        synchronized (lifecycle) {
            if (closed) throw new RepositoryException(RepositoryException.Code.CANCELLED, "Document reader is closed");
        }
    }

    private void enterOperation() {
        synchronized (lifecycle) {
            if (closed) throw new RepositoryException(RepositoryException.Code.CANCELLED, "Document reader is closed");
            operations++;
        }
    }

    private void exitOperation() {
        synchronized (lifecycle) { operations--; lifecycle.notifyAll(); }
    }

    public DocumentPartReader(BackendResolver backends) { this(backends, 32); }

    /** Hosts share this reader across requests that consume the same provider resource budget. */
    public DocumentPartReader(BackendResolver backends, int maxConcurrentReads) {
        this(backends, maxConcurrentReads, 256L * 1024 * 1024);
    }

    /** Per-selection legacy reuse bound with a private active-payload budget. */
    public DocumentPartReader(BackendResolver backends, int maxConcurrentReads, long maxLegacyReuseBytes) {
        this(backends, maxConcurrentReads, maxLegacyReuseBytes, new PayloadBudget(256L * 1024 * 1024));
    }

    /** Borrow the host budget also supplied to writers; returned batches own reservations until closed. */
    public DocumentPartReader(BackendResolver backends, int maxConcurrentReads, long maxLegacyReuseBytes,
            PayloadBudget payloadBudget) {
        this.backends = Objects.requireNonNull(backends);
        this.payloadBudget = Objects.requireNonNull(payloadBudget);
        if (maxConcurrentReads <= 0) throw new IllegalArgumentException("Concurrent read limit must be positive");
        if (maxLegacyReuseBytes <= 0) throw new IllegalArgumentException("Legacy reuse byte limit must be positive");
        this.maxLegacyReuseBytes = maxLegacyReuseBytes;
        this.maxConcurrentReads = maxConcurrentReads;
        this.readSlots = new java.util.concurrent.Semaphore(maxConcurrentReads);
    }

    public <T extends Message> T read(DocumentPublicationLedger.Publication publication,
            Set<DocumentPart> mask, Set<String> chunkSets, T prototype) {
        return read(publication, mask, chunkSets, prototype, RepositoryReadControl.NONE);
    }

    public <T extends Message> T read(DocumentPublicationLedger.Publication publication,
            Set<DocumentPart> mask, Set<String> chunkSets, T prototype, RepositoryReadControl control) {
        try (var batch = readFragments(publication, mask, chunkSets, control)) {
            checkActive(control);
            T result = DocumentPartCodec.assemble(batch.parts().stream()
                    .map(PartObject::bytes).toList(), prototype);
            checkActive(control);
            return result;
        } catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Published document fragments cannot be decoded",invalid);
        }
    }

    /**
     * Exact verified bytes in publication order, without protobuf decoding or reserialization.
     * Intended for carrying unchanged parts from an already-authorized snapshot into a new
     * attempt. The caller must still fence the source revision and access at publication.
     * This checks storage integrity, not schema validity. Close the returned batch only
     * after finishing with its bytes, including any source reuse in a writer.
     */
    public DocumentReadBatch readFragments(
            DocumentPublicationLedger.Publication publication, Set<DocumentPart> mask,
            Set<String> chunkSets, RepositoryReadControl control) {
        enterOperation();
        try { return readPublicationFragments(publication, mask, chunkSets, control); }
        finally { exitOperation(); }
    }

    private DocumentReadBatch readPublicationFragments(
            DocumentPublicationLedger.Publication publication, Set<DocumentPart> mask,
            Set<String> chunkSets, RepositoryReadControl control) {
        checkActive(control);
        var wanted=publication.boundParts().stream().filter(p -> mask.isEmpty() || mask.contains(p.part().part()))
                .filter(p -> p.part().part()!=DocumentPart.DOCUMENT_PART_CHUNKS || chunkSets.isEmpty() || chunkSets.contains(p.part().subKey())).toList();
        return readBoundParts(wanted, control);
    }

    private record ResolvedPart(DocumentPublicationLedger.Part part, BlobStore store, String namespace) {}

    /**
     * Read one member's retained inputs from ledger-issued command evidence.
     * Results correspond positionally to that member's filtered plan entries in full revision order;
     * omitted upload/EMPTY ordinals are not renumbered in the plan. This does not
     * decode content or publish a revision. The host must protect source objects
     * through actual I/O completion and re-fence policy/revisions afterwards;
     * the plan alone is not a reader lease. Keep the returned batch open while
     * using its bytes, including during assembly.
     */
    public DocumentReadBatch readRetained(
            ai.protomolt.proto.repo.container.ledger.DocumentRetainedReadPlan plan,
            String memberId, RepositoryReadControl control) {
        return readRetained(plan, memberId, control, null);
    }

    /**
     * Keeps the ledger-issued protection through setup, actual provider completion
     * and returned batch ownership. The coordinator closes the plan, awaits drain
     * and releases its SQL pins separately. Cancellation does not imply drain.
     */
    @Override public DocumentReadBatch readRetained(DocumentReadLedger.PinnedPlan plan,
            String memberId, RepositoryReadControl control) {
        try (var setup = Objects.requireNonNull(plan).use()) {
            return readRetained(setup.plan(), memberId, control, setup);
        }
    }

    private DocumentReadBatch readRetained(
            ai.protomolt.proto.repo.container.ledger.DocumentRetainedReadPlan plan,
            String memberId, RepositoryReadControl control, DocumentReadLedger.PinnedPlan.Use protection) {
        Objects.requireNonNull(plan); Objects.requireNonNull(memberId);
        enterOperation();
        try {
            checkActive(control);
            if (plan.command().intent().getMembersList().stream().noneMatch(m -> m.getMemberId().equals(memberId)))
                throw new IllegalArgumentException("Member is absent from retained read command");
            var wanted = new ArrayList<DocumentPublicationLedger.BoundPart>();
            for (var entry : plan.entries()) {
                checkActive(control);
                if (!entry.memberId().equals(memberId)) continue;
                var object = entry.source().getObject();
                var slot = entry.destinationSlot();
                wanted.add(new DocumentPublicationLedger.BoundPart(new DocumentPublicationLedger.Part(
                        slot.getPart(), slot.getSubKey(), object.getObjectKey(), object.getSizeBytes(), object.getSha256(),
                        object.hasProviderVersion() ? object.getProviderVersion() : null, null, object.getContentType()),
                        entry.binding()));
            }
            return readFragments(wanted.stream().map(DocumentPublicationLedger.BoundPart::part).toList(),
                    () -> resolveBoundParts(wanted, control), control, false, protection);
        } finally { exitOperation(); }
    }

    /** Retained candidate reads; returned fragments have no publication or semantic-review authority. */
    @Override public DocumentReadBatch readAssessment(DocumentReadLedger.PinnedAssessment assessment,
            String member, RepositoryReadControl control) {
        Objects.requireNonNull(assessment); Objects.requireNonNull(member); Objects.requireNonNull(control);
        enterOperation();
        DocumentReadBatch batch = null;
        boolean delivered = false;
        // The setup use transfers to workers/batch. A separate admitted use stays
        // here so failed I/O can still be authorized after its batch has closed.
        try (var delivery = assessment.use(); var setup = assessment.use()) {
            try {
                checkActive(control);
                var wanted = setup.plan().entries(member).stream().map(entry -> entry.part()).toList();
                batch = readFragments(wanted.stream().map(DocumentPublicationLedger.BoundPart::part).toList(),
                        () -> resolveBoundParts(wanted, control), control, false, setup);
                checkActive(control);
            } catch (java.util.concurrent.CancellationException cancelled) {
                throw new RepositoryException(RepositoryException.Code.CANCELLED, "Assessment read cancelled");
            } catch (RuntimeException failure) {
                if (failure instanceof RepositoryException repository
                        && (repository.code() == RepositoryException.Code.CANCELLED
                            || repository.code() == RepositoryException.Code.DEADLINE_EXCEEDED))
                    throw new RepositoryException(repository.code(), "Assessment read cancelled or expired");
                control.check();
                assessment.authorizeDelivery(delivery, control);
                throw failure;
            }
            assessment.authorizeDelivery(delivery, control);
            delivered = true;
            return batch;
        } finally {
            if (!delivered && batch != null) batch.close();
            exitOperation();
        }
    }

    /**
     * Reads original historical provider versions without protobuf decoding. The
     * capture-bound caller is reauthorized before bytes or detailed failures leave
     * this method. Keep the returned batch open while using its bytes; the host
     * closes, drains and releases the plan separately, including after cancellation.
     */
    public DocumentReadBatch readHistorical(DocumentReadLedger.PinnedHistory history, RepositoryReadControl control) {
        return readHistorical(history, java.util.OptionalInt.empty(), control);
    }

    /** Read only one complete-revision ordinal, using the same provider identity and delivery checks. */
    @Override
    public DocumentReadBatch readHistorical(DocumentReadLedger.PinnedHistory history, int ordinal, RepositoryReadControl control) {
        return readHistorical(history, java.util.OptionalInt.of(ordinal), control);
    }

    private DocumentReadBatch readHistorical(DocumentReadLedger.PinnedHistory history, java.util.OptionalInt ordinal,
            RepositoryReadControl control) {
        Objects.requireNonNull(history); Objects.requireNonNull(control);
        enterOperation();
        DocumentReadBatch batch = null;
        boolean delivered = false;
        try (var setup = history.use()) {
            try {
                checkActive(control);
                var entries = setup.plan().entries().stream()
                        .filter(entry -> ordinal.isEmpty() || entry.revisionOrdinal() == ordinal.getAsInt()).toList();
                if (ordinal.isPresent() && entries.isEmpty())
                    throw new RepositoryException(RepositoryException.Code.NOT_FOUND, "Historical fragment is unavailable");
                if (ordinal.isPresent() && entries.size() != 1)
                    throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical fragment ordinal is ambiguous");
                var wanted = entries.stream().map(entry -> entry.part()).toList();
                batch = readFragments(wanted.stream().map(DocumentPublicationLedger.BoundPart::part).toList(),
                        () -> resolveBoundParts(wanted, control), control, false, setup);
                checkActive(control);
            } catch (java.util.concurrent.CancellationException cancelled) {
                throw new RepositoryException(RepositoryException.Code.CANCELLED, "Historical document read cancelled");
            } catch (RuntimeException failure) {
                // Cancellation has no result to authorize. Do not start JDBC on
                // an interrupted caller or disclose provider exception details.
                if (failure instanceof RepositoryException repository
                        && (repository.code() == RepositoryException.Code.CANCELLED
                            || repository.code() == RepositoryException.Code.DEADLINE_EXCEEDED))
                    throw new RepositoryException(repository.code(), "Historical document read cancelled or expired");
                authorizeHistoricalDelivery(history, control);
                throw failure;
            }
            authorizeHistoricalDelivery(history, control);
            delivered = true;
            return batch;
        } finally {
            if (!delivered && batch != null) batch.close();
            exitOperation();
        }
    }

    private static void authorizeHistoricalDelivery(DocumentReadLedger.PinnedHistory history, RepositoryReadControl control) {
        try { history.authorizeDelivery(control); }
        catch (java.util.concurrent.CancellationException cancelled) {
            throw new RepositoryException(RepositoryException.Code.CANCELLED, "Historical document read cancelled");
        } catch (RepositoryException failure) {
            if (failure.code() == RepositoryException.Code.CANCELLED || failure.code() == RepositoryException.Code.DEADLINE_EXCEEDED)
                throw new RepositoryException(failure.code(), "Historical document read cancelled or expired");
            throw failure;
        }
    }

    private DocumentReadBatch readBoundParts(List<DocumentPublicationLedger.BoundPart> wanted, RepositoryReadControl control) {
        return readFragments(wanted.stream().map(DocumentPublicationLedger.BoundPart::part).toList(),
                () -> resolveBoundParts(wanted,control),control,false);
    }
    private List<ResolvedPart> resolveBoundParts(List<DocumentPublicationLedger.BoundPart> wanted, RepositoryReadControl control) {
        record Backend(String generation, ManagedBackendLedger.Profile profile) {}
        var stores = new java.util.HashMap<Backend, BlobStore>();
        var resolved = new ArrayList<ResolvedPart>(wanted.size());
        for (var selected : wanted) {
            checkActive(control);
            var binding = selected.binding();
            var backend = new Backend(binding.generation(), binding.profile());
            var store = stores.get(backend);
            if (store == null) {
                store = backends.resolve(backend.generation(), backend.profile());
                checkActive(control);
                if (store == null) throw RepositoryErrors.failedPrecondition("Original document backend is unavailable");
                stores.put(backend, store);
            }
            resolved.add(new ResolvedPart(selected.part(), store, binding.namespace()));
        }
        return resolved;
    }

    /**
     * Read selected fragments from a captured physical source fence. Managed sources
     * use their retained publication and original-backend resolver; the legacy store
     * argument is ignored. Legacy sources use the captured namespace and supplied
     * host-qualified legacy store, without re-reading current drive configuration.
     * The caller must strictly parse and authorize the snapshot, establish that it has no managed publication,
     * resolve its drive, and fence the source revision and drive at publication. This is
     * not historical-backend recovery: legacy manifests retain no non-CORE provider versions.
     * Missing checksums are rejected; current bytes must match the recorded size and digest.
     * A recorded CORE version/ETag is honored; null or blank means unknown, never inferred.
     */
    public DocumentReadBatch readSource(ai.protomolt.proto.repo.container.ledger.DocumentSourceSnapshot source,
            BlobStore qualifiedLegacyStore, Set<DocumentPart> mask, Set<String> chunkSets, RepositoryReadControl control) {
        Objects.requireNonNull(source);
        if (!source.legacy())
            return readFragments(source.publication().orElseThrow(), mask, chunkSets, control);
        var selected = source.manifest().getPartsList().stream()
                .filter(p -> p.getState() == PartState.PART_STATE_PRESENT)
                .filter(p -> mask.isEmpty() || mask.contains(p.getPart()))
                .filter(p -> p.getPart() != DocumentPart.DOCUMENT_PART_CHUNKS
                        || chunkSets.isEmpty() || chunkSets.contains(p.getSubKey())).toList();
        return readLegacyFragments(Objects.requireNonNull(qualifiedLegacyStore, "Qualified legacy source store"),
                source.legacyNamespace(), selected, source.coreVersion(), source.coreEtag(), control);
    }

    /** Internal partial-save selection, including no reused objects, from the captured source. */
    record PartSlot(DocumentPart part, String subKey) {}
    static List<DocumentPublicationLedger.BoundPart> selectSlots(DocumentPublicationLedger.Publication publication, Set<PartSlot> slots) {
        var wanted=publication.boundParts().stream()
                .filter(p -> slots.contains(new PartSlot(p.part().part(),p.part().subKey()))).toList();
        // Publication's immutable constructor rejects duplicate slots, so this
        // subset has exact membership only when its cardinality matches.
        if (wanted.size()!=slots.size()) throw RepositoryErrors.failedPrecondition("Source slots differ from retained publication");
        return wanted;
    }
    DocumentReadBatch readSourceSlots(ai.protomolt.proto.repo.container.ledger.DocumentSourceSnapshot source,
            BlobStore qualifiedLegacyStore, Set<PartSlot> slots, RepositoryReadControl control) {
        enterOperation();
        try {
            checkActive(control);
            var selected = source.manifest().getPartsList().stream()
                    .filter(p -> p.getState() == PartState.PART_STATE_PRESENT && slots.contains(new PartSlot(p.getPart(),p.getSubKey()))).toList();
            if (selected.size() != slots.size()) throw RepositoryErrors.failedPrecondition("Source selection differs from captured manifest");
            if (source.legacy()) return readLegacySelection(qualifiedLegacyStore, source.legacyNamespace(), selected,
                    source.coreVersion(), source.coreEtag(), control);
            var publication = source.publication().orElseThrow();
            return readBoundParts(selectSlots(publication,slots), control);
        } finally { exitOperation(); }
    }

    /** Low-level legacy read; callers supplying a source fence should prefer readSource. */
    public DocumentReadBatch readLegacyFragments(BlobStore store, String namespace,
            List<PartManifestEntry> selected, String coreVersion, String coreEtag, RepositoryReadControl control) {
        enterOperation();
        try { return readLegacySelection(store, namespace, selected, coreVersion, coreEtag, control); }
        finally { exitOperation(); }
    }

    private DocumentReadBatch readLegacySelection(BlobStore store, String namespace,
            List<PartManifestEntry> selected, String coreVersion, String coreEtag, RepositoryReadControl control) {
        checkActive(control);
        Objects.requireNonNull(store, "store");
        if (namespace == null || namespace.isBlank())
            throw RepositoryErrors.failedPrecondition("Legacy document namespace is missing");
        var entries = List.copyOf(selected);
        if (entries.size() > 10000) throw RepositoryErrors.failedPrecondition("Legacy document has too many selected parts");
        var slots = new java.util.HashSet<java.util.Map.Entry<DocumentPart, String>>();
        var keys = new java.util.HashSet<String>();
        var wanted = new ArrayList<DocumentPublicationLedger.Part>(entries.size());
        long total = 0;
        for (var part : entries) {
            if (part.getState() != PartState.PART_STATE_PRESENT || part.getPart() == DocumentPart.UNRECOGNIZED
                    || part.getPart() == DocumentPart.DOCUMENT_PART_UNSPECIFIED || part.getObjectKey().isBlank()
                    || part.getSizeBytes() < 0 || !part.getSha256().matches("[0-9a-f]{64}")
                    || (part.getPart() == DocumentPart.DOCUMENT_PART_CHUNKS ? part.getSubKey().isBlank() : !part.getSubKey().isEmpty())
                    || !part.getDeletedReason().isEmpty()
                    || !slots.add(java.util.Map.entry(part.getPart(), part.getSubKey())) || !keys.add(part.getObjectKey()))
                throw RepositoryErrors.failedPrecondition("Legacy document lacks an unambiguous measured part identity");
            if (part.getSizeBytes() > maxLegacyReuseBytes - total)
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Legacy reuse selection exceeds byte limit");
            total += part.getSizeBytes();
            boolean core = part.getPart() == DocumentPart.DOCUMENT_PART_CORE;
            wanted.add(new DocumentPublicationLedger.Part(part.getPart(), part.getSubKey(), part.getObjectKey(),
                    part.getSizeBytes(), part.getSha256(), core ? known(coreVersion) : null, core ? known(coreEtag) : null));
        }
        checkActive(control);
        return readFragments(store, namespace, wanted, control, true);
    }

    private static String known(String identity) { return identity == null || identity.isBlank() ? null : identity; }

    private DocumentReadBatch readFragments(BlobStore store, String namespace,
            List<DocumentPublicationLedger.Part> wanted, RepositoryReadControl control, boolean legacy) {
        return readFragments(wanted, () -> wanted.stream().map(part -> new ResolvedPart(part, store, namespace)).toList(), control, legacy);
    }

    private DocumentReadBatch readFragments(List<DocumentPublicationLedger.Part> parts,
            java.util.function.Supplier<List<ResolvedPart>> resolve, RepositoryReadControl control, boolean legacy) {
        return readFragments(parts, resolve, control, legacy, null);
    }

    private DocumentReadBatch readFragments(List<DocumentPublicationLedger.Part> parts,
            java.util.function.Supplier<List<ResolvedPart>> resolve, RepositoryReadControl control, boolean legacy,
            DocumentReadLedger.PinnedRead<?>.Use protection) {
        checkActive(control);
        long total = 0;
        for (var part : parts) {
            if (part.size() < 0) throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Negative document part size");
            if (part.size() > Integer.MAX_VALUE || part.size() > Long.MAX_VALUE / 2 - total)
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Document selection exceeds read capacity");
            total += part.size();
        }
        DocumentReadBatch batch;
        try { batch = new DocumentReadBatch(payloadBudget.reserve(total * 2), protection); }
        catch (PayloadBudget.CapacityExceededException exhausted) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Document payload capacity exhausted", exhausted);
        }
        boolean complete = false;
        try {
        // Reserve before any resolver work; failures close the same batch below.
        var wanted=resolve.get();
        var fragments=new ArrayList<byte[]>(java.util.Collections.nCopies(wanted.size(),null));
        if (!wanted.isEmpty()) {
            int parallelism = Math.min(Math.min(32, maxConcurrentReads), wanted.size());
            var executor=Executors.newFixedThreadPool(parallelism,Thread.ofVirtual().factory());
            try {
                record Fragment(int index, byte[] bytes) {}
                var completions=new java.util.concurrent.ExecutorCompletionService<Fragment>(executor);
                var pending=new java.util.HashSet<Future<Fragment>>();
                try {
                    int submitted=0;
                    while (submitted<parallelism) {
                        checkActive(control);
                        final int index=submitted++;
                        pending.add(completions.submit(() -> {
                            var selected = wanted.get(index);
                            return new Fragment(index,readOwned(batch,selected.store(),selected.namespace(),selected.part(),control,legacy));
                        }));
                    }
                    int completed=0;
                    while (completed<wanted.size()) {
                        checkActive(control);
                        var future=completions.poll(Math.max(1, Math.min(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(50),
                                control.remainingNanos())), java.util.concurrent.TimeUnit.NANOSECONDS);
                        if (future == null) continue;
                        checkActive(control);
                        pending.remove(future);
                        var fragment=future.get();
                        fragments.set(fragment.index(),fragment.bytes());
                        completed++;
                        if (submitted<wanted.size()) {
                            final int index=submitted++;
                            pending.add(completions.submit(() -> {
                                var selected = wanted.get(index);
                                return new Fragment(index,readOwned(batch,selected.store(),selected.namespace(),selected.part(),control,legacy));
                            }));
                        }
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RepositoryException(RepositoryException.Code.CANCELLED,"Document read was interrupted",interrupted);
                } catch (ExecutionException failed) {
                    if (failed.getCause() instanceof RuntimeException runtime) throw runtime;
                    if (failed.getCause() instanceof Error error) throw error;
                    throw new IllegalStateException("Document part read failed",failed.getCause());
                } finally { for (var future : pending) if (!future.isDone()) future.cancel(true); }
            } finally {
                // shutdownNow requests cancellation; it does not wait for provider completion.
                // The batch retains its reservation until entered workers actually return.
                // Cancel outstanding work without holding the caller until provider timeouts expire.
                // The host owns the borrowed client and must configure finite request timeouts.
                executor.shutdownNow();
            }
        }
        checkActive(control);
        var result = new ArrayList<PartObject>(wanted.size());
        for (int i = 0; i < wanted.size(); i++) {
            var part = wanted.get(i).part();
            result.add(new PartObject(part.part(), part.subKey(), fragments.get(i), part.sha256()));
        }
        checkActive(control);
        batch.complete(result);
        complete = true;
        return batch;
        } finally { if (!complete) batch.close(); }
    }

    private byte[] readOwned(DocumentReadBatch batch, BlobStore store, String namespace,
            DocumentPublicationLedger.Part part, RepositoryReadControl control, boolean legacy) {
        synchronized (lifecycle) {
            if (closed) throw new RepositoryException(RepositoryException.Code.CANCELLED, "Document reader is closed");
            workers++;
        }
        try {
            batch.enterWorker();
            try { return readBounded(store, namespace, part, control, legacy); }
            finally { batch.exitWorker(); }
        } finally {
            synchronized (lifecycle) { workers--; lifecycle.notifyAll(); }
        }
    }

    private byte[] readBounded(BlobStore store, String namespace, DocumentPublicationLedger.Part part,
            RepositoryReadControl control, boolean legacy) {
        checkActive(control);
        if (!readSlots.tryAcquire())
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                    "Concurrent document read capacity exhausted");
        try {
            checkActive(control);
            return readPart(store, namespace, part, legacy);
        } finally {
            // A cancelled Future does not imply the provider stopped. Hold its slot until actual return.
            readSlots.release();
        }
    }

    private static byte[] readPart(BlobStore store, String namespace, DocumentPublicationLedger.Part part, boolean legacy) {
        if (part.size() < 0)
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Document part has a negative recorded size");
        if (part.size() > Integer.MAX_VALUE)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Document part exceeds the byte-array read limit");
        BlobStore.GetResult result;
        try { result=store.getBounded(namespace,part.key(),part.providerVersion(), (int) part.size()); }
        catch (BlobStore.BlobReadLimitException oversized) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                    "Document part exceeds its recorded size", oversized);
        } catch (UnsupportedOperationException unsupported) {
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Document backend does not support bounded reads", unsupported);
        }
        catch (BlobStore.BlobNotFoundException missing) {
            if (legacy) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Legacy partial-save source object is unavailable; retry as a full save", missing);
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Published document part is missing from its original backend",missing);
        }
        // Detach before measuring: the byte SPI does not promise ownership of its result buffer.
        byte[] bytes = result == null || result.data() == null ? null : result.data().clone();
        if (bytes==null || bytes.length!=part.size()
                || !DocumentPartCodec.sha256Hex(bytes).equals(part.sha256())
                || (part.contentType()!=null && !part.contentType().equals(result.contentType()))
                || (part.providerVersion()!=null && !part.providerVersion().equals(result.versionId()))
                || (part.etag()!=null && !part.etag().equals(result.eTag())))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Document part disagrees with its published byte or provider identity");
        return bytes;
    }
}
