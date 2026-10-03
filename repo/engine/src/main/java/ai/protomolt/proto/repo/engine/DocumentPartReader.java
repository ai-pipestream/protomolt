package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.container.ledger.DocumentPublicationLedger;
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
public final class DocumentPartReader {
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

    public DocumentPartReader(BackendResolver backends) { this(backends, 32); }

    /** Hosts share this reader across requests that consume the same provider resource budget. */
    public DocumentPartReader(BackendResolver backends, int maxConcurrentReads) {
        this(backends, maxConcurrentReads, 256L * 1024 * 1024);
    }

    /** Per-selection legacy reuse bound; returned buffers belong to the caller, not this budget. */
    public DocumentPartReader(BackendResolver backends, int maxConcurrentReads, long maxLegacyReuseBytes) {
        this.backends = Objects.requireNonNull(backends);
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
        var fragments = readFragments(publication, mask, chunkSets, control);
        control.check();
        try {
            T result = DocumentPartCodec.assemble(fragments.stream()
                    .map(PartObject::bytes).toList(), prototype);
            control.check();
            return result;
        } catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Published document fragments cannot be decoded",invalid);
        }
    }

    /**
     * Exact verified bytes in publication order, without protobuf decoding or reserialization.
     * Intended for carrying unchanged parts from an already-authorized snapshot into a new
     * attempt. The caller must still fence the source revision and access at publication.
     * This checks storage integrity, not schema validity. Returned byte arrays belong to the caller.
     */
    public List<PartObject> readFragments(
            DocumentPublicationLedger.Publication publication, Set<DocumentPart> mask,
            Set<String> chunkSets, RepositoryReadControl control) {
        control.check();
        var store=backends.resolve(publication.generation(),publication.profile());
        control.check();
        if (store==null) throw RepositoryErrors.failedPrecondition("Original document backend is unavailable");
        var wanted=publication.parts().stream().filter(p -> mask.isEmpty() || mask.contains(p.part()))
                .filter(p -> p.part()!=DocumentPart.DOCUMENT_PART_CHUNKS || chunkSets.isEmpty() || chunkSets.contains(p.subKey())).toList();
        return readFragments(store, publication.namespace(), wanted, control, false);
    }

    /**
     * Verify selected legacy PRESENT fragments without decoding or changing their bytes.
     * The caller must strictly parse and authorize the snapshot, establish that it has no managed publication,
     * resolve its drive, and fence the source revision and drive at publication. This is
     * not historical-backend recovery: legacy manifests retain no non-CORE provider versions.
     * Missing checksums are rejected; current bytes must match the recorded size and digest.
     * A recorded CORE version/ETag is honored; null or blank means unknown, never inferred.
     */
    public List<PartObject> readLegacyFragments(BlobStore store, String namespace,
            List<PartManifestEntry> selected, String coreVersion, String coreEtag, RepositoryReadControl control) {
        control.check();
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
        control.check();
        return readFragments(store, namespace, wanted, control, true);
    }

    private static String known(String identity) { return identity == null || identity.isBlank() ? null : identity; }

    private List<PartObject> readFragments(BlobStore store, String namespace,
            List<DocumentPublicationLedger.Part> wanted, RepositoryReadControl control, boolean legacy) {
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
                        control.check();
                        final int index=submitted++;
                        pending.add(completions.submit(() -> new Fragment(index,readBounded(store,namespace,wanted.get(index),control,legacy))));
                    }
                    int completed=0;
                    while (completed<wanted.size()) {
                        control.check();
                        var future=completions.poll(Math.max(1, Math.min(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(50),
                                control.remainingNanos())), java.util.concurrent.TimeUnit.NANOSECONDS);
                        if (future == null) continue;
                        control.check();
                        pending.remove(future);
                        var fragment=future.get();
                        fragments.set(fragment.index(),fragment.bytes());
                        completed++;
                        if (submitted<wanted.size()) {
                            final int index=submitted++;
                            pending.add(completions.submit(() -> new Fragment(index,readBounded(store,namespace,wanted.get(index),control,legacy))));
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
                // ExecutorService.close waits for provider calls even if they ignore interruption.
                // Cancel outstanding work without holding the caller until provider timeouts expire.
                // The host owns the borrowed client and must configure finite request timeouts.
                executor.shutdownNow();
            }
        }
        control.check();
        var result = new ArrayList<PartObject>(wanted.size());
        for (int i = 0; i < wanted.size(); i++) {
            var part = wanted.get(i);
            result.add(new PartObject(part.part(), part.subKey(), fragments.get(i), part.sha256()));
        }
        control.check();
        return List.copyOf(result);
    }

    private byte[] readBounded(BlobStore store, String namespace, DocumentPublicationLedger.Part part,
            RepositoryReadControl control, boolean legacy) {
        control.check();
        if (!readSlots.tryAcquire())
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                    "Concurrent document read capacity exhausted");
        try {
            control.check();
            return readPart(store, namespace, part, legacy);
        } finally {
            // A cancelled Future does not imply the provider stopped. Hold its slot until actual return.
            readSlots.release();
        }
    }

    private static byte[] readPart(BlobStore store, String namespace, DocumentPublicationLedger.Part part, boolean legacy) {
        BlobStore.GetResult result;
        try { result=store.get(namespace,part.key(),part.providerVersion()); }
        catch (BlobStore.BlobNotFoundException missing) {
            if (legacy) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Legacy partial-save source object is unavailable; retry as a full save", missing);
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Published document part is missing from its original backend",missing);
        }
        // Detach before measuring: the byte SPI does not promise ownership of its result buffer.
        byte[] bytes = result == null || result.data() == null ? null : result.data().clone();
        if (bytes==null || bytes.length!=part.size()
                || !DocumentPartCodec.sha256Hex(bytes).equals(part.sha256())
                || (part.providerVersion()!=null && !part.providerVersion().equals(result.versionId()))
                || (part.etag()!=null && !part.etag().equals(result.eTag())))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Document part disagrees with its published byte or provider identity");
        return bytes;
    }
}
