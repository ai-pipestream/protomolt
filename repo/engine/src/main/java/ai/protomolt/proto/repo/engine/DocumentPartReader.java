package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.ledger.DocumentPublicationLedger;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPart;
import com.google.protobuf.Message;
import java.util.ArrayList;
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

    public DocumentPartReader(BackendResolver backends) { this(backends, 32); }

    /** Hosts share this reader across requests that consume the same provider resource budget. */
    public DocumentPartReader(BackendResolver backends, int maxConcurrentReads) {
        this.backends = Objects.requireNonNull(backends);
        if (maxConcurrentReads <= 0) throw new IllegalArgumentException("Concurrent read limit must be positive");
        this.maxConcurrentReads = maxConcurrentReads;
        this.readSlots = new java.util.concurrent.Semaphore(maxConcurrentReads);
    }

    public <T extends Message> T read(DocumentPublicationLedger.Publication publication,
            Set<DocumentPart> mask, Set<String> chunkSets, T prototype) {
        return read(publication, mask, chunkSets, prototype, RepositoryReadControl.NONE);
    }

    public <T extends Message> T read(DocumentPublicationLedger.Publication publication,
            Set<DocumentPart> mask, Set<String> chunkSets, T prototype, RepositoryReadControl control) {
        control.check();
        var store=backends.resolve(publication.generation(),publication.profile());
        control.check();
        if (store==null) throw RepositoryErrors.failedPrecondition("Original document backend is unavailable");
        var wanted=publication.parts().stream().filter(p -> mask.isEmpty() || mask.contains(p.part()))
                .filter(p -> p.part()!=DocumentPart.DOCUMENT_PART_CHUNKS || chunkSets.isEmpty() || chunkSets.contains(p.subKey())).toList();
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
                        pending.add(completions.submit(() -> new Fragment(index,readBounded(store,publication.namespace(),wanted.get(index),control))));
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
                            pending.add(completions.submit(() -> new Fragment(index,readBounded(store,publication.namespace(),wanted.get(index),control))));
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
        try {
            T result = DocumentPartCodec.assemble(fragments,prototype);
            control.check();
            return result;
        }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Published document fragments cannot be decoded",invalid);
        }
    }

    private byte[] readBounded(BlobStore store, String namespace, DocumentPublicationLedger.Part part,
            RepositoryReadControl control) {
        control.check();
        if (!readSlots.tryAcquire())
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                    "Concurrent document read capacity exhausted");
        try {
            control.check();
            return readPart(store, namespace, part);
        } finally {
            // A cancelled Future does not imply the provider stopped. Hold its slot until actual return.
            readSlots.release();
        }
    }

    private static byte[] readPart(BlobStore store, String namespace, DocumentPublicationLedger.Part part) {
        BlobStore.GetResult result;
        try { result=store.get(namespace,part.key(),part.providerVersion()); }
        catch (BlobStore.BlobNotFoundException missing) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Published document part is missing from its original backend",missing);
        }
        if (result==null || result.data()==null || result.data().length!=part.size()
                || !DocumentPartCodec.sha256Hex(result.data()).equals(part.sha256())
                || (part.providerVersion()!=null && !part.providerVersion().equals(result.versionId()))
                || (part.etag()!=null && !part.etag().equals(result.eTag())))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Document part disagrees with its published byte or provider identity");
        return result.data();
    }
}
