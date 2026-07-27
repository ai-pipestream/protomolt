package ai.pipestream.proto.repo.container.blob;

import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import ai.pipestream.proto.repo.v1.WriteProvenance;
import ai.pipestream.proto.repo.container.codec.DocumentPartCodec;
import ai.pipestream.proto.repo.container.codec.PartLayout;
import ai.pipestream.proto.repo.container.codec.PartObject;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The async-join IO layer of the four-part document store.
 *
 * <p>Descriptor-driven: any {@link Message} type decomposed by a
 * {@link PartLayout} splits, writes, copies and assembles through the same
 * code path — the layout names the part boundaries, this class only moves
 * bytes.
 *
 * <p><b>Assembly latency contract:</b> every multi-object read or write issues
 * its object operations CONCURRENTLY on virtual threads, so an all-parts
 * operation costs {@code max(part latencies)}, not the sum — the split is
 * latency-neutral for full-document callers and strictly cheaper for masked
 * ones.
 */
public class PartStorage {

    private static final Logger LOG = LoggerFactory.getLogger(PartStorage.class);

    /** One shared fan-out pool: each task is a blocking S3 call on its own virtual thread. */
    private static final ExecutorService FANOUT = Executors.newVirtualThreadPerTaskExecutor();

    /** Creates the part storage IO layer (stateless — nothing is injected). */
    public PartStorage() {
    }

    /**
     * Everything the row upsert needs to know about a completed part write.
     *
     * @param manifest the full part manifest (one entry per object, plus EMPTY entries)
     * @param rootChecksum the Merkle-style whole-document checksum (dedupe key)
     * @param totalSizeBytes summed size of all written part objects
     * @param coreEtag the CORE object's entity tag (the row's representative etag)
     * @param coreVersionId the CORE object's version id, or {@code null}
     * @param partObjectKeys every written object key (purge fan-out list)
     */
    public record WriteResult(DocumentManifest manifest, String rootChecksum, long totalSizeBytes,
                              String coreEtag, String coreVersionId, List<String> partObjectKeys) {
    }

    /**
     * Splits and writes a document's part objects under {@code basePrefix},
     * all PUTs in parallel, and returns the manifest describing them.
     *
     * @param store the blob store for the resolved drive
     * @param bucket the target bucket
     * @param basePrefix the address prefix ({@code …/{nodeId}}) the parts live under
     * @param doc the document to store
     * @param layout the part layout decomposing the document's type
     * @param docId the logical document id (manifest identity)
     * @param graphAddressId the stored coordinate (manifest identity)
     * @param accountId the owning account (manifest identity)
     * @param graphId the owning graph (manifest identity)
     * @param writtenBy writer provenance recorded on each entry (may be {@code null};
     *        when present it is stamped verbatim)
     * @param contentType the MIME type stored with each object
     * @param s3Metadata provider metadata stored with each object; may be {@code null}
     * @param verifyChecksums when {@code true} each PUT carries its part SHA-256
     *        so the store verifies the landed bytes (force-save semantics)
     * @param docVersion the per-address document version this write lands as
     * @return the write result feeding the row upsert
     */
    public WriteResult writeParts(BlobStore store, String bucket, String basePrefix, Message doc,
            PartLayout layout, String docId, String graphAddressId, String accountId, String graphId,
            WriteProvenance writtenBy, String contentType, Map<String, String> s3Metadata,
            boolean verifyChecksums, long docVersion) {
        List<PartObject> parts = DocumentPartCodec.split(doc, layout);
        return writePartObjects(store, bucket, basePrefix, parts, docId, graphAddressId, accountId, graphId,
                writtenBy, contentType, s3Metadata, verifyChecksums, docVersion);
    }

    /**
     * Writes pre-split part objects (the {@link #writeParts} body; also the
     * partial-save leg's writer for the supplied parts).
     *
     * @param store the blob store for the resolved drive
     * @param bucket the target bucket
     * @param basePrefix the address prefix the parts live under
     * @param parts the split fragments in manifest order
     * @param docId the logical document id
     * @param graphAddressId the stored coordinate
     * @param accountId the owning account
     * @param graphId the owning graph
     * @param writtenBy writer provenance for the entries (may be {@code null});
     *        when present it is stamped verbatim
     * @param contentType the MIME type stored with each object
     * @param s3Metadata provider metadata; may be {@code null}
     * @param verifyChecksums when {@code true} PUTs carry part SHA-256s
     * @param docVersion the per-address document version this write lands as
     * @return the write result feeding the row upsert
     */
    public WriteResult writePartObjects(BlobStore store, String bucket, String basePrefix,
            List<PartObject> parts, String docId, String graphAddressId, String accountId, String graphId,
            WriteProvenance writtenBy, String contentType,
            Map<String, String> s3Metadata, boolean verifyChecksums, long docVersion) {

        record Put(PartObject part, String key, BlobStore.PutResult result) {
        }
        List<Callable<Put>> puts = new ArrayList<>(parts.size());
        for (PartObject p : parts) {
            String key = DocumentPartCodec.objectKey(basePrefix, p.part(), p.subKey());
            puts.add(() -> new Put(p, key,
                    store.put(new BlobStore.PutSpec(bucket, key, contentType, s3Metadata,
                            verifyChecksums ? p.sha256() : null), p.bytes())));
        }
        List<Put> landed = joinAll(puts, "part write");

        Instant now = Instant.now();
        Timestamp ts = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        DocumentManifest.Builder manifest = DocumentManifest.newBuilder()
                .setDocId(docId)
                .setGraphAddressId(graphAddressId)
                .setAccountId(accountId)
                .setGraphId(graphId)
                .setDocVersion(docVersion);

        long total = 0;
        String coreEtag = "";
        String coreVersion = null;
        List<String> keys = new ArrayList<>(landed.size());
        Set<DocumentPart> present = EnumSet.noneOf(DocumentPart.class);
        for (Put put : landed) {
            total += put.part().bytes().length;
            keys.add(put.key());
            present.add(put.part().part());
            if (put.part().part() == DocumentPart.DOCUMENT_PART_CORE) {
                coreEtag = put.result().eTag() != null ? put.result().eTag() : "";
                coreVersion = put.result().versionId();
            }
            manifest.addParts(withWriter(PartManifestEntry.newBuilder()
                            .setPart(put.part().part())
                            .setState(PartState.PART_STATE_PRESENT)
                            .setSizeBytes(put.part().bytes().length)
                            .setSha256(put.part().sha256())
                            .setUpdatedAt(ts)
                            .setObjectKey(put.key())
                            .setSubKey(put.part().subKey()),
                    writtenBy));
        }
        // Record the parts that had no content as EMPTY — no object written.
        for (DocumentPart part : List.of(DocumentPart.DOCUMENT_PART_BLOBS,
                DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_PARSED)) {
            if (!present.contains(part)) {
                manifest.addParts(withWriter(PartManifestEntry.newBuilder()
                                .setPart(part)
                                .setState(PartState.PART_STATE_EMPTY)
                                .setUpdatedAt(ts),
                        writtenBy));
            }
        }

        String root = DocumentPartCodec.rootChecksum(parts);
        return new WriteResult(manifest.build(), root, total, coreEtag, coreVersion, keys);
    }

    /**
     * Applies writer attribution to a manifest entry: when a provenance struct
     * is present it is stamped verbatim; absent provenance stays absent — the
     * platform never invents an attribution.
     */
    private static PartManifestEntry.Builder withWriter(PartManifestEntry.Builder entry,
            WriteProvenance writtenBy) {
        if (writtenBy != null) {
            return entry.setWrittenBy(writtenBy);
        }
        return entry;
    }

    /**
     * One copy-forward instruction of the partial save: carry an unwritten
     * part object from the consumed hop's address to the destination.
     *
     * @param sourceEntry the source manifest entry being carried forward
     * @param destKey the destination object key to copy to
     */
    public record CopySpec(PartManifestEntry sourceEntry, String destKey) {
    }

    /**
     * Copies unwritten part objects to the destination address, all copies in
     * parallel. Same-drive copies are object-store server-side copies — the
     * bytes never transit this service; cross-drive copies fall back to
     * get+put through memory.
     *
     * @param srcStore the source drive's blob store
     * @param srcBucket the source bucket
     * @param dstStore the destination drive's blob store
     * @param dstBucket the destination bucket
     * @param sameStore whether source and destination are the same drive
     *        (enables server-side copy)
     * @param specs the copy instructions
     * @throws BlobStore.BlobNotFoundException (wrapped) when a copy source is
     *         already gone — the caller maps this to FAILED_PRECONDITION
     */
    public void copyParts(BlobStore srcStore, String srcBucket, BlobStore dstStore, String dstBucket,
            boolean sameStore, List<CopySpec> specs) {
        if (specs.isEmpty()) {
            return;
        }
        List<Callable<Boolean>> copies = new ArrayList<>(specs.size());
        for (CopySpec spec : specs) {
            copies.add(() -> {
                if (sameStore) {
                    dstStore.copy(srcBucket, spec.sourceEntry().getObjectKey(), dstBucket, spec.destKey());
                } else {
                    BlobStore.GetResult src = srcStore.get(srcBucket, spec.sourceEntry().getObjectKey());
                    dstStore.put(new BlobStore.PutSpec(dstBucket, spec.destKey(),
                            "application/x-protobuf", null, null), src.data());
                }
                return Boolean.TRUE; // joinAll's single-task path uses List.of — no nulls
            });
        }
        joinAll(copies, "part copy-forward");
    }

    /**
     * Reads and assembles a document from its manifest, all GETs in parallel.
     *
     * @param store the blob store for the resolved drive
     * @param bucket the bucket holding the part objects
     * @param manifest the stored state's part manifest
     * @param parts which parts to assemble; empty = ALL (full document)
     * @param chunkSets which CHUNKS sub keys to include; empty = all chunk sets
     * @param prototype the message type to assemble into
     * @param <T> the assembled message type
     * @return the assembled document, or {@code null} when any requested
     *         PRESENT object is missing from storage (purged out from under us)
     */
    public <T extends Message> T readParts(BlobStore store, String bucket, DocumentManifest manifest,
            Set<DocumentPart> parts, Set<String> chunkSets, T prototype) {
        List<PartManifestEntry> wanted = new ArrayList<>();
        for (PartManifestEntry e : manifest.getPartsList()) {
            if (e.getState() != PartState.PART_STATE_PRESENT) {
                continue;
            }
            if (!parts.isEmpty() && !parts.contains(e.getPart())) {
                continue;
            }
            if (e.getPart() == DocumentPart.DOCUMENT_PART_CHUNKS
                    && !chunkSets.isEmpty() && !chunkSets.contains(e.getSubKey())) {
                continue;
            }
            wanted.add(e);
        }
        if (wanted.isEmpty()) {
            return defaultInstance(prototype);
        }

        List<Callable<byte[]>> gets = new ArrayList<>(wanted.size());
        for (PartManifestEntry e : wanted) {
            gets.add(() -> store.get(bucket, e.getObjectKey()).data());
        }
        List<byte[]> fragments;
        try {
            fragments = joinAll(gets, "part read");
        } catch (RuntimeException ex) {
            if (causeIsNotFound(ex)) {
                // A manifest-PRESENT object is physically gone (purged out from
                // under us — settlement teardown, index-prefix GC, a drive
                // delete that never tombstoned the manifest). Attribute exactly
                // which parts are missing (a targeted re-check, error path only)
                // and raise a typed signal so the caller can reconcile the
                // manifest instead of reporting an opaque "not found".
                Set<DocumentPart> missing = attributeMissing(store, bucket, wanted);
                if (missing.isEmpty()) {
                    // Re-check found everything present — the original failure
                    // was transient; preserve the prior null contract.
                    LOG.warn("Part read reported not-found but re-check found all present "
                                    + "(doc_id={}, address={}) — treating as transient",
                            manifest.getDocId(), manifest.getGraphAddressId());
                    return null;
                }
                LOG.warn("Part object(s) missing during assembly (doc_id={}, address={}, parts={}): {}",
                        manifest.getDocId(), manifest.getGraphAddressId(), missing, ex.getMessage());
                throw new PartObjectMissingException(
                        "part object(s) gone from storage: " + missing, missing);
            }
            throw ex;
        }
        try {
            return DocumentPartCodec.assemble(fragments, prototype);
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Part fragment parse failed (doc_id={}, address={})",
                    manifest.getDocId(), manifest.getGraphAddressId(), e);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Message> T defaultInstance(T prototype) {
        return (T) prototype.getDefaultInstanceForType();
    }

    /**
     * Re-checks each wanted part's object and returns exactly the parts whose
     * object is confirmed gone (a {@link BlobStore.BlobNotFoundException} on a
     * fresh HEAD — existence only, no bytes fetched). Runs only on the assembly
     * error path, so the extra sequential probes are rare. A non-not-found
     * error on the re-check is NOT counted as missing — we only tombstone what
     * we can prove is gone.
     */
    private static Set<DocumentPart> attributeMissing(BlobStore store, String bucket,
            List<PartManifestEntry> wanted) {
        EnumSet<DocumentPart> missing = EnumSet.noneOf(DocumentPart.class);
        for (PartManifestEntry e : wanted) {
            try {
                store.headObject(bucket, e.getObjectKey());
            } catch (BlobStore.BlobNotFoundException notFound) {
                missing.add(e.getPart());
            } catch (RuntimeException other) {
                // A transient/other error — do not misattribute as "gone".
                LOG.debug("attributeMissing: non-not-found error probing {} ({}) — not tombstoning",
                        e.getObjectKey(), other.getMessage());
            }
        }
        return missing;
    }

    /**
     * Raised by {@link #readParts} when the manifest claims parts are PRESENT
     * but their objects are physically gone from storage. Carries the exact set
     * of missing parts so the caller can reconcile the manifest (tombstone them
     * to DELETED) and surface an honest error rather than a bare {@code null}.
     */
    public static final class PartObjectMissingException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final transient Set<DocumentPart> missingParts;

        /**
         * Creates the exception.
         *
         * @param message detail message
         * @param missingParts the parts whose objects are confirmed gone
         */
        public PartObjectMissingException(String message, Set<DocumentPart> missingParts) {
            super(message);
            this.missingParts = missingParts;
        }

        /**
         * The parts whose objects are confirmed missing from storage.
         *
         * @return the missing parts
         */
        public Set<DocumentPart> missingParts() {
            return missingParts;
        }
    }

    private static boolean causeIsNotFound(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause() == cur ? null : cur.getCause()) {
            if (cur instanceof BlobStore.BlobNotFoundException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Runs all tasks concurrently on virtual threads and joins them; the first
     * failure propagates (after all tasks settle) as a RuntimeException.
     */
    private static <T> List<T> joinAll(List<Callable<T>> tasks, String what) {
        if (tasks.size() == 1) {
            try {
                return List.of(tasks.get(0).call());
            } catch (Exception e) {
                throw asRuntime(e, what);
            }
        }
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(FANOUT.submit(task));
        }
        List<T> out = new ArrayList<>(tasks.size());
        RuntimeException failure = null;
        for (Future<T> f : futures) {
            try {
                out.add(f.get());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (failure == null) {
                    failure = asRuntime(cause != null ? cause : e, what);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (failure == null) {
                    failure = new IllegalStateException("Interrupted during " + what, e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
        return out;
    }

    private static RuntimeException asRuntime(Throwable t, String what) {
        return t instanceof RuntimeException re ? re
                : new IllegalStateException(what + " failed: " + t.getMessage(), t);
    }
}
