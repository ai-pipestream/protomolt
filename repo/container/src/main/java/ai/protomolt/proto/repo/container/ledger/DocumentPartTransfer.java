package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import java.util.Map;
import java.util.Objects;

/**
 * One provider transfer after admission. The caller owns backend qualification,
 * private validated payloads, byte/concurrency limits and resource lifetime.
 * No SQL transaction may surround this call. It neither admits nor publishes.
 */
final class DocumentPartTransfer {
    private DocumentPartTransfer() {}

    record Verified(DocumentPartAttemptLedger.PlannedObject planned, String version, String etag) {}

    static final class Failure extends RuntimeException {
        private final String phase;
        Failure(String phase, RuntimeException cause) { super("Document part transfer failed during " + phase, cause); this.phase=phase; }
        String phase() { return phase; }
    }

    /**
     * The body is a private copy already matched to the immutable declaration.
     * afterPut permits a post-I/O lease check before read-back; it must finish its
     * own transaction before returning. Cancellation cannot undo a completed PUT.
     * A failed PUT acknowledgement is propagated unchanged as the failure cause.
     */
    static Verified upload(BlobStore store, String namespace, DocumentPartAttemptLedger.PlannedObject planned,
            byte[] body, Map<String,String> attributes, Runnable check, Runnable afterPut) {
        Objects.requireNonNull(store); Objects.requireNonNull(namespace); Objects.requireNonNull(planned);
        Objects.requireNonNull(body); Objects.requireNonNull(attributes); Objects.requireNonNull(check); Objects.requireNonNull(afterPut);
        String phase="PUT";
        try {
            check.run();
            var put=store.put(new BlobStore.PutSpec(namespace,planned.objectKey(),planned.contentType(),attributes,planned.sha256()),body);
            check.run();
            if (put==null) throw new IllegalStateException("Provider did not return a PUT receipt");
            phase="post-PUT lease check";
            afterPut.run();
            phase="read-back verification";
            check.run();
            var actual=store.getBounded(namespace,planned.objectKey(),put.versionId(),body.length);
            check.run();
            if (actual==null || actual.data()==null || actual.data().length!=planned.size()
                    || !DocumentPartCodec.sha256Hex(actual.data()).equals(planned.sha256())
                    || !Objects.equals(planned.contentType(),actual.contentType())
                    || !Objects.equals(put.versionId(),actual.versionId()) || !Objects.equals(put.eTag(),actual.eTag()))
                throw new BlobStoreException(BlobStoreException.Code.DATA_LOSS,"Read-back differs from planned bytes or PUT identity",null);
            return new Verified(planned,actual.versionId(),actual.eTag());
        } catch (RuntimeException failure) { throw new Failure(phase,failure); }
    }
}
