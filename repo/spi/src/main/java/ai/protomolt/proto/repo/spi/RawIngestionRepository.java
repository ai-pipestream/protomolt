package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.SaveDocumentResponse;
import java.io.InputStream;
import java.util.UUID;

/** Shared streaming intake operation; transport parsing belongs outside this interface. */
public interface RawIngestionRepository {
    /** Blank docId derives identity from content. Blank optional metadata is omitted. */
    record Request(String accountId, String datasourceId, String driveName, String docId,
            String filename, String mimeType, String connectorId, String crawlId, String declaredSha256) {}

    /** The storage reference names committed bytes, which may predate a deduplicated attempt. */
    record Result(UUID attemptId, SaveDocumentResponse document, FileStorageReference storageRef,
            long sizeBytes, String sha256) {}

    /**
     * Consumes exactly knownLength bytes and checks EOF; the caller owns/closes body.
     * A failed or ambiguous invocation must not be assumed to have published nothing.
     * attemptId is an internal upload identity, not a client idempotency key.
     */
    Result upload(RepositoryCaller caller, Request request, InputStream body, long knownLength);
}
