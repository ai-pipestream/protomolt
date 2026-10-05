package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.admission.DocumentHistoricalResponseVerifier;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.HistoricalMaterializationRepository;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.InvalidProtocolBufferException;

/** Owns a bounded transport snapshot independently of the repository result lifetime. */
final class HistoricalOccurrenceResponses {
    private HistoricalOccurrenceResponses() {}
    record Snapshot(ReadHistoricalOccurrenceResponse response, PayloadBudget.Lease lease) implements AutoCloseable {
        @Override public void close() { lease.close(); }
    }

    static Snapshot capture(ReadHistoricalOccurrenceRequest request, HistoricalMaterializationRepository.Result read,
            PayloadBudget budget, RepositoryReadControl control) {
        var view = read.view(control);
        var selected = view.selection();
        var definition = view.definition();
        var response = ReadHistoricalOccurrenceResponse.newBuilder().setAddress(view.address())
                .setRevisionId(view.revision().toString()).setSelection(HistoricalOccurrenceSelection.newBuilder()
                        .setRevisionOrdinal(selected.revisionOrdinal()).setRootSha256(selected.rootSha256()).setPathSha256(selected.pathSha256()))
                .setOriginal(view.original()).setRoot(view.occurrence().root()).setPath(view.path())
                .setDefinition(HistoricalRetainedDefinition.newBuilder().setReference(definition.reference())
                        .setDescriptorArtifact(definition.descriptorArtifact()).setMetadataArtifact(definition.metadataArtifact())).build();
        long bytes = response.getSerializedSize();
        if (bytes > DocumentHistoricalResponseVerifier.MAX_BYTES)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Selected response exceeds wire bound");
        // One serialization plus the independent parsed byte fields. This is not total JVM heap accounting.
        var lease = reserve(budget, 2 * bytes);
        boolean delivered = false;
        try {
            control.check();
            var snapshot = ReadHistoricalOccurrenceResponse.parseFrom(response.toByteString());
            DocumentHistoricalResponseVerifier.verify(request, snapshot,
                    size -> { var scratch = reserve(budget, size); return scratch::close; }, control::check);
            control.check();
            var result = new Snapshot(snapshot, lease);
            delivered = true;
            return result;
        } catch (InvalidProtocolBufferException | IllegalArgumentException
                | ai.protomolt.proto.validate.ValidationResult.ValidationException malformed) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Invalid selected historical response", malformed);
        } finally { if (!delivered) lease.close(); }
    }

    private static PayloadBudget.Lease reserve(PayloadBudget budget, long size) {
        try { return budget.reserve(size); }
        catch (PayloadBudget.CapacityExceededException full) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Selected response capacity exhausted");
        }
    }
}
