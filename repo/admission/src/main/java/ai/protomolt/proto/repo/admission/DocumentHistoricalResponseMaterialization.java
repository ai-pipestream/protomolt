package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.ReadHistoricalOccurrenceRequest;
import ai.protomolt.proto.repo.v1.ReadHistoricalOccurrenceResponse;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Objects;

/** Selected wire decoding with exact retained descriptors, without registry discovery or a new admission verdict. */
public final class DocumentHistoricalResponseMaterialization {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private DocumentHistoricalResponseMaterialization() {}

    public record Limits(int maxValueBytes, long maxWireValues, int maxDepth) {
        public Limits {
            if (maxValueBytes < 0 || maxValueBytes > DocumentHistoricalResponseVerifier.MAX_BYTES
                    || maxWireValues < 1 || maxDepth < 0 || maxDepth > 100)
                throw new IllegalArgumentException("Invalid selected response decode limits");
        }
    }

    public static final class ResourceLimit extends IllegalArgumentException {
        ResourceLimit(String message, Throwable cause) { super(message, cause); }
    }

    /** Borrowed until close; an already returned Java view cannot be revoked. Nested Any values remain envelopes. */
    public record View(ReadHistoricalOccurrenceResponse response, DynamicMessage value, RepositorySchemaAsset metadata) {}

    public static final class Result implements AutoCloseable {
        private View view;
        private final DocumentAdmissionReservations.Lease lease;
        private Result(View view, DocumentAdmissionReservations.Lease lease) {
            this.view = view; this.lease = lease;
        }
        public synchronized View view(Runnable control) {
            if (view == null) throw new IllegalStateException("Selected response is closed");
            Objects.requireNonNull(control).run();
            if (view == null) throw new IllegalStateException("Selected response is closed");
            return view;
        }
        @Override public synchronized void close() {
            if (view == null) return;
            view = null;
            lease.close();
        }
    }

    /**
     * Owns serialized response and selected-value reservations through result close. The host
     * must bound transport allocation before calling, and separately bound concurrent parsed
     * heap. Control exceptions propagate unchanged. Verification is not authorization: the
     * transport authenticates the peer and the server authorizes delivery.
     */
    public static Result read(ReadHistoricalOccurrenceRequest request, ReadHistoricalOccurrenceResponse response,
            Limits limits, DocumentAdmissionReservations reservations, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(request); Objects.requireNonNull(response); Objects.requireNonNull(limits);
        Objects.requireNonNull(reservations); Objects.requireNonNull(control).run();
        if (request.getSerializedSize() > 32 * 1024 || !VALIDATOR.validate(request).valid()
                || !request.getUnknownFields().asMap().isEmpty()
                || !request.getAddress().getUnknownFields().asMap().isEmpty()
                || !request.getSelection().getUnknownFields().asMap().isEmpty()
                || !request.getLimits().getUnknownFields().asMap().isEmpty())
            throw new IllegalArgumentException("Invalid selected request envelope");
        int responseBytes = response.getSerializedSize();
        int valueBytes = response.getOriginal().getValue().size();
        if (responseBytes > DocumentHistoricalResponseVerifier.MAX_BYTES
                || valueBytes > limits.maxValueBytes() || valueBytes > request.getLimits().getMaxDecodedBytes())
            throw new ResourceLimit("Selected response exceeds decode byte limit", null);
        var lease = Objects.requireNonNull(reservations.reserve((long) responseBytes + valueBytes));
        boolean transferred = false;
        try {
            final DocumentSchemaAssetBinding binding;
            try {
                binding = DocumentHistoricalResponseVerifier.verifyBinding(request, response, reservations, () -> {
                    try { control.run(); }
                    catch (RuntimeException failure) { throw new ControlFailure(failure); }
                });
            } catch (ControlFailure failure) {
                throw failure.original;
            } catch (ClosedDescriptorSet.LimitExceededException failure) {
                throw new ResourceLimit("Selected descriptor exceeds linking limits", failure);
            }
            var steps = response.getPath().getStepsList();
            var boundary = steps.getLast().getAnyBoundary();
            var occurrence = new DocumentSchemaAdmission.Selection(response.getSelection().getRevisionOrdinal(),
                    response.getRoot(), boundary.getTypeUrl(), steps.subList(0, steps.size() - 1),
                    boundary.getValueSha256(), boundary.getValueSizeBytes());
            var decoded = DocumentAnyMaterialization.read(occurrence, response.getOriginal(),
                    DocumentAnyMaterialization.Mode.MATERIALIZE_IF_AVAILABLE,
                    new DocumentAnyMaterialization.Limits(limits.maxValueBytes(), limits.maxWireValues(), limits.maxDepth()),
                    ignored -> new DocumentAnyMaterialization.Resolved(binding), control);
            if (decoded instanceof DocumentAnyMaterialization.Failed failed) {
                if (failed.failure().reason() == DocumentAnyMaterialization.Reason.RESOURCE_LIMIT)
                    throw new ResourceLimit("Selected response exceeds wire decode limits", failed.failure().cause().orElse(null));
                throw new IllegalArgumentException("Selected response decode failed: " + failed.failure().reason(),
                        failed.failure().cause().orElse(null));
            }
            if (!(decoded instanceof DocumentAnyMaterialization.Decoded value))
                throw new IllegalStateException("Selected response did not decode");
            control.run();
            var result = new Result(new View(response, value.value(), binding.metadata()), lease);
            transferred = true;
            return result;
        } finally {
            if (!transferred) lease.close();
        }
    }

    private static final class ControlFailure extends RuntimeException {
        private final RuntimeException original;
        private ControlFailure(RuntimeException original) { this.original = original; }
    }
}
