package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.GetPayloadRequest;
import ai.protomolt.proto.mesh.runtime.v1.HeadPayloadRequest;
import ai.protomolt.proto.mesh.runtime.v1.MarkPayloadEligibleRequest;
import ai.protomolt.proto.mesh.runtime.v1.PayloadChunk;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLeaseRequest;
import ai.protomolt.proto.mesh.runtime.v1.PayloadLeaseResponse;
import ai.protomolt.proto.mesh.runtime.v1.PayloadStoreServiceGrpc;
import ai.protomolt.proto.mesh.runtime.v1.PurgePayloadRequest;
import ai.protomolt.proto.mesh.runtime.v1.PurgePayloadResponse;
import ai.protomolt.proto.mesh.runtime.v1.PutPayloadHeader;
import ai.protomolt.proto.mesh.runtime.v1.PutPayloadRequest;
import ai.protomolt.proto.mesh.runtime.v1.PutPayloadResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.util.Objects;

/** Bounded protobuf gRPC facade over the transport-neutral payload store. */
public final class PayloadStoreGrpcService
        extends PayloadStoreServiceGrpc.PayloadStoreServiceImplBase {

    private static final int MAX_PUT_BYTES = 256 * 1024 * 1024;
    private static final int CHUNK_BYTES = 1024 * 1024;

    private final PayloadStore store;
    private final Clock clock;

    public PayloadStoreGrpcService(PayloadStore store, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public StreamObserver<PutPayloadRequest> putPayload(
            StreamObserver<PutPayloadResponse> response) {
        return new StreamObserver<>() {
            private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            private PutPayloadHeader header;
            private boolean ended;

            @Override
            public void onNext(PutPayloadRequest request) {
                if (ended) {
                    return;
                }
                try {
                    if (request.hasHeader()) {
                        if (header != null || bytes.size() != 0) {
                            throw new IllegalArgumentException(
                                    "payload-put-header-order: exactly one first header is required");
                        }
                        header = request.getHeader();
                        if (header.getExpectedSizeBytes() > MAX_PUT_BYTES) {
                            throw new IllegalArgumentException("payload-put-too-large");
                        }
                    } else if (request.hasData()) {
                        if (header == null) {
                            throw new IllegalArgumentException("payload-put-header-missing");
                        }
                        if ((long) bytes.size() + request.getData().size() > MAX_PUT_BYTES) {
                            throw new IllegalArgumentException("payload-put-too-large");
                        }
                        bytes.writeBytes(request.getData().toByteArray());
                    } else {
                        throw new IllegalArgumentException("payload-put-frame-empty");
                    }
                } catch (RuntimeException failure) {
                    ended = true;
                    response.onError(status(failure));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                ended = true;
            }

            @Override
            public void onCompleted() {
                if (ended) {
                    return;
                }
                try {
                    if (header == null || header.getExpectedSizeBytes() != bytes.size()) {
                        throw new IllegalArgumentException("payload-put-length-mismatch");
                    }
                    var metadata = store.put(new PayloadStore.Put(
                            header.getNamespace(), header.getProfile(),
                            header.getPayloadTypeName(), header.getDescriptorFingerprint(),
                            bytes.toByteArray(), header.getExpectedSha256()));
                    response.onNext(PutPayloadResponse.newBuilder()
                            .setMetadata(metadata).build());
                    response.onCompleted();
                    ended = true;
                } catch (RuntimeException failure) {
                    ended = true;
                    response.onError(status(failure));
                }
            }
        };
    }

    @Override
    public void getPayload(GetPayloadRequest request,
            StreamObserver<PayloadChunk> response) {
        try {
            var metadata = store.head(request.getIdentity());
            long size = metadata.getIdentity().getArtifact().getSizeBytes();
            long offset = request.getOffset();
            long length = request.getLength() == 0 ? size - offset : request.getLength();
            if (length < 0 || length > PayloadStore.MAX_RANGE_BYTES
                    || offset > size || offset + length > size) {
                throw new IllegalArgumentException("payload-range-invalid");
            }
            response.onNext(PayloadChunk.newBuilder()
                    .setMetadata(metadata).setOffset(offset).build());
            long emitted = 0;
            while (emitted < length) {
                int chunk = Math.toIntExact(Math.min(CHUNK_BYTES, length - emitted));
                byte[] bytes = store.get(request.getIdentity(), offset + emitted, chunk);
                response.onNext(PayloadChunk.newBuilder()
                        .setOffset(offset + emitted)
                        .setData(ByteString.copyFrom(bytes)).build());
                emitted += chunk;
            }
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure));
        }
    }

    @Override
    public void acquireLease(PayloadLeaseRequest request,
            StreamObserver<PayloadLeaseResponse> response) {
        unary(response, () -> PayloadLeaseResponse.newBuilder()
                .setMetadata(store.acquire(request.getIdentity(), request.getOwnerId(),
                        request.getLeaseId(), RemoteValidation.instant(request.getExpiresAt())))
                .build());
    }

    @Override
    public void releaseLease(PayloadLeaseRequest request,
            StreamObserver<PayloadLeaseResponse> response) {
        unary(response, () -> PayloadLeaseResponse.newBuilder()
                .setMetadata(store.release(request.getIdentity(), request.getOwnerId(),
                        request.getLeaseId())).build());
    }

    @Override
    public void headPayload(HeadPayloadRequest request,
            StreamObserver<ai.protomolt.proto.mesh.runtime.v1.PayloadMetadata> response) {
        unary(response, () -> store.head(request.getIdentity()));
    }

    @Override
    public void markEligibleForDeletion(MarkPayloadEligibleRequest request,
            StreamObserver<ai.protomolt.proto.mesh.runtime.v1.PayloadMetadata> response) {
        unary(response, () -> store.markEligible(request.getIdentity(),
                RemoteValidation.instant(request.getNotBefore()),
                request.getRetentionPolicyReference(),
                request.getLegalHoldPolicyReference()));
    }

    @Override
    public void purgePayload(PurgePayloadRequest request,
            StreamObserver<PurgePayloadResponse> response) {
        unary(response, () -> {
            var metadata = store.purge(
                    request.getIdentity(), request.getReason(), clock.instant());
            return PurgePayloadResponse.newBuilder()
                    .setMetadata(metadata).setPurged(metadata.getPurged()).build();
        });
    }

    private static <T> void unary(StreamObserver<T> response,
            java.util.function.Supplier<T> operation) {
        try {
            response.onNext(operation.get());
            response.onCompleted();
        } catch (RuntimeException failure) {
            response.onError(status(failure));
        }
    }

    private static io.grpc.StatusRuntimeException status(RuntimeException failure) {
        Status code = failure instanceof IllegalArgumentException
                ? Status.INVALID_ARGUMENT : Status.FAILED_PRECONDITION;
        return code.withDescription(failure.getMessage()).withCause(failure)
                .asRuntimeException();
    }
}
