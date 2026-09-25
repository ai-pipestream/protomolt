package ai.protomolt.proto.correction;

import ai.protomolt.proto.inference.spi.ChunkObserver;
import ai.protomolt.proto.inference.spi.InferenceException;
import ai.protomolt.proto.inference.spi.InferenceProvider;
import ai.protomolt.proto.inference.v1.*;
import ai.protomolt.proto.validate.ValidationResult;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.util.concurrent.TimeUnit;

/** Fixed client for the workstation's authenticated InferenceService bridge. */
final class RemoteInferenceProvider implements InferenceProvider, AutoCloseable {
    private final ManagedChannel channel;
    private final InferenceServiceGrpc.InferenceServiceBlockingStub stub;
    private volatile ModelEntry described;

    RemoteInferenceProvider(String target, String token) {
        if (target == null || target.isBlank() || token == null || token.isBlank()) {
            throw new IllegalArgumentException("remote inference target and token are required");
        }
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        stub = InferenceServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    @Override public String id() {
        ModelEntry entry = described;
        if (entry == null) throw new IllegalStateException("remote model must be described first");
        return entry.getProvider();
    }

    ModelEntry describe(String model) {
        try {
            ModelEntry entry = stub.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .describeModel(DescribeModelRequest.newBuilder().setModel(model).build()).getEntry();
            if (!model.equals(entry.getId())) {
                throw new InferenceException("remote inference returned a different model identity");
            }
            ValidationResult.validate(entry).throwIfInvalid();
            if (entry.getProvider().isBlank()) {
                throw new InferenceException("remote inference did not name its provider");
            }
            ModelEntry previous = described;
            if (previous != null && !previous.equals(entry)) {
                throw new InferenceException("remote inference model identity changed");
            }
            described = entry;
            return entry;
        } catch (StatusRuntimeException failure) {
            throw new InferenceException("remote inference DescribeModel failed: "
                    + failure.getStatus().getCode(), failure);
        }
    }

    @Override public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
        ModelEntry identity = described;
        if (identity == null || !identity.getId().equals(request.getModel())
                || !identity.getProvider().equals(model.getProvider())) {
            throw new InferenceException("remote inference model was not described or changed");
        }
        try {
            GenerateResponse response = stub.withDeadlineAfter(90, TimeUnit.SECONDS).generate(request);
            ValidationResult.validate(response).throwIfInvalid();
            if (!request.getModel().equals(response.getModel())) {
                throw new InferenceException("remote inference response names a different model");
            }
            if (!identity.getProvider().equals(response.getProvider())) {
                throw new InferenceException("remote inference response names a different provider");
            }
            return response;
        } catch (StatusRuntimeException failure) {
            throw new InferenceException("remote inference Generate failed: "
                    + failure.getStatus().getCode(), failure);
        }
    }

    @Override public void generateStream(ModelEntry model, GenerateStreamRequest request,
                                         ChunkObserver observer) {
        throw new InferenceException("remote correction streaming is unavailable");
    }

    @Override public void close() {
        channel.shutdownNow();
    }
}
