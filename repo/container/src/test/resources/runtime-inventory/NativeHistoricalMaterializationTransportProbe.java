package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.service.DocumentHistoryMaterializationGrpcService;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.*;
import io.grpc.inprocess.*;
import io.grpc.stub.MetadataUtils;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Actual gRPC over the actual SQL/provider repository; wrappers inject only invalid output or revocation. */
public final class NativeHistoricalMaterializationTransportProbe {
    static void run(HistoricalMaterializationRepository repository, NodeAddress address, UUID revision,
            HistoricalMaterializationRepository.Selection selection, HistoricalMaterializationRepository.Limits limits,
            Runnable revoke, Runnable restore) throws Exception {
        var corrupt = new AtomicBoolean(); var revokeOnView = new AtomicBoolean(); var invalidMetadata = new AtomicBoolean();
        HistoricalMaterializationRepository observed = (caller, node, id, selected, bound, control) -> {
            var actual = repository.readMaterialized(caller, node, id, selected, bound, control);
            return new HistoricalMaterializationRepository.Result() {
                public HistoricalMaterializationRepository.View view(RepositoryReadControl active) {
                    var view = actual.view(active);
                    if (revokeOnView.compareAndSet(true, false)) revoke.run();
                    if (invalidMetadata.get()) {
                        var metadata = view.definition().metadata().toBuilder().clearTypeUrl().build();
                        var bytes = metadata.toByteString();
                        var reference = view.definition().reference().toBuilder().setMetadataSha256(
                                ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(bytes.toByteArray())).build();
                        var definition = new HistoricalMaterializationRepository.Definition(metadata,
                                view.definition().descriptorArtifact(), reference, bytes);
                        return new HistoricalMaterializationRepository.View(view.selection(), view.original(), view.value(), view.schema(),
                                view.occurrence(), definition, view.path(), view.address(), view.revision());
                    }
                    if (!corrupt.get()) return view;
                    return new HistoricalMaterializationRepository.View(view.selection(), view.original(), view.value(), view.schema(),
                            view.occurrence(), view.definition(), view.path(), view.address(), UUID.randomUUID());
                }
                public void close() { actual.close(); }
            };
        };
        var budget = new PayloadBudget(32L * 1024 * 1024);
        var service = new DocumentHistoryMaterializationGrpcService(observed, caller -> new RepositoryCaller(caller.name(), false,
                Set.of(caller.name().equals("owner") ? address.getAccountId() : "other-account"), Set.of()), limits, budget, 2);
        var identity = Metadata.Key.of("test-identity", Metadata.ASCII_STRING_MARSHALLER);
        ServerInterceptor auth = new ServerInterceptor() {
            public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                String name = headers.get(identity);
                return name == null ? next.startCall(call, headers) : Contexts.interceptCall(
                        Context.current().withValue(CallerContexts.CALLER, Caller.scoped(name, Set.of())), call, headers, next);
            }
        };
        var request = ReadHistoricalOccurrenceRequest.newBuilder().setAddress(address).setRevisionId(revision.toString())
                .setSelection(HistoricalOccurrenceSelection.newBuilder().setRevisionOrdinal(selection.revisionOrdinal())
                        .setRootSha256(selection.rootSha256()).setPathSha256(selection.pathSha256()))
                .setLimits(HistoricalMaterializationLimits.newBuilder().setMaxFragmentBytes(limits.maxFragmentBytes())
                        .setMaxEvidenceBytes(limits.maxEvidenceBytes()).setMaxRetainedBytes(limits.maxRetainedBytes())
                        .setMaxReferences(limits.maxReferences()).setMaxDecodedBytes(limits.maxDecodedBytes()).setMaxBoundaries(limits.maxBoundaries())).build();
        String name = InProcessServerBuilder.generateName();
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var server = InProcessServerBuilder.forName(name).executor(executor).intercept(auth).addService(service).build().start();
            var channel = InProcessChannelBuilder.forName(name).build();
            try {
                var plain = DocumentHistoryMaterializationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(20, TimeUnit.SECONDS);
                var headers = new Metadata(); headers.put(identity, "owner");
                var owner = plain.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
                status(() -> plain.readHistoricalOccurrence(request), Status.Code.UNAUTHENTICATED);
                var response = owner.readHistoricalOccurrence(request);
                require(response.getAddress().equals(address) && response.getRevisionId().equals(revision.toString()), "captured transport identity");
                require(response.getOriginal().unpack(com.google.protobuf.StringValue.class).getValue().equals("retained payload"), "real selected transport payload");
                require(response.getSelection().equals(request.getSelection()), "exact selection echoed from result");
                status(() -> owner.readHistoricalOccurrence(request.toBuilder().clearSelection().build()), Status.Code.INVALID_ARGUMENT);
                var foreignHeaders = new Metadata(); foreignHeaders.put(identity, "foreign");
                status(() -> plain.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(foreignHeaders)).readHistoricalOccurrence(request), Status.Code.NOT_FOUND);
                corrupt.set(true);
                status(() -> owner.readHistoricalOccurrence(request), Status.Code.DATA_LOSS);
                corrupt.set(false);
                invalidMetadata.set(true);
                status(() -> owner.readHistoricalOccurrence(request), Status.Code.DATA_LOSS);
                invalidMetadata.set(false);
                revokeOnView.set(true);
                try { status(() -> owner.readHistoricalOccurrence(request), Status.Code.NOT_FOUND); }
                finally { restore.run(); }
                // Explicitly lower client work limits; no raw fallback is allowed.
                status(() -> owner.readHistoricalOccurrence(request.toBuilder().setLimits(request.getLimits().toBuilder().setMaxFragmentBytes(1)).build()),
                        Status.Code.RESOURCE_EXHAUSTED);
            } finally {
                channel.shutdownNow(); server.shutdownNow();
                require(channel.awaitTermination(5, TimeUnit.SECONDS) && server.awaitTermination(5, TimeUnit.SECONDS), "transport drained");
            }
        }
        require(budget.reservedBytes() == 0, "transport reservations drained");
        System.out.println("NATIVE_HISTORICAL_MATERIALIZATION_TRANSPORT_OK");
    }
    private static void status(Runnable work, Status.Code expected) {
        try { work.run(); throw new AssertionError("Expected " + expected); }
        catch (StatusRuntimeException failure) { require(failure.getStatus().getCode() == expected, "expected " + expected + " but got " + failure.getStatus()); }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
