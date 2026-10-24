package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.archive.DocumentArchive;
import ai.protomolt.proto.repo.archive.RenditionReads;
import ai.protomolt.proto.repo.archive.RenditionUploads;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationReceipt;
import ai.protomolt.proto.repo.archive.v1.ArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryResponse;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.GetArchiveMutationRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionReadHeader;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.engine.ArchivePutAdmission;
import ai.protomolt.proto.repo.spi.ArchiveMutationRepository;
import ai.protomolt.proto.repo.spi.ArchiveRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Adapts the v2 {@link DocumentArchive}, {@link RenditionUploads}, and
 * {@link RenditionReads} to the in-JVM {@link ArchiveRepository} and
 * {@link ArchiveMutationRepository} SPIs, and supplies the gRPC
 * {@link ServerInterceptor} enforcing caller account bindings and bounded-profile
 * admission on {@code DocumentArchiveGrpcService} and
 * {@code DocumentArchiveMutationGrpcService}.
 */
final class DocumentArchiveRepository implements ArchiveRepository, ArchiveMutationRepository {

    static final ThreadLocal<Boolean> IN_PUT_ENTRY = ThreadLocal.withInitial(() -> false);

    private static final int STREAM_CHUNK_BYTES = 64 * 1024;
    private static final long UPLOAD_READY_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);
    private static final RepositoryCaller OPERATOR_CALLER = new RepositoryCaller("archive-operator", true);
    private static final AuthenticatedCaller OPERATOR_AUTH = AuthenticatedCaller.unbound(Caller.operator());

    private final DocumentArchive archive;
    private final RenditionUploads uploads;
    private final RenditionReads reads;
    private final ArchivePutAdmission putAdmission;
    private final boolean bounded;

    DocumentArchiveRepository(
            DocumentArchive archive,
            RenditionUploads uploads,
            RenditionReads reads,
            ArchivePutAdmission putAdmission,
            boolean bounded) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.uploads = Objects.requireNonNull(uploads, "uploads");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.putAdmission = putAdmission;
        this.bounded = bounded;
    }

    @Override
    public CreateArchiveResponse createArchive(RepositoryCaller caller, CreateArchiveRequest request) {
        requireProcessAuthority(caller);
        Objects.requireNonNull(request, "request");
        return callAsOperator(() -> archive.createArchive(request));
    }

    @Override
    public GetArchiveResponse getArchive(RepositoryCaller caller, GetArchiveRequest request) {
        Objects.requireNonNull(request, "request");
        requireAccountAccess(caller, request.getAccountId());
        return callAsOperator(() -> archive.getArchive(request));
    }

    @Override
    public ListArchivesResponse listArchives(RepositoryCaller caller, ListArchivesRequest request) {
        Objects.requireNonNull(request, "request");
        requireAccountAccess(caller, request.getAccountId());
        return callAsOperator(() -> archive.listArchives(request));
    }

    @Override
    public GetArchiveStatsResponse stats(RepositoryCaller caller, GetArchiveStatsRequest request) {
        Objects.requireNonNull(request, "request");
        requireAccountAccess(caller, request.getAccountId());
        return callAsOperator(() -> archive.getArchiveStats(request));
    }

    @Override
    public PutEntryResponse putEntry(RepositoryCaller caller, PutEntryRequest request) {
        requireProcessAuthority(caller);
        Objects.requireNonNull(request, "request");
        if (putAdmission == null) {
            return callAsOperator(() -> archive.putEntry(request));
        }
        try (ArchivePutAdmission.Scope ignored = putAdmission.admit(request)) {
            IN_PUT_ENTRY.set(true);
            try {
                return callAsOperator(() -> archive.putEntry(request));
            } finally {
                IN_PUT_ENTRY.remove();
            }
        }
    }

    @Override
    public GetEntryResponse getEntry(RepositoryCaller caller, GetEntryRequest request) {
        Objects.requireNonNull(request, "request");
        requireAccountAccess(caller, request.getAddress().getAccountId());
        return callAsOperator(() -> archive.getEntry(request));
    }

    @Override
    public GetEntryManifestResponse getManifest(RepositoryCaller caller, GetEntryManifestRequest request) {
        Objects.requireNonNull(request, "request");
        requireAccountAccess(caller, request.getAddress().getAccountId());
        return callAsOperator(() -> archive.getEntryManifest(request));
    }

    @Override
    public ListEntriesResponse listEntries(RepositoryCaller caller, ListEntriesRequest request) {
        Objects.requireNonNull(request, "request");
        requireAccountAccess(caller, request.getAccountId());
        return callAsOperator(() -> archive.listEntries(request));
    }

    @Override
    public ListVersionsResponse listVersions(RepositoryCaller caller, ListVersionsRequest request) {
        Objects.requireNonNull(request, "request");
        requireAccountAccess(caller, request.getAddress().getAccountId());
        return callAsOperator(() -> archive.listVersions(request));
    }

    @Override
    public ClassifyEntryResponse classifyEntry(RepositoryCaller caller, ClassifyEntryRequest request) {
        requireProcessAuthority(caller);
        Objects.requireNonNull(request, "request");
        return callAsOperator(() -> archive.classifyEntry(request));
    }

    @Override
    public BridgeEntryResponse bridgeEntry(RepositoryCaller caller, BridgeEntryRequest request) {
        requireProcessAuthority(caller);
        Objects.requireNonNull(request, "request");
        if (bounded) {
            throw new RepositoryException(
                    RepositoryException.Code.UNSUPPORTED,
                    "Bridge execution is disabled in the bounded archive profile");
        }
        return callAsOperator(() -> archive.bridgeEntry(request));
    }

    @Override
    public RenditionStream readRendition(RepositoryCaller caller, ReadRenditionRequest request) {
        Objects.requireNonNull(request, "request");
        requireAccountAccess(caller, request.getAddress().getAccountId());
        if (bounded) {
            throw new RepositoryException(
                    RepositoryException.Code.UNSUPPORTED,
                    "Streaming rendition reads are disabled in the bounded archive profile");
        }
        return callAsOperator(() -> {
            final RenditionReadHeader[] headerHolder = new RenditionReadHeader[1];
            final ByteString[] dataHolder = new ByteString[] {ByteString.EMPTY};
            final Throwable[] errorHolder = new Throwable[1];
            reads.read(request, new StreamObserver<>() {
                @Override
                public void onNext(ReadRenditionResponse value) {
                    if (value.hasHeader()) {
                        headerHolder[0] = value.getHeader();
                    } else if (value.hasData()) {
                        dataHolder[0] = dataHolder[0].concat(value.getData());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    errorHolder[0] = t;
                }

                @Override
                public void onCompleted() {
                }
            });
            if (errorHolder[0] != null) {
                if (errorHolder[0] instanceof StatusRuntimeException sre) {
                    throw sre;
                }
                if (errorHolder[0] instanceof RuntimeException re) {
                    throw re;
                }
                throw new IllegalStateException("rendition read failed", errorHolder[0]);
            }
            RenditionReadHeader header = headerHolder[0];
            RenditionManifestEntry entry = RenditionManifestEntry.newBuilder()
                    .setRendition(header.getRendition())
                    .setSizeBytes(header.getSizeBytes())
                    .setSha256(header.getSha256())
                    .setState(RenditionState.RENDITION_STATE_PRESENT)
                    .build();
            return new RenditionStream(
                    header.getVersion(),
                    entry,
                    header.getOffset(),
                    header.getLength(),
                    header.getWholeObject(),
                    dataHolder[0].newInput());
        });
    }

    @Override
    public UploadResult uploadStream(
            RepositoryCaller caller,
            EntryAddress address,
            RenditionDescriptor descriptor,
            long size,
            String sha256,
            WriteAttribution attribution,
            String filename,
            FormatFact declared,
            ObjectStoreOrigin origin,
            InputStream body) throws IOException {
        requireProcessAuthority(caller);
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(body, "body");
        if (bounded) {
            throw new RepositoryException(
                    RepositoryException.Code.UNSUPPORTED,
                    "Streaming archive uploads are disabled in the bounded archive profile");
        }
        UploadRenditionHeader.Builder header = UploadRenditionHeader.newBuilder()
                .setAddress(address)
                .setRendition(descriptor)
                .setSizeBytes(size);
        if (sha256 != null && !sha256.isBlank()) {
            header.setExpectedSha256(sha256);
        } else {
            header.setOperationId("upload-" + UUID.randomUUID());
        }
        if (attribution != null) {
            header.setWrittenBy(attribution);
        }
        if (declared != null) {
            header.setDeclared(declared);
        }
        if (origin != null) {
            header.setOrigin(origin);
        }
        CompletableFuture<UploadRenditionResponse> future = new CompletableFuture<>();
        StreamObserver<UploadRenditionResponse> responseObserver = new StreamObserver<>() {
            private UploadRenditionResponse value;

            @Override
            public void onNext(UploadRenditionResponse response) {
                this.value = response;
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                if (value == null) {
                    future.completeExceptionally(Status.INTERNAL
                            .withDescription("upload completed without a response")
                            .asRuntimeException());
                } else {
                    future.complete(value);
                }
            }
        };
        try {
            UploadRenditionResponse response = ArchiveCallerHandoff.callWithCaller(
                    OPERATOR_CALLER,
                    OPERATOR_AUTH,
                    () -> {
                        RenditionUploads.FlowControlledUpload stream =
                                uploads.uploadFlowControlled(responseObserver);
                        long bytesRead = 0L;
                        try {
                            stream.onNext(UploadRenditionRequest.newBuilder()
                                    .setHeader(header.build())
                                    .build());
                            byte[] buffer = new byte[STREAM_CHUNK_BYTES];
                            while (bytesRead < size && !future.isDone()) {
                                if (!stream.awaitReady(UPLOAD_READY_TIMEOUT_NANOS)) {
                                    StatusRuntimeException timeout = Status.DEADLINE_EXCEEDED
                                            .withDescription("timed out waiting for archive upload stream readiness")
                                            .asRuntimeException();
                                    stream.onError(timeout);
                                    throw timeout;
                                }
                                if (future.isDone()) {
                                    break;
                                }
                                int read = body.read(buffer, 0, (int) Math.min(buffer.length, size - bytesRead));
                                if (read == -1) {
                                    break;
                                }
                                if (read > 0) {
                                    bytesRead += read;
                                    stream.onNext(UploadRenditionRequest.newBuilder()
                                            .setChunk(ByteString.copyFrom(buffer, 0, read))
                                            .build());
                                }
                            }
                            if (!future.isDone()) {
                                stream.onCompleted();
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            StatusRuntimeException cancelled = Status.CANCELLED
                                    .withDescription("archive upload interrupted")
                                    .withCause(interrupted)
                                    .asRuntimeException();
                            stream.onError(cancelled);
                            throw cancelled;
                        } catch (RuntimeException | IOException failure) {
                            stream.onError(failure);
                            throw failure;
                        }
                        try {
                            return future.get();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw Status.CANCELLED
                                    .withDescription("archive upload interrupted")
                                    .withCause(interrupted)
                                    .asRuntimeException();
                        } catch (ExecutionException failed) {
                            Throwable cause = failed.getCause();
                            if (cause instanceof StatusRuntimeException sre) {
                                throw sre;
                            }
                            if (cause instanceof IOException io) {
                                throw io;
                            }
                            if (cause instanceof RuntimeException re) {
                                throw re;
                            }
                            throw new IllegalStateException("archive upload failed", cause);
                        }
                    });
            return new UploadResult(
                    response.getEntryUuid(),
                    response.getVersion(),
                    response.getSha256(),
                    response.getSizeBytes(),
                    response.getObjectKey(),
                    response.getRootChecksum(),
                    response.getReplayed());
        } catch (StatusRuntimeException failure) {
            throw toRepositoryException(failure);
        }
    }

    @Override
    public ArchiveMutationReceipt mutateArchive(RepositoryCaller caller, ArchiveMutationRequest request) {
        requireProcessAuthority(caller);
        Objects.requireNonNull(request, "request");
        return callAsOperator(() -> archive.archiveMutation(request).getReceipt());
    }

    @Override
    public ArchiveMutationReceipt getArchiveMutation(RepositoryCaller caller, GetArchiveMutationRequest request) {
        requireProcessAuthority(caller);
        Objects.requireNonNull(request, "request");
        return callAsOperator(() -> archive.getArchiveMutation(request).getReceipt());
    }

    private static void requireProcessAuthority(RepositoryCaller caller) {
        Objects.requireNonNull(caller, "caller");
        if (!caller.processAuthority()) {
            throw new RepositoryException(
                    RepositoryException.Code.PERMISSION_DENIED,
                    "Repository account policy is not configured for this principal");
        }
    }

    private static void requireAccountAccess(RepositoryCaller caller, String accountId) {
        Objects.requireNonNull(caller, "caller");
        if (caller.processAuthority()) {
            return;
        }
        if (caller.accountIds().isEmpty()) {
            throw new RepositoryException(
                    RepositoryException.Code.PERMISSION_DENIED,
                    "The caller is bound to no account");
        }
        if (accountId != null && !accountId.trim().isEmpty() && !caller.accountIds().contains(accountId)) {
            throw new RepositoryException(
                    RepositoryException.Code.PERMISSION_DENIED,
                    "The caller is not bound to account '" + accountId + "'");
        }
    }

    private static <T> T callAsOperator(Supplier<T> action) {
        try {
            return ArchiveCallerHandoff.callWithCaller(OPERATOR_CALLER, OPERATOR_AUTH, action::get);
        } catch (StatusRuntimeException failure) {
            throw toRepositoryException(failure);
        } catch (IOException impossible) {
            throw new IllegalStateException("unexpected IOException", impossible);
        }
    }

    static RepositoryException toRepositoryException(StatusRuntimeException failure) {
        String description = failure.getStatus().getDescription();
        String message = description == null || description.isBlank()
                ? failure.getStatus().getCode().name()
                : description;
        RepositoryException.Code code = switch (failure.getStatus().getCode()) {
            case INVALID_ARGUMENT -> RepositoryException.Code.INVALID_ARGUMENT;
            case NOT_FOUND -> RepositoryException.Code.NOT_FOUND;
            case ALREADY_EXISTS, ABORTED -> RepositoryException.Code.CONFLICT;
            case PERMISSION_DENIED, UNAUTHENTICATED -> RepositoryException.Code.PERMISSION_DENIED;
            case FAILED_PRECONDITION -> RepositoryException.Code.FAILED_PRECONDITION;
            case RESOURCE_EXHAUSTED -> RepositoryException.Code.RESOURCE_EXHAUSTED;
            case CANCELLED -> RepositoryException.Code.CANCELLED;
            case UNIMPLEMENTED -> RepositoryException.Code.UNSUPPORTED;
            case DATA_LOSS -> RepositoryException.Code.DATA_LOSS;
            default -> RepositoryException.Code.UNAVAILABLE;
        };
        return new RepositoryException(code, message, failure);
    }

    static Status toGrpcStatus(RepositoryException failure) {
        Status base = switch (failure.code()) {
            case INVALID_ARGUMENT -> Status.INVALID_ARGUMENT;
            case NOT_FOUND -> Status.NOT_FOUND;
            case CONFLICT -> Status.ABORTED;
            case PERMISSION_DENIED -> Status.PERMISSION_DENIED;
            case FAILED_PRECONDITION -> Status.FAILED_PRECONDITION;
            case RESOURCE_EXHAUSTED -> Status.RESOURCE_EXHAUSTED;
            case CANCELLED -> Status.CANCELLED;
            case UNSUPPORTED -> Status.UNIMPLEMENTED;
            case DATA_LOSS -> Status.DATA_LOSS;
            default -> Status.UNAVAILABLE;
        };
        return base.withDescription(failure.getMessage());
    }

    /**
     * Server interceptor for {@code DocumentArchiveGrpcService} and
     * {@code DocumentArchiveMutationGrpcService}: enforces caller authentication,
     * account-binding checks on reads, process-authority checks on writes and
     * mutations, and bounded-profile admission gates.
     */
    static ServerInterceptor archiveInterceptor(
            RepoCallerBindings callerBindings,
            ArchivePutAdmission putAdmission,
            boolean bounded) {
        Objects.requireNonNull(callerBindings, "callerBindings");
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                String fullMethod = call.getMethodDescriptor().getFullMethodName();
                int slash = fullMethod.indexOf('/');
                String serviceName = slash > 0 ? fullMethod.substring(0, slash) : "";
                String methodName = slash > 0 ? fullMethod.substring(slash + 1) : fullMethod;

                boolean isMutationService = serviceName.endsWith("ArchiveMutationService");
                if (isMutationService && CallerContexts.CALLER.get() == null) {
                    call.close(
                            Status.UNAUTHENTICATED.withDescription("A trusted archive caller is required"),
                            new Metadata());
                    return new ServerCall.Listener<>() {};
                }
                if (bounded && ("BridgeEntry".equals(methodName)
                        || "UploadRendition".equals(methodName)
                        || "ReadRendition".equals(methodName))) {
                    call.close(
                            Status.UNIMPLEMENTED.withDescription(
                                    methodName + " is disabled in the bounded archive profile"),
                            new Metadata());
                    return new ServerCall.Listener<>() {};
                }

                RepositoryCaller caller = callerBindings.current();

                boolean isWrite = isMutationService
                        || "CreateArchive".equals(methodName)
                        || "PutEntry".equals(methodName)
                        || "UploadRendition".equals(methodName)
                        || "ClassifyEntry".equals(methodName)
                        || "BridgeEntry".equals(methodName);

                if (isWrite) {
                    if (!caller.processAuthority()) {
                        call.close(
                                Status.PERMISSION_DENIED.withDescription(
                                        "Repository account policy is not configured for this principal"),
                                new Metadata());
                        return new ServerCall.Listener<>() {};
                    }
                } else if (!caller.processAuthority() && caller.accountIds().isEmpty()) {
                    call.close(
                            Status.PERMISSION_DENIED.withDescription("The caller is bound to no account"),
                            new Metadata());
                    return new ServerCall.Listener<>() {};
                }

                Context operatorContext = Context.current().withValues(
                        CallerContexts.CALLER, OPERATOR_AUTH.caller(),
                        CallerContexts.AUTHENTICATED_CALLER, OPERATOR_AUTH,
                        ArchiveCallerHandoff.REPOSITORY_CALLER, OPERATOR_CALLER);
                ServerCall.Listener<ReqT> delegate =
                        Contexts.interceptCall(operatorContext, call, headers, next);

                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
                    private boolean closed;

                    @Override
                    public void onMessage(ReqT message) {
                        if (closed) {
                            return;
                        }
                        if (!isWrite && !caller.processAuthority()) {
                            String accountId = extractAccountId(message);
                            if (accountId != null && !accountId.trim().isEmpty()
                                    && !caller.accountIds().contains(accountId)) {
                                closed = true;
                                call.close(
                                        Status.PERMISSION_DENIED.withDescription(
                                                "The caller is not bound to account '" + accountId + "'"),
                                        new Metadata());
                                return;
                            }
                        }
                        if (putAdmission != null && message instanceof PutEntryRequest putReq) {
                            try (ArchivePutAdmission.Scope ignored = putAdmission.admit(putReq)) {
                                IN_PUT_ENTRY.set(true);
                                try {
                                    Context prev = operatorContext.attach();
                                    try {
                                        super.onMessage(message);
                                    } finally {
                                        operatorContext.detach(prev);
                                    }
                                } finally {
                                    IN_PUT_ENTRY.remove();
                                }
                            } catch (RepositoryException failure) {
                                closed = true;
                                call.close(toGrpcStatus(failure), new Metadata());
                            }
                            return;
                        }
                        Context prev = operatorContext.attach();
                        try {
                            super.onMessage(message);
                        } finally {
                            operatorContext.detach(prev);
                        }
                    }

                    @Override
                    public void onHalfClose() {
                        if (closed) {
                            return;
                        }
                        Context prev = operatorContext.attach();
                        try {
                            super.onHalfClose();
                        } finally {
                            operatorContext.detach(prev);
                        }
                    }
                };
            }
        };
    }

    private static String extractAccountId(Object message) {
        if (message instanceof GetArchiveRequest req) {
            return req.getAccountId();
        }
        if (message instanceof ListArchivesRequest req) {
            return req.getAccountId();
        }
        if (message instanceof GetArchiveStatsRequest req) {
            return req.getAccountId();
        }
        if (message instanceof ListEntriesRequest req) {
            return req.getAccountId();
        }
        if (message instanceof GetEntryRequest req) {
            return req.getAddress().getAccountId();
        }
        if (message instanceof GetEntryManifestRequest req) {
            return req.getAddress().getAccountId();
        }
        if (message instanceof ListVersionsRequest req) {
            return req.getAddress().getAccountId();
        }
        if (message instanceof ReadRenditionRequest req) {
            return req.getAddress().getAccountId();
        }
        return null;
    }
}
