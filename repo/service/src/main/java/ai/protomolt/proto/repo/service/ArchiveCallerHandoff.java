package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.CredentialBinding;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.archive.DocumentArchiveBindings;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentRequest;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Propagates the authenticated caller resolved on an in-process archive entry door
 * across the gRPC hop into {@code DocumentService}.
 */
final class ArchiveCallerHandoff {

    @FunctionalInterface
    interface Action<T> {
        T run() throws IOException;
    }

    static final Context.Key<RepositoryCaller> REPOSITORY_CALLER =
            Context.key("protomolt-archive-repository-caller");
    private static final Context.Key<AtomicBoolean> UPLOAD_COMMITTED =
            Context.key("protomolt-archive-upload-committed");

    private static final Metadata.Key<String> CALL_KEY =
            Metadata.Key.of("protomolt-archive-call", Metadata.ASCII_STRING_MARSHALLER);
    private static final String PUBLIC_IDENTITY = "public";
    private static final ConcurrentMap<String, AuthenticatedCaller> HANDOFFS =
            new ConcurrentHashMap<>();

    private ArchiveCallerHandoff() {
    }

    /**
     * Runs {@code action} with {@code caller} and {@code authenticated} bound to the
     * current gRPC {@link Context}.
     *
     * @param caller the bound repository caller
     * @param authenticated the authenticated caller identity, or null to synthesize from {@code caller}
     * @param action the work to execute under that caller context
     * @param <T> the return type
     * @return the result of {@code action}
     * @throws IOException if {@code action} throws an {@link IOException}
     */
    static <T> T callWithCaller(
            RepositoryCaller caller,
            AuthenticatedCaller authenticated,
            Action<T> action) throws IOException {
        Objects.requireNonNull(caller, "caller");
        Objects.requireNonNull(action, "action");
        AuthenticatedCaller effective = authenticated != null
                ? authenticated
                : synthesizeAuthenticated(caller);
        Context context = Context.current().withValues(
                CallerContexts.CALLER, effective.caller(),
                CallerContexts.AUTHENTICATED_CALLER, effective,
                REPOSITORY_CALLER, caller,
                UPLOAD_COMMITTED, new AtomicBoolean(false));
        Context previous = context.attach();
        try {
            return action.run();
        } finally {
            context.detach(previous);
        }
    }

    private static AuthenticatedCaller synthesizeAuthenticated(RepositoryCaller caller) {
        if (caller.processAuthority()) {
            return AuthenticatedCaller.unbound(CallerContexts.current());
        }
        Caller scoped = Caller.scoped(caller.principalName(), Set.of());
        return caller.credentialBinding()
                .map(b -> new AuthenticatedCaller(
                        scoped,
                         java.util.Optional.of(new CredentialBinding(b.issuer(), b.credentialId(), b.generation()))))
                .orElseGet(() -> AuthenticatedCaller.unbound(scoped));
    }

    private static final class HandoffChannel extends Channel {
        private final Channel delegate;

        private HandoffChannel(Channel delegate) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            return delegate.newCall(methodDescriptor, callOptions);
        }

        @Override
        public String authority() {
            return delegate.authority();
        }
    }

    /**
     * Wraps {@code channel} with {@link #clientInterceptor()} if it is not already wrapped.
     *
     * @param channel the target gRPC channel
     * @return the caller-forwarding channel
     */
    static Channel interceptChannel(Channel channel) {
        Objects.requireNonNull(channel, "channel");
        if (channel instanceof HandoffChannel) {
            return channel;
        }
        return new HandoffChannel(ClientInterceptors.intercept(channel, clientInterceptor()));
    }

    /**
     * Client interceptor that forwards the active {@link CallerContexts} identity to the
     * in-process {@code DocumentService} server and stamps account-scoped access rules
     * when a non-process-authority caller uploads an archive entry.
     *
     * @return the client interceptor
     */
    static ClientInterceptor clientInterceptor() {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                    MethodDescriptor<ReqT, RespT> method,
                    CallOptions callOptions,
                    Channel next) {
                AuthenticatedCaller auth = CallerContexts.AUTHENTICATED_CALLER.get();
                if (auth == null) {
                    var current = CallerContexts.CALLER.get();
                    if (current != null) {
                        auth = AuthenticatedCaller.unbound(current);
                    }
                }
                RepositoryCaller repoCaller = REPOSITORY_CALLER.get();
                AtomicBoolean uploadCommitted = UPLOAD_COMMITTED.get();
                AuthenticatedCaller handedOff =
                        uploadCommitted != null && uploadCommitted.get() ? null : auth;
                return new ForwardingClientCall.SimpleForwardingClientCall<>(
                        next.newCall(method, callOptions)) {
                    private String handoffId;

                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        if (handedOff != null && !headers.containsKey(CALL_KEY)) {
                            handoffId = UUID.randomUUID().toString();
                            HANDOFFS.put(handoffId, handedOff);
                            headers.put(CALL_KEY, handoffId);
                        }
                        super.start(
                                new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(
                                        responseListener) {
                                    @Override
                                    public void onMessage(RespT message) {
                                        if (message instanceof UploadDocumentResponse
                                                && uploadCommitted != null) {
                                            uploadCommitted.set(true);
                                        }
                                        super.onMessage(message);
                                    }

                                    @Override
                                    public void onClose(Status status, Metadata trailers) {
                                        if (handoffId != null) {
                                            HANDOFFS.remove(handoffId);
                                        }
                                        super.onClose(status, trailers);
                                    }
                                },
                                headers);
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public void sendMessage(ReqT message) {
                        if (message instanceof SaveDocumentRequest saveReq
                                && saveReq.getTarget().getAddress().getGraphId()
                                        .startsWith(DocumentArchiveBindings.DEFINITION_GRAPH_PREFIX)
                                && saveReq.getExpected().getNone()
                                && !saveReq.getDocument().getOwnership().hasSecurity()) {
                            SaveDocumentRequest updated = withDefinitionSecurity(saveReq);
                            super.sendMessage((ReqT) updated);
                            return;
                        }
                        if (message instanceof UploadDocumentRequest uploadReq
                                && uploadReq.hasHeader()
                                && repoCaller != null
                                && !repoCaller.processAuthority()
                                && !uploadReq.getHeader().getSave().getDocument().getOwnership().hasSecurity()) {
                            UploadDocumentRequest updated = withUploaderSecurity(uploadReq, repoCaller);
                            super.sendMessage((ReqT) updated);
                            return;
                        }
                        super.sendMessage(message);
                    }

                    @Override
                    public void cancel(String message, Throwable cause) {
                        if (handoffId != null) {
                            HANDOFFS.remove(handoffId);
                        }
                        super.cancel(message, cause);
                    }
                };
            }
        };
    }

    /**
     * Server interceptor that restores the handed-off {@link AuthenticatedCaller} onto the
     * server call's gRPC {@link Context}.
     *
     * @return the server interceptor
     */
    static ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                String id = headers.get(CALL_KEY);
                if (id == null) {
                    return next.startCall(call, headers);
                }
                AuthenticatedCaller caller = HANDOFFS.remove(id);
                if (caller == null) {
                    call.close(
                            Status.UNAUTHENTICATED.withDescription("no caller was resolved for this archive call"),
                            new Metadata());
                    return new ServerCall.Listener<>() {
                    };
                }
                Context context = Context.current().withValues(
                        CallerContexts.CALLER, caller.caller(),
                        CallerContexts.AUTHENTICATED_CALLER, caller);
                return Contexts.interceptCall(context, call, headers, next);
            }
        };
    }

    private static SaveDocumentRequest withDefinitionSecurity(SaveDocumentRequest request) {
        var security = DocumentSecurity.newBuilder()
                .addPermissions(AccessRule.newBuilder()
                        .setIdentityType(PUBLIC_IDENTITY)
                        .setIdentity(PUBLIC_IDENTITY)
                        .setAccess(Access.ACCESS_READ))
                .build();
        var document = request.getDocument().toBuilder();
        document.getOwnershipBuilder().setSecurity(security);
        return request.toBuilder().setDocument(document).build();
    }

    /**
     * Stamps READ and WRITE access rules for each ACL identity presented by a scoped
     * caller, matching {@code UploadHttpHandler.uploadSecurity}. A scoped caller that
     * presents no ACL identities writes no rules.
     */
    private static UploadDocumentRequest withUploaderSecurity(
            UploadDocumentRequest request, RepositoryCaller caller) {
        if (caller.identities().isEmpty()) {
            return request;
        }
        var security = DocumentSecurity.newBuilder();
        caller.identities().stream()
                .sorted(Comparator.comparing(Principal::getIdentityType).thenComparing(Principal::getIdentity))
                .forEach(identity -> {
                    for (Access access : new Access[] {Access.ACCESS_READ, Access.ACCESS_WRITE}) {
                        security.addPermissions(AccessRule.newBuilder()
                                .setIdentityType(identity.getIdentityType())
                                .setIdentity(identity.getIdentity())
                                .setDisplayName(caller.principalName())
                                .setAccess(access));
                    }
                });
        var header = request.getHeader().toBuilder();
        header.getSaveBuilder().getDocumentBuilder().getOwnershipBuilder().setSecurity(security);
        return request.toBuilder().setHeader(header).build();
    }
}
