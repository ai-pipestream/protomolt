package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.archive.RepositorySinkAuthorizer;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.v2.WriterGrant;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Host {@link RepositorySinkAuthorizer} and gRPC {@link ServerInterceptor} binding
 * {@link RepositorySink} operations to {@link RepoCallerBindings}, {@link Catalog} writer grants,
 * and {@link ArchiveCallerHandoff}.
 *
 * <p>Background asynchronous conversions run strictly under the admitting caller's captured
 * identity, and {@link CapturedCaller#callForCommit} re-evaluates both {@link RepoCallerBindings}
 * and live {@link Catalog} writer grants immediately prior to commit so authority revoked during
 * long-running conversions fails with {@code PERMISSION_DENIED} before staging or committing bytes.
 */
final class RepoSinkAuthorization implements RepositorySinkAuthorizer {

    static final String UNBOUND_ACCOUNT_MESSAGE = "The caller is bound to no account";
    private static final AuthenticatedCaller OPERATOR_AUTH =
            AuthenticatedCaller.unbound(Caller.operator());

    private final RepoCallerBindings callerBindings;
    private final Supplier<? extends Catalog> catalogSupplier;

    RepoSinkAuthorization(
            RepoCallerBindings callerBindings, Supplier<? extends Catalog> catalogSupplier) {
        this.callerBindings = Objects.requireNonNull(callerBindings, "callerBindings");
        this.catalogSupplier = Objects.requireNonNull(catalogSupplier, "catalogSupplier");
    }

    /**
     * Rejects scoped callers bound to no account at the gRPC/Connect door with the same
     * {@code PERMISSION_DENIED} message as {@code DocumentService} and {@code ArchiveService}.
     */
    ServerInterceptor serverInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                RepositoryCaller caller = currentOriginCaller().resolveLive().repositoryCaller();
                if (!caller.processAuthority() && caller.accountIds().isEmpty()) {
                    call.close(
                            Status.PERMISSION_DENIED.withDescription(UNBOUND_ACCOUNT_MESSAGE),
                            new Metadata());
                    return new ServerCall.Listener<>() {};
                }
                return next.startCall(call, headers);
            }
        };
    }

    @Override
    public void checkAccountAccess(String accountId) {
        ResolvedCaller resolved = currentOriginCaller().resolveLive();
        requireAccountAccess(resolved.repositoryCaller(), accountId);
    }

    @Override
    public void checkWriteAccess(
            EntryAddress destination, String datasourceId, WriteAttribution writtenBy) {
        ResolvedCaller resolved = currentOriginCaller().resolveLive();
        requireLiveWriteAuthority(resolved.repositoryCaller(), destination, datasourceId, writtenBy);
    }

    @Override
    public CapturedCaller captureCaller() {
        OriginCaller origin = currentOriginCaller();
        return new CapturedCaller() {
            @Override
            public <T> T callForRead(String accountId, Supplier<T> action) {
                ResolvedCaller resolved = origin.resolveLive();
                requireAccountAccess(resolved.repositoryCaller(), accountId);
                return runUnderCaller(resolved, action);
            }

            @Override
            public <T> T callForCommit(
                    EntryAddress destination,
                    String datasourceId,
                    WriteAttribution writtenBy,
                    Supplier<T> commitAction) {
                ResolvedCaller resolved = origin.resolveLive();
                requireLiveWriteAuthority(
                        resolved.repositoryCaller(), destination, datasourceId, writtenBy);
                return runUnderCaller(resolved, commitAction);
            }
        };
    }

    private OriginCaller currentOriginCaller() {
        Caller caller = CallerContexts.current();
        AuthenticatedCaller auth =
                CallerContexts.authentication().orElseGet(() -> AuthenticatedCaller.unbound(caller));
        // Strip binding-synthesized credentials so re-evaluation sees live AccountBinding changes.
        AuthenticatedCaller rawAuth =
                auth.binding()
                                .filter(b -> RepoBindingWriters.ISSUER.equals(b.issuer()))
                                .isPresent()
                        ? AuthenticatedCaller.unbound(auth.caller())
                        : auth;
        return new OriginCaller(rawAuth);
    }

    private static void requireAccountAccess(RepositoryCaller caller, String accountId) {
        if (caller.processAuthority()) {
            return;
        }
        if (caller.accountIds().isEmpty()) {
            throw Status.PERMISSION_DENIED
                    .withDescription(UNBOUND_ACCOUNT_MESSAGE)
                    .asRuntimeException();
        }
        if (accountId == null || accountId.isBlank() || !caller.accountIds().contains(accountId)) {
            throw Status.PERMISSION_DENIED
                    .withDescription("Caller is not bound to account '" + accountId + "'")
                    .asRuntimeException();
        }
    }

    private void requireLiveWriteAuthority(
            RepositoryCaller caller,
            EntryAddress destination,
            String datasourceId,
            WriteAttribution writtenBy) {
        String accountId = destination.getAccountId();
        requireAccountAccess(caller, accountId);
        if (caller.processAuthority()) {
            return;
        }
        RepositoryCredentialBinding credBinding = caller.credentialBinding()
                .orElseThrow(() -> Status.PERMISSION_DENIED
                        .withDescription(
                                "Caller holds no writer credential for account '" + accountId + "'")
                        .asRuntimeException());
        Catalog catalog = Objects.requireNonNull(catalogSupplier.get(), "catalog");
        CatalogRecords.Credential registered = catalog
                .credential(credBinding.issuer(), credBinding.credentialId())
                .orElseThrow(() -> Status.PERMISSION_DENIED
                        .withDescription("The caller's writer credential is not registered")
                        .asRuntimeException());
        if (registered.revoked() || registered.generation() != credBinding.generation()) {
            throw Status.PERMISSION_DENIED
                    .withDescription("The caller's writer credential generation is not current")
                    .asRuntimeException();
        }
        String moduleId = writtenBy == null ? "" : writtenBy.getModule().trim();
        for (CatalogRecords.Grant held :
                catalog.grants(accountId, credBinding.issuer(), credBinding.credentialId())) {
            if (held.revoked()) {
                continue;
            }
            if (grantCovers(held.grant(), datasourceId, moduleId)) {
                return;
            }
        }
        throw Status.PERMISSION_DENIED
                .withDescription("The caller holds no active writer grant for this archive target")
                .asRuntimeException();
    }

    private static boolean grantCovers(WriterGrant grant, String datasourceId, String moduleId) {
        if (!moduleId.isEmpty()) {
            return grant.hasStep() && moduleId.equals(grant.getStep().getModuleId());
        }
        return grant.hasIntake() && datasourceId.equals(grant.getIntake().getDatasourceId());
    }

    private static <T> T runUnderCaller(ResolvedCaller resolved, Supplier<T> action) {
        try {
            return ArchiveCallerHandoff.callWithCaller(
                    resolved.repositoryCaller(),
                    resolved.repositoryCaller().processAuthority()
                            ? OPERATOR_AUTH
                            : resolved.authenticated(),
                    action::get);
        } catch (IOException unexpected) {
            throw new UncheckedIOException(unexpected);
        }
    }

    private record ResolvedCaller(
            AuthenticatedCaller authenticated, RepositoryCaller repositoryCaller) {}

    private final class OriginCaller {
        private final AuthenticatedCaller rawAuthenticated;

        private OriginCaller(AuthenticatedCaller rawAuthenticated) {
            this.rawAuthenticated = rawAuthenticated;
        }

        ResolvedCaller resolveLive() {
            AuthenticatedCaller withWriter = callerBindings.withBindingWriter(rawAuthenticated);
            RepositoryCaller bound = callerBindings.bind(withWriter);
            return new ResolvedCaller(withWriter, bound);
        }
    }
}
