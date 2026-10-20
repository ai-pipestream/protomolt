package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.CredentialBinding;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.StepWriterScope;
import ai.protomolt.proto.repo.v2.WriterCredential;
import ai.protomolt.proto.repo.v2.WriterGrant;
import io.grpc.Context;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Rebinds v2 step saves that arrive with process authority (in-process or
 * operator-token callers, e.g. a co-mounted parse coordinator) to the
 * internal step credential, so the writer-authority grant check applies to
 * them. The catalog arrives through a supplier because the composition
 * assigns it after this object exists; the interceptor and the grant
 * installation only run once the services are serving, when the catalog is
 * set.
 */
final class RepoStepSaveRebinding {

    /** Deterministic credential the step-save interceptor binds co-mounted step writes to. */
    static final UUID INTERNAL_STEP_CREDENTIAL_ID = UUID.nameUUIDFromBytes(
            "repo-service:internal-step-credential".getBytes(StandardCharsets.UTF_8));
    /** Carries the account a rebound step save acts on from the interceptor to the binding. */
    private static final Context.Key<Set<String>> CURRENT_STEP_ACCOUNT =
            Context.key("repo-service-step-save-accounts");
    /** Carries the module ID of a rebound step save from the interceptor to the binding. */
    private static final Context.Key<String> CURRENT_STEP_MODULE =
            Context.key("repo-service-step-save-module");

    private final Supplier<PostgresCatalog> catalog;
    private final Set<String> stepGrantsInstalled =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    RepoStepSaveRebinding(Supplier<PostgresCatalog> catalog) {
        this.catalog = catalog;
    }

    /**
     * The interceptor that rebinds qualifying step saves. The service reads
     * the caller at half-close, so the rebound context wraps both
     * {@code onMessage} and {@code onHalfClose}.
     */
    ServerInterceptor interceptor() {
        return new ServerInterceptor() {
            @Override public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
                    private Context stepContext;
                    @Override public void onMessage(ReqT message) {
                        if (stepContext == null
                                && CallerContexts.current().unrestricted()
                                && message instanceof SaveDocumentRequest save
                                && save.getOwner().hasStep()) {
                            String accountId = save.getTarget().getAddress().getAccountId();
                            String moduleId = save.getOwner().getStep().getModuleId();
                            if (!accountId.isBlank() && !moduleId.isBlank()) {
                                ensureStepGrant(accountId, moduleId);
                                var stepCaller = Caller.scoped("internal-step-writer", Set.of());
                                stepContext = Context.current().withValues(
                                        CallerContexts.CALLER, stepCaller,
                                        CallerContexts.AUTHENTICATED_CALLER,
                                                new AuthenticatedCaller(stepCaller, Optional.of(
                                                        new CredentialBinding(
                                                                "repo-service", INTERNAL_STEP_CREDENTIAL_ID, 1))),
                                        CURRENT_STEP_ACCOUNT, Set.of(accountId),
                                        CURRENT_STEP_MODULE, moduleId);
                            }
                        }
                        if (stepContext != null) stepContext.run(() -> super.onMessage(message));
                        else super.onMessage(message);
                    }
                    @Override public void onHalfClose() {
                        if (stepContext != null) stepContext.run(() -> super.onHalfClose());
                        else super.onHalfClose();
                    }
                };
            }
        };
    }

    /**
     * v2 caller binding: the general case preserves the authenticated principal exactly
     * (process authority stays process authority, scoped callers stay scoped to no
     * repository accounts); the internal step credential rebinds to the account the
     * step-save interceptor recorded for this call.
     */
    RepositoryCaller bindCaller(AuthenticatedCaller auth) {
        var binding = auth.binding();
        if (binding.isPresent() && "repo-service".equals(binding.get().issuer())
                && INTERNAL_STEP_CREDENTIAL_ID.equals(binding.get().credentialId()) && binding.get().generation() == 1) {
            var accounts = CURRENT_STEP_ACCOUNT.get();
            var moduleId = CURRENT_STEP_MODULE.get();
            // The step's service identity lets the document's ACL name which pipeline
            // modules may advance it (intake writes those rules at creation).
            var identities = moduleId == null ? Set.<Principal>of()
                    : Set.of(Principal.newBuilder()
                            .setIdentityType("service").setIdentity(moduleId).build());
            return new RepositoryCaller(auth.caller().name(), false,
                    accounts == null ? Set.of() : accounts, identities,
                    Optional.of(new RepositoryCredentialBinding(
                            "repo-service", INTERNAL_STEP_CREDENTIAL_ID, 1)));
        }
        return new RepositoryCaller(auth.caller().name(), auth.caller().unrestricted(),
                Set.of(), Set.of(),
                binding.map(b -> new RepositoryCredentialBinding(
                        b.issuer(), b.credentialId(), b.generation())));
    }

    /** Installs the built-in step-writer grant for (account, module) once per process. */
    private void ensureStepGrant(String accountId, String moduleId) {
        if (!stepGrantsInstalled.add(accountId + "|" + moduleId)) return;
        try {
            PostgresCatalog catalog = this.catalog.get();
            var grant = WriterGrant.newBuilder()
                    .setGrantId("builtin-step:" + accountId + ":" + moduleId)
                    .setAccountId(accountId)
                    .setCredential(WriterCredential.newBuilder()
                            .setIssuer("repo-service")
                            .setCredentialId(INTERNAL_STEP_CREDENTIAL_ID.toString()))
                    .setStep(StepWriterScope.newBuilder().setModuleId(moduleId))
                    .build();
            boolean present = catalog.grants(accountId, "repo-service", INTERNAL_STEP_CREDENTIAL_ID).stream()
                    .anyMatch(existing -> !existing.revoked() && existing.grant().equals(grant));
            if (!present) {
                try {
                    catalog.installGrant(grant);
                } catch (RuntimeException concurrent) {
                    boolean nowPresent = catalog.grants(accountId, "repo-service", INTERNAL_STEP_CREDENTIAL_ID).stream()
                            .anyMatch(existing -> !existing.revoked() && existing.grant().equals(grant));
                    if (!nowPresent) throw concurrent;
                }
            }
        } catch (RuntimeException failure) {
            stepGrantsInstalled.remove(accountId + "|" + moduleId);
            throw failure;
        }
    }
}
