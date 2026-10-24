package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AclIdentity;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.CredentialBinding;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.v1.Principal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The repository host's one mapping from an authenticated caller to a {@link RepositoryCaller}.
 * Process authority stays process authority. A scoped caller carries the accounts and ACL
 * identities its {@link AccountBindingResolver account binding} names, and its credential
 * identity when the resolver provisioned one; a principal the bindings do not name is
 * bound to no account and every account-addressed call refuses it. On the HTTP port, a
 * scoped caller without a credential of its own (a console session, an access-policy
 * bearer) whose binding lists intake writers is given the credential
 * {@link RepoBindingWriters} keeps for it as it is resolved ({@link #withBindingWriter}).
 * The document, drive and archive services all bind through this object, so the three
 * agree on who a caller is.
 */
final class RepoCallerBindings {

    private volatile AccountBindingResolver accounts = AccountBindingResolver.none();
    private volatile Optional<RepoBindingWriters> writers = Optional.empty();

    /** Replaces the binding source; composition roots set it before serving. */
    void use(AccountBindingResolver resolver) {
        this.accounts = Objects.requireNonNull(resolver, "resolver");
    }

    /** Provisions the writers bindings declare; the composition root sets it before serving. */
    void provisionWriters(RepoBindingWriters writers) {
        this.writers = Optional.of(Objects.requireNonNull(writers, "writers"));
    }

    /** The repository caller of the current gRPC call; in-process calls carry no context and run as the operator. */
    RepositoryCaller current() {
        var caller = CallerContexts.current();
        return bind(CallerContexts.authentication().orElseGet(() -> AuthenticatedCaller.unbound(caller)));
    }

    /**
     * The accounts one authenticated caller is bound to, in binding order; empty for a
     * principal the bindings do not name. Process authority is bound to every account and
     * answers empty here too, because the console never signs the operator in.
     */
    List<String> boundAccountIds(AuthenticatedCaller authentication) {
        Objects.requireNonNull(authentication, "authentication");
        if (authentication.caller().unrestricted()) return List.of();
        return accounts.bindingOf(authentication).map(found -> List.copyOf(found.getAccountIdsList())).orElse(List.of());
    }

    /**
     * The intake owners one authenticated caller's binding lets it save as; empty for
     * process authority (which the console never signs in) and for an unbound principal.
     */
    List<IntakeWriterScope> boundIntakeWriters(AuthenticatedCaller authentication) {
        Objects.requireNonNull(authentication, "authentication");
        if (authentication.caller().unrestricted()) return List.of();
        return accounts.bindingOf(authentication).map(found -> List.copyOf(found.getIntakeWritersList())).orElse(List.of());
    }

    /**
     * The authenticated caller with the writer credential its binding declares. A scoped
     * caller without a credential of its own whose binding lists intake writers comes back
     * carrying the credential {@link RepoBindingWriters} keeps for its principal, so every
     * later binding and the services' own identity check see one credential from the
     * transport onward. Process authority, a caller whose resolver provisioned its own
     * credential (never replaced), an unbound principal and a read-only binding come back
     * as they are.
     */
    AuthenticatedCaller withBindingWriter(AuthenticatedCaller authentication) {
        Objects.requireNonNull(authentication, "authentication");
        if (authentication.caller().unrestricted() || authentication.binding().isPresent() || writers.isEmpty()) {
            return authentication;
        }
        return accounts.bindingOf(authentication)
                .flatMap(binding -> writers.get().credentialOf(binding))
                .map(credential -> new AuthenticatedCaller(authentication.caller(), Optional.of(
                        new CredentialBinding(credential.issuer(), credential.credentialId(), credential.generation()))))
                .orElse(authentication);
    }

    /** The repository caller of one authenticated caller. */
    RepositoryCaller bind(AuthenticatedCaller authentication) {
        Objects.requireNonNull(authentication, "authentication");
        var caller = authentication.caller();
        var credential = authentication.binding().map(binding ->
                new RepositoryCredentialBinding(binding.issuer(), binding.credentialId(), binding.generation()));
        if (caller.unrestricted()) return new RepositoryCaller(caller.name(), true);
        Optional<AccountBinding> binding = accounts.bindingOf(authentication);
        if (credential.isEmpty() && binding.isPresent() && writers.isPresent()) {
            credential = writers.get().credentialOf(binding.get());
        }
        Set<String> accountIds = binding.map(found -> Set.copyOf(found.getAccountIdsList())).orElse(Set.of());
        Set<Principal> identities = new LinkedHashSet<>();
        binding.ifPresent(found -> {
            for (AclIdentity identity : found.getAclIdentitiesList()) {
                identities.add(Principal.newBuilder().setIdentityType(identity.getIdentityType())
                        .setIdentity(identity.getIdentity()).build());
            }
        });
        return new RepositoryCaller(caller.name(), false, accountIds, identities, credential);
    }
}
