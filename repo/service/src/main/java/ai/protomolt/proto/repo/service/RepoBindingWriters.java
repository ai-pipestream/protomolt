package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.spi.RepositoryCredentialBinding;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.WriterCredential;
import ai.protomolt.proto.repo.v2.WriterGrant;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The writer authority an account binding declares. A principal whose binding lists
 * {@link IntakeWriterScope intake writers} saves through one credential the host registers
 * for it under {@link #ISSUER} and one intake writer grant per listed scope, so the
 * document service's writer check (an active grant for the current credential generation,
 * rechecked at commit) applies to it exactly as to any credential-bound caller. Nothing is
 * granted beyond the list: a principal with no intake writers gets no credential and reads
 * only.
 *
 * <p>The grants follow the binding. The first time a principal is bound in this process,
 * and again whenever its binding changes, the grants of that credential in each bound
 * account are reconciled with the list: a grant whose scope is no longer listed is revoked,
 * and a listed scope with no active grant gets a new one. A grant ID is never reused, so a
 * scope listed again after removal gets the next ID of its sequence. Grants in an account
 * the principal is no longer bound to stay in the catalog but are unreachable, because every
 * account-addressed call refuses the caller first; binding the account again reconciles them.
 * Hosts sharing a catalog converge when they load the same bindings.
 */
final class RepoBindingWriters {

    /** The credential issuer of binding-declared writers: {@value}. */
    static final String ISSUER = "repo-account-bindings";

    private static final String GRANT_PREFIX = "account-binding:";

    private final Supplier<? extends Catalog> catalog;
    private final Map<String, Reconciled> reconciled = new ConcurrentHashMap<>();

    private record Reconciled(AccountBinding binding, Optional<RepositoryCredentialBinding> credential) {}

    /**
     * @param catalog the parts-based catalog; it may answer null while the host is
     *        composing or when the host mounts no parts-based store, and then no binding
     *        provisions a writer
     */
    RepoBindingWriters(Supplier<? extends Catalog> catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    /** The deterministic credential ID of one principal's binding-declared writer. */
    static UUID credentialId(String principal) {
        return UUID.nameUUIDFromBytes((ISSUER + "|" + principal).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The credential a bound principal saves with, after its grants match its binding.
     *
     * @param binding the principal's account binding
     * @return the credential binding, or empty when the binding lists no intake writer (or
     *         the host has no parts-based catalog to hold grants)
     */
    Optional<RepositoryCredentialBinding> credentialOf(AccountBinding binding) {
        Objects.requireNonNull(binding, "binding");
        Reconciled known = reconciled.get(binding.getPrincipal());
        if (known != null && known.binding().equals(binding)) return known.credential();
        synchronized (this) {
            known = reconciled.get(binding.getPrincipal());
            if (known != null && known.binding().equals(binding)) return known.credential();
            Catalog store = catalog.get();
            if (store == null) return Optional.empty();
            Optional<RepositoryCredentialBinding> credential = reconcile(store, binding);
            reconciled.put(binding.getPrincipal(), new Reconciled(binding, credential));
            return credential;
        }
    }

    private static Optional<RepositoryCredentialBinding> reconcile(Catalog store, AccountBinding binding) {
        String principal = binding.getPrincipal();
        UUID credentialId = credentialId(principal);
        Optional<CatalogRecords.Credential> credential = store.credential(ISSUER, credentialId);
        if (credential.isEmpty()) {
            // A principal that never wrote has nothing to revoke and needs no credential.
            if (binding.getIntakeWritersCount() == 0) return Optional.empty();
            try {
                store.registerCredential(ISSUER, credentialId, principal);
            } catch (RuntimeException concurrent) {
                // Another host sharing the catalog registered it first.
                if (store.credential(ISSUER, credentialId).isEmpty()) throw concurrent;
            }
            credential = store.credential(ISSUER, credentialId);
        }
        for (String accountId : binding.getAccountIdsList()) {
            List<WriterGrant> wanted = new ArrayList<>();
            for (IntakeWriterScope scope : binding.getIntakeWritersList()) {
                if (scope.getAccountId().equals(accountId)) wanted.add(grantOf(credentialId, scope));
            }
            reconcileAccount(store, accountId, credentialId, wanted);
        }
        if (binding.getIntakeWritersCount() == 0) return Optional.empty();
        // An administratively revoked or rotated credential keeps its catalog generation; the
        // writer check then refuses the save by name instead of this host renewing it.
        long generation = credential.orElseThrow().generation();
        return Optional.of(new RepositoryCredentialBinding(ISSUER, credentialId, generation));
    }

    private static void reconcileAccount(Catalog store, String accountId, UUID credentialId, List<WriterGrant> wanted) {
        List<CatalogRecords.Grant> held = store.grants(accountId, ISSUER, credentialId);
        List<WriterGrant> active = new ArrayList<>();
        for (CatalogRecords.Grant grant : held) {
            if (grant.revoked()) continue;
            if (wanted.contains(withoutId(grant.grant()))) {
                active.add(withoutId(grant.grant()));
            } else {
                store.revokeGrant(grant.grant().getGrantId());
            }
        }
        for (WriterGrant scope : wanted) {
            if (active.contains(scope)) continue;
            String series = GRANT_PREFIX + digest(scope) + ":";
            long issued = held.stream().filter(grant -> grant.grant().getGrantId().startsWith(series)).count();
            WriterGrant grant = scope.toBuilder().setGrantId(series + (issued + 1)).build();
            try {
                store.installGrant(grant);
            } catch (RuntimeException concurrent) {
                // Another host sharing the catalog issued the same next ID first.
                boolean installed = store.grants(accountId, ISSUER, credentialId).stream()
                        .anyMatch(existing -> !existing.revoked() && withoutId(existing.grant()).equals(scope));
                if (!installed) throw concurrent;
            }
        }
    }

    /** The grant one scope asks for, without its ID. */
    private static WriterGrant grantOf(UUID credentialId, IntakeWriterScope scope) {
        IntakeOrigin.Builder intake = IntakeOrigin.newBuilder().setDatasourceId(scope.getDatasourceId());
        if (scope.hasConnectorId()) intake.setConnectorId(scope.getConnectorId());
        return WriterGrant.newBuilder()
                .setAccountId(scope.getAccountId())
                .setCredential(WriterCredential.newBuilder().setIssuer(ISSUER).setCredentialId(credentialId.toString()))
                .setIntake(intake)
                .setAllowCreate(scope.getAllowCreate())
                .build();
    }

    private static WriterGrant withoutId(WriterGrant grant) {
        return grant.toBuilder().clearGrantId().build();
    }

    /** The first 128 bits of the scope's SHA-256, naming its grant series. */
    private static String digest(WriterGrant scope) {
        try {
            byte[] sha = MessageDigest.getInstance("SHA-256").digest(scope.toByteArray());
            return HexFormat.of().formatHex(sha, 0, 16);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
