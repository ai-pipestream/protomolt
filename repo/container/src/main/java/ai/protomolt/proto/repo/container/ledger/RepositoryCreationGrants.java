package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Internal exact-command creation authority. Installation does not admit or execute publication. */
final class RepositoryCreationGrants {
    static final class Prepared {
        private final RepositoryCaller grantee;
        private final DocumentPublicationCommand command;
        private final Map<UUID, DocumentUploadPlan.Placement> placements;
        private final long expiresAtEpochMicros;
        private final ByteString placementSha256;
        private Prepared(RepositoryCaller grantee, DocumentPublicationCommand command,
                Map<UUID, DocumentUploadPlan.Placement> placements, long expiresAtEpochMicros) {
            Objects.requireNonNull(grantee); Objects.requireNonNull(command);
            placements = Map.copyOf(placements);
            if (grantee.processAuthority() || grantee.credentialBinding().isEmpty()
                    || !grantee.accountIds().contains(command.intent().getAccountId()))
                throw new IllegalArgumentException("Creation grant requires a key-bound account caller");
            if (expiresAtEpochMicros <= 0) throw new IllegalArgumentException("Creation grant expiry must be positive epoch microseconds");
            if (!command.intent().getMembersList().stream().anyMatch(member -> member.getDestination().getIfAbsent()))
                throw new IllegalArgumentException("Creation grant requires an absent destination condition");
            var required = command.intent().getMembersList().stream().map(member -> UUID.fromString(member.getDriveId()))
                    .collect(java.util.stream.Collectors.toSet());
            if (!required.equals(placements.keySet())) throw new IllegalArgumentException("Grant placements differ from complete command");
            for (var entry : placements.entrySet()) {
                if (!entry.getKey().equals(entry.getValue().drive().id())
                        || !command.intent().getAccountId().equals(entry.getValue().drive().account()))
                    throw new IllegalArgumentException("Grant placement differs from account or drive");
            }
            this.grantee = grantee; this.command = command; this.placements = placements;
            this.expiresAtEpochMicros = expiresAtEpochMicros;
            this.placementSha256 = ByteString.copyFrom(DocumentPublicationPreparationCodec.placementDigest(placements));
        }
        RepositoryCaller grantee() { return grantee; }
        DocumentPublicationCommand command() { return command; }
        Map<UUID, DocumentUploadPlan.Placement> placements() { return placements; }
        long expiresAtEpochMicros() { return expiresAtEpochMicros; }
        ByteString placementSha256() { return placementSha256; }
        RepositoryOperationLedger.Key key() {
            return new RepositoryOperationLedger.Key(command.intent().getAccountId(), grantee.principalName(), command.operationId());
        }
        @Override public String toString() { return "CreationGrant[private]"; }
    }

    static Prepared prepare(RepositoryCaller grantee, DocumentPublicationCommand command,
            Map<UUID, DocumentUploadPlan.Placement> placements, long expiresAtEpochMicros) {
        return new Prepared(grantee, command, placements, expiresAtEpochMicros);
    }

    private final Tx tx;
    private final DriveLedger drives;
    RepositoryCreationGrants(Tx tx, DriveLedger drives) { this.tx = Objects.requireNonNull(tx); this.drives = Objects.requireNonNull(drives); }

    void install(RepositoryCaller administrator, Prepared prepared) {
        administrator(administrator); Objects.requireNonNull(prepared);
        tx.inTransaction(em -> { install(em, prepared); });
    }

    private void install(EntityManager em, Prepared prepared) {
        var key = prepared.key();
        int inserted = scope(em.createNativeQuery("""
                INSERT INTO repository_execution_scopes(account_id,principal,operation_id,claim_required)
                VALUES (:a,:p,:o,true) ON CONFLICT(account_id,principal,operation_id) DO NOTHING
                """), key).executeUpdate();
        if (inserted == 0) {
            // Never adopt an old scope. Only a previously installed exact live grant can be retried.
            long expiry = requireLive(em, prepared.grantee(), prepared.command(), prepared.placementSha256());
            if (expiry != prepared.expiresAtEpochMicros()) throw conflict();
            return;
        }
        for (var placement : prepared.placements().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            placement.getValue().drive().lock(em, drives);
            if (!ManagedBackendLedger.find(em, placement.getValue().generation())
                    .orElseThrow(RepositoryCreationGrants::conflict).equals(placement.getValue().profile()))
                throw conflict();
        }
        RepositoryCredentialAuthorities.requireLive(em, prepared.grantee());
        var binding = prepared.grantee().credentialBinding().orElseThrow();
        scope(em.createNativeQuery("""
                INSERT INTO repository_creation_grants(account_id,principal,operation_id,issuer,credential_id,credential_generation,
                    command_codec,command_version,command_sha256,placement_version,placement_sha256,expires_at_epoch_micros)
                VALUES (:a,:p,:o,:issuer,:id,:generation,:codec,:version,:command,1,:placement,:expiry)
                """), key).setParameter("issuer", binding.issuer()).setParameter("id", binding.credentialId())
                .setParameter("generation", binding.generation()).setParameter("codec", DocumentPublicationCommand.CODEC)
                .setParameter("version", DocumentPublicationCommand.ENCODING_VERSION)
                .setParameter("command", HexFormat.of().parseHex(prepared.command().sha256()))
                .setParameter("placement", prepared.placementSha256().toByteArray())
                .setParameter("expiry", prepared.expiresAtEpochMicros()).executeUpdate();
    }

    /**
     * Intended shared admission check; the caller owns its transaction and preceding domain locks.
     * Admission must derive the digest from its actual selected placements, checked under those locks,
     * never from a request-supplied digest or the persisted grant itself.
     */
    static long requireLive(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command, ByteString placementSha256) {
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        RepositoryCredentialAuthorities.requireLive(em, caller);
        var binding = caller.credentialBinding().orElseThrow();
        var rows = scope(em.createNativeQuery("SELECT * FROM lock_repository_creation_grant(:a,:p,:o)"), key).getResultList();
        if (rows.isEmpty()) throw unavailable();
        var row = (Object[]) rows.getFirst();
        if (!binding.issuer().equals(row[0]) || !binding.credentialId().equals(row[1])
                || binding.generation() != ((Number) row[2]).longValue()
                || !command.sha256().equals(HexFormat.of().formatHex((byte[]) row[3]))
                || !placementSha256.equals(ByteString.copyFrom((byte[]) row[4])) || !Boolean.TRUE.equals(row[6])) throw unavailable();
        return ((Number) row[5]).longValue();
    }

    /** Read visibility only: never establishes selected-placement or publication authority. */
    static void authorizeObservation(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command,
            boolean requireGrant, boolean unfinished) {
        if (caller.processAuthority()) return;
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        // Grant identity is immutable and cannot be attached after operation admission.
        // A plain read avoids taking a grant lock before its credential lock.
        var rows = scope(em.createNativeQuery("""
                SELECT issuer,credential_id,credential_generation,command_sha256
                FROM repository_creation_grants WHERE account_id=:a AND principal=:p AND operation_id=:o
                """), key).getResultList();
        if (rows.isEmpty()) {
            if (requireGrant) throw unavailable();
            return;
        }
        var binding = caller.credentialBinding().orElseThrow(RepositoryCreationGrants::unavailable);
        var identity = (Object[]) rows.getFirst();
        if (!binding.issuer().equals(identity[0]) || !binding.credentialId().equals(identity[1])
                || binding.generation() != ((Number) identity[2]).longValue()
                || !command.sha256().equals(HexFormat.of().formatHex((byte[]) identity[3]))) throw unavailable();
        RepositoryCredentialAuthorities.requireLive(em, caller);
        if (unfinished) {
            var current = scope(em.createNativeQuery("SELECT * FROM lock_repository_creation_grant(:a,:p,:o)"), key).getResultList();
            if (current.isEmpty() || !Boolean.TRUE.equals(((Object[]) current.getFirst())[6])) throw unavailable();
        }
    }

    void revoke(RepositoryCaller administrator, RepositoryOperationLedger.Key key) {
        administrator(administrator); Objects.requireNonNull(key);
        tx.inTransaction(em -> {
            if (scope(em.createNativeQuery("UPDATE repository_creation_grants SET revoked=true WHERE account_id=:a AND principal=:p AND operation_id=:o"), key)
                    .executeUpdate() != 1) throw unavailable();
        });
    }

    private static jakarta.persistence.Query scope(jakarta.persistence.Query query, RepositoryOperationLedger.Key key) {
        return query.setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId());
    }
    private static void administrator(RepositoryCaller caller) {
        if (caller == null || !caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Creation grant provisioning requires process authority");
    }
    private static RepositoryException unavailable() { return new RepositoryException(RepositoryException.Code.NOT_FOUND, "Creation grant is unavailable"); }
    private static RepositoryException conflict() { return new RepositoryException(RepositoryException.Code.CONFLICT, "Creation grant differs from installed authorization"); }
}
