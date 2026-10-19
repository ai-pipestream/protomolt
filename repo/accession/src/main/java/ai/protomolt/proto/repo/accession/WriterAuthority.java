package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.admission.DocumentSchemaRetention;
import ai.protomolt.proto.repo.catalog.CatalogRecords.CredentialCheck;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v2.ProcessAuthorityEvidence;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.WriterGrant;
import ai.protomolt.proto.repo.v2.WriterGrantEvidence;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;

/**
 * Selects the write authority for one save (requirements §5, decisions 25-27).
 * A process-authority caller writes only as an intake owner and is recorded as
 * such. Every other caller needs its current credential generation and an
 * active grant in the account that covers the owner; eligible grants are taken
 * in unsigned UTF-8 grant ID order. The commit transaction rechecks both.
 */
final class WriterAuthority {
    /** The evidence for the revision and what the commit must recheck. */
    record Authorized(Optional<WriterGrantEvidence> grant, Optional<ProcessAuthorityEvidence> process,
                      List<String> grantIds, Optional<CredentialCheck> credential) {}

    private final Catalog catalog;
    private final DocumentSchemaRetention.Retained grantSchema;

    WriterAuthority(Catalog catalog, DocumentSchemaRetention.Retained grantSchema) {
        this.catalog = catalog;
        this.grantSchema = grantSchema;
    }

    DocumentSchemaRetention.Retained grantSchema() { return grantSchema; }

    /**
     * @param connector the document's resolved connector, if any
     * @param creating the save creates the document
     */
    Authorized authorize(RepositoryCaller caller, SaveOwner owner, String accountId, Optional<String> connector,
            boolean creating) {
        if (caller.processAuthority()) {
            if (!owner.hasIntake())
                throw denied("Process authority may save only as an intake owner");
            return new Authorized(Optional.empty(),
                    Optional.of(ProcessAuthorityEvidence.newBuilder().setPrincipal(caller.principalName()).build()),
                    List.of(), Optional.empty());
        }
        var binding = caller.credentialBinding()
                .orElseThrow(() -> denied("Saving requires a credential-bound caller or process authority"));
        var credential = catalog.credential(binding.issuer(), binding.credentialId())
                .orElseThrow(() -> denied("The caller's credential is not registered"));
        if (credential.revoked() || credential.generation() != binding.generation())
            throw denied("The caller's credential generation is not current");
        for (var grant : catalog.grants(accountId, binding.issuer(), binding.credentialId())) {
            if (grant.revoked() || !covers(grant.grant(), owner, connector)) continue;
            // Decision 31: creation needs allow_create; derived_from is lineage, not authority.
            if (creating && !grant.grant().getAllowCreate()) continue;
            var evidence = WriterGrantEvidence.newBuilder().setWriterGrantId(grant.grant().getGrantId())
                    .setWriterGrantSha256(grant.snapshotSha256()).setGrantSnapshot(grant.snapshot())
                    .setSnapshotSchema(grantSchema.reference()).setCredentialGeneration(binding.generation()).build();
            return new Authorized(Optional.of(evidence), Optional.empty(), List.of(grant.grant().getGrantId()),
                    Optional.of(new CredentialCheck(binding.issuer(), binding.credentialId(), binding.generation())));
        }
        if (!catalog.ownerKnown(accountId, grant -> covers(grant, owner, connector)))
            throw new AccessionException(Status.Code.FAILED_PRECONDITION, "No writer grant covers this owner in the account",
                    DocumentSaves.rejection(RejectionReason.REJECTION_REASON_OWNER_UNKNOWN,
                            "No writer grant covers this owner in the account"));
        throw denied(creating ? "The caller holds no active grant that may create this document"
                : "The caller holds no active grant for this owner");
    }

    /** The producer grant selected for one newly produced opaque occurrence. */
    record ProducerGrant(WriterGrantEvidence evidence, String grantId) {}

    /**
     * Selects the caller's active step grant authorizing one opaque producer for one
     * exact type URL (requirements §5, §9). Eligible grants are taken in unsigned
     * UTF-8 grant ID order, never database row order; there is no wildcard or prefix
     * matching. Empty when no grant the caller holds covers the producer and type:
     * the caller records that as OPAQUE_UNOWNED. Process-authority callers hold no
     * credential, so no grant ever matches them.
     */
    Optional<ProducerGrant> authorizeOpaqueProducer(RepositoryCaller caller,
            ai.protomolt.proto.repo.v1.WriteProvenance producer, String accountId, String typeUrl) {
        if (caller.processAuthority()) return Optional.empty();
        var binding = caller.credentialBinding()
                .orElseThrow(() -> denied("Saving requires a credential-bound caller or process authority"));
        var credential = catalog.credential(binding.issuer(), binding.credentialId())
                .orElseThrow(() -> denied("The caller's credential is not registered"));
        if (credential.revoked() || credential.generation() != binding.generation())
            throw denied("The caller's credential generation is not current");
        var owner = SaveOwner.newBuilder().setStep(producer).build();
        var grants = new java.util.ArrayList<>(catalog.grants(accountId, binding.issuer(), binding.credentialId()));
        grants.sort((left, right) -> SaveIntents.compareUtf8(left.grant().getGrantId(), right.grant().getGrantId()));
        for (var grant : grants) {
            if (grant.revoked() || !covers(grant.grant(), owner, Optional.empty())) continue;
            if (!grant.grant().getOpaqueTypeUrlsList().contains(typeUrl)) continue;
            var evidence = WriterGrantEvidence.newBuilder().setWriterGrantId(grant.grant().getGrantId())
                    .setWriterGrantSha256(grant.snapshotSha256()).setGrantSnapshot(grant.snapshot())
                    .setSnapshotSchema(grantSchema.reference()).setCredentialGeneration(binding.generation()).build();
            return Optional.of(new ProducerGrant(evidence, grant.grant().getGrantId()));
        }
        return Optional.empty();
    }

    static boolean covers(WriterGrant grant, SaveOwner owner, Optional<String> connector) {
        if (owner.hasIntake()) {
            if (!grant.hasIntake() || !grant.getIntake().getDatasourceId().equals(owner.getIntake().getDatasourceId()))
                return false;
            if (owner.getIntake().hasConnectorId()
                    && !connector.equals(Optional.of(owner.getIntake().getConnectorId()))) return false;
            return !grant.getIntake().hasConnectorId() || connector.equals(Optional.of(grant.getIntake().getConnectorId()));
        }
        if (!grant.hasStep() || !grant.getStep().getModuleId().equals(owner.getStep().getModuleId())) return false;
        return !grant.getStep().hasNodeId() || grant.getStep().getNodeId().equals(owner.getStep().getNodeId());
    }

    static AccessionException denied(String message) {
        return new AccessionException(Status.Code.PERMISSION_DENIED, message,
                DocumentSaves.rejection(RejectionReason.REJECTION_REASON_WRITE_DENIED, message));
    }
}
