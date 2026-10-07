package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

/** Publish retained and freshly uploaded parts together, then read their exact provider versions. */
final class HistoricalClaimedMixedPublicationProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            RepositoryOperationLedger.Owner owner, DocumentHistoricalExecution execution,
            DocumentPublicationAssessment.Historical assessment,
            Map<String, DocumentSelectedAttemptLedger.Selected> selections,
            DocumentAssessmentRuntimeObserver.Observation observation, DocumentAssessmentCreation.Created stage,
            Map<Integer, ByteString> fragments) throws Exception {
        require(selections.size() == 1, "mixed publication selects one fresh attempt");
        var result = execution.publishAssessment(caller, assessment, selections, observation,
                new RepositorySchemaArtifacts(tx), new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false),
                stage, RepositoryReadControl.NONE);
        require(result.getMembersCount() == 1 && result.getOwnerGeneration() == owner.generation()
                && result.getCommandSha256().equals(command.sha256()), "mixed receipt binds exact owner and command");
        require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(result),
                "mixed receipt replays exactly");
        var member = result.getMembers(0);
        var revision = UUID.fromString(member.getRevisionId());
        var current = new DocumentLedger(tx).findByNodeId(
                ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getAddress())).orElseThrow();
        require(current.mutationRevision == member.getMutationRevision()
                && new DocumentPublicationLedger(tx).findForRead(current).orElseThrow().revisionId().equals(revision),
                "mixed result is the current revision");
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var retained = reads.captureHistorical(caller, member.getAddress(), revision);
        try {
            int historical = 0, uploaded = 0;
            try (var use = retained.use()) {
                require(use.plan().entries().size() == fragments.size(), "published revision retains every physical part");
                for (var entry : use.plan().entries()) {
                    var declaration = command.intent().getMembers(0).getParts(entry.revisionOrdinal());
                    var part = entry.part().part();
                    var binding = entry.part().binding();
                    var actual = provider.store().getBounded(binding.namespace(), part.key(), part.providerVersion(),
                            Math.toIntExact(part.size())).data();
                    require(Arrays.equals(actual, fragments.get(entry.revisionOrdinal()).toByteArray()),
                            "published exact provider version contains the supplied part bytes");
                    if (declaration.hasHistoricalReuse()) {
                        var original = declaration.getHistoricalReuse().getObject();
                        require(entry.objectId().toString().equals(original.getObjectId())
                                && part.providerVersion().equals(original.getProviderVersion())
                                && binding.generation().equals(original.getBackendGeneration())
                                && binding.profile().storageRealm().equals(original.getStorageRealm())
                                && binding.namespace().equals(original.getNamespace())
                                && part.key().equals(original.getObjectKey()), "historical part preserves physical identity");
                        historical++;
                    } else {
                        require(declaration.hasUpload(), "mixed fixture has only retained and uploaded parts");
                        require(part.sha256().equals(declaration.getUpload().getSha256())
                                && part.size() == declaration.getUpload().getSizeBytes(), "new part matches upload declaration");
                        require(tx.readOnly(em -> em.createNativeQuery("""
                                SELECT source_id FROM repository_physical_locations
                                WHERE object_id=:object AND source_kind='DOCUMENT_PART'
                                """).setParameter("object", entry.objectId()).getSingleResult())
                                .equals(selections.get("a").attempt()), "new part originates in the selected upload attempt");
                        uploaded++;
                    }
                }
            }
            require(historical > 0 && uploaded == 1, "one committed revision combines historical and new content");
        } finally {
            retained.close();
            require(retained.awaitDrained(Duration.ofSeconds(1)), "mixed read capture drains");
            retained.release(); reads.fence(); reads.attestLocalQuiescence();
        }
        try {
            execution.publishAssessment(caller, assessment, selections, observation,
                    new RepositorySchemaArtifacts(tx), new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false),
                    stage, RepositoryReadControl.NONE);
            throw new AssertionError("Mixed publication allowed another promotion");
        } catch (RepositoryException refused) {
            require(refused.code() == RepositoryException.Code.FAILED_PRECONDITION
                    && refused.getMessage().contains("requires reconciliation"), "mixed retry requires reconciliation");
        }
        System.out.println(caller.processAuthority() ? "CLAIMED_HISTORICAL_MIXED_PUBLICATION_OK"
                : "SCOPED_CLAIMED_HISTORICAL_MIXED_PUBLICATION_OK");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
