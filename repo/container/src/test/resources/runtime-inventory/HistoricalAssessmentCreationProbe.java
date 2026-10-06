package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.time.*;
import java.util.*;

/** Real provider reads, production-JAR observation, and SQL CREATE of historical evidence. */
public final class HistoricalAssessmentCreationProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentPublishedRevision revision, javax.sql.DataSource database) throws Exception {
        for (boolean lostAck : new boolean[]{false, true}) run(tx, provider, source, revision, database, lostAck);
    }

    private static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentPublishedRevision revision, javax.sql.DataSource database, boolean lostAck) throws Exception {
        var caller = new RepositoryCaller("principal", true);
        var ledger = new DocumentReadLedger(tx, UUID.randomUUID());
        var history = ledger.captureHistorical(caller, revision.getAddress(), UUID.fromString(revision.getRevisionId()));
        var budget = new PayloadBudget(128_000_000);
        try {
            var current = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow();
            var member = source.candidate().toBuilder().clearParts().setDestination(DocumentRevisionCondition.newBuilder()
                    .setAddress(revision.getAddress()).setExpectedMutationRevision(current.mutationRevision));
            var fragments = new HashMap<Integer, ByteString>();
            try (var use = history.use()) {
                for (var entry : use.plan().entries()) {
                    var bound = entry.part(); var part = bound.part(); var binding = bound.binding();
                    var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()).build();
                    var object = PublicationObjectIdentity.newBuilder().setObjectId(entry.objectId().toString())
                            .setBackendGeneration(binding.generation()).setStorageRealm(binding.profile().storageRealm())
                            .setNamespace(binding.namespace()).setObjectKey(part.key()).setProviderVersion(part.providerVersion())
                            .setSizeBytes(part.size()).setSha256(part.sha256()).setContentType(part.contentType());
                    fragments.put(member.getPartsCount(), ByteString.copyFrom(provider.store().getBounded(
                            binding.namespace(), part.key(), part.providerVersion(), Math.toIntExact(part.size())).data()));
                    member.addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setHistoricalReuse(
                            PublicationHistoricalReuse.newBuilder().setSource(revision.getAddress()).setRevisionId(revision.getRevisionId())
                                    .setRevisionOrdinal(entry.revisionOrdinal()).setSourceSlot(slot).setObject(object)));
                }
            }
            var command = AssessmentMixedReuseProbe.command(member.build());
            var policy = new DocumentSchemaPolicies(tx).read("account", () -> {});
            var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")), () -> {});
            DocumentOperationUploadAdmission.Prepared borrowedPlan;
            try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, policy,
                    Map.of("a", DocumentPublicationCandidate.Mode.TYPED), Map.of("a", fragments), Optional.empty(),
                    (selected, occurrence) -> { throw new AssertionError("Historical CREATE must not use the registry"); }, budget,
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), caller,
                    List.of(history), RepositoryReadControl.NONE)) {
                var prepared = assessment.preparePhysical(Map.of(source.placement().drive().id(), source.placement()),
                        Map.of(), Duration.ofMinutes(5), Map.of(), RepositoryReadControl.NONE);
                borrowedPlan = prepared;
                var owner = new RepositoryOperationLedger(tx).admitHistorical(caller,
                        new RepositoryOperationLedger.Key("account", "principal", command.operationId()), prepared,
                        UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                new DocumentOperationUploadAdmission(tx, new DriveLedger(tx)).admit(caller, owner, prepared);
                history.close(); require(!history.isDrained(), "assessment retains historical source");
                var id = UUID.randomUUID(); var deadline = Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
                final DocumentAssessmentCreation.Created created;
                if (lostAck) {
                    AssessmentStageFaultProbe.loseAcknowledgement(database, faultTx -> {
                        try {
                            assessment.create(caller, owner, prepared, Map.of(), observation, new RepositorySchemaArtifacts(tx),
                                    new DocumentAssessmentCreation(faultTx, new DriveLedger(faultTx)), id, deadline, RepositoryReadControl.NONE);
                        } catch (com.google.protobuf.InvalidProtocolBufferException invalid) { throw new IllegalStateException(invalid); }
                    });
                    var discovered = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
                    require(discovered.stage().assessment().equals(id) && discovered.stage().retainUntil().equals(deadline),
                            "lost acknowledgement discovers original identity and deadline");
                    require(discovered.selections().isEmpty(), "historical-only assessment has no upload selections");
                    created = new DocumentAssessmentReconciliation(tx).observeRetained(caller, owner, command, discovered.selections(),
                            id, discovered.stage().manifestSha256(), deadline, budget, () -> {}).orElseThrow();
                    require(created.equals(discovered.stage()), "historical retained roots and slot snapshot reconcile exactly");
                } else created = assessment.create(caller, owner, prepared, Map.of(), observation,
                            new RepositorySchemaArtifacts(tx), new DocumentAssessmentCreation(tx, new DriveLedger(tx)),
                            id, deadline, RepositoryReadControl.NONE);
                require(created.assessment().equals(id), "exact historical assessment identity");
                var slots = tx.readOnly(em -> em.createNativeQuery("""
                        SELECT declaration,source_revision,source_node FROM document_assessment_slots WHERE assessment_id=:id
                        """).setParameter("id", id).getResultList());
                require(slots.size() == member.getPartsCount(), "all historical slots retained");
                for (var value : slots) {
                    var row = (Object[]) value;
                    require(row[0].equals("HISTORICAL_REUSE") && row[1].equals(UUID.fromString(revision.getRevisionId()))
                            && row[2].equals(source.node()), "exact historical source provenance");
                }
                long publications = tx.readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                        .setParameter("op", command.operationId()).getSingleResult()).longValue());
                require(publications == 0, "CREATE is not publication");
                require(new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow().mutationRevision == current.mutationRevision,
                        "historical CREATE does not advance destination");
            }
            require(budget.reservedBytes() == 0, "historical CREATE releases byte ownership");
            try {
                new RepositoryOperationLedger(tx).admitHistorical(caller,
                        new RepositoryOperationLedger.Key("account", "principal", command.operationId()), borrowedPlan,
                        UUID.randomUUID(), Duration.ofMinutes(5));
                throw new AssertionError("Expired borrowed source plan was admitted");
            } catch (IllegalStateException expected) {
                require(expected.getMessage().equals("Read plan use has ended"), "expired plan fails on its closed source Use");
            }
        } finally {
            history.close(); require(history.awaitDrained(Duration.ofSeconds(1)), "historical source drains"); history.release();
            ledger.fence(); ledger.attestLocalQuiescence();
        }
        System.out.println(lostAck ? "HISTORICAL_ASSESSMENT_LOST_ACK_OK" : "HISTORICAL_ASSESSMENT_CREATE_OK");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
