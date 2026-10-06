package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.engine.DocumentHistoricalOperations;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;

/** Production-JAR publication using real retained provider bytes and a post-commit JDBC fault. */
public final class HistoricalPublicationProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentPublishedRevision original, javax.sql.DataSource database) throws Exception {
        for (boolean lostAck : new boolean[]{false, true}) run(tx, provider, source, original, database, lostAck);
    }

    private static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            DocumentPublishedRevision original, javax.sql.DataSource database, boolean lostAck) throws Exception {
        var caller = new RepositoryCaller("principal", true);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var history = reads.captureHistorical(caller, original.getAddress(), UUID.fromString(original.getRevisionId()));
        var budget = new PayloadBudget(128_000_000);
        try {
            var current = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow();
            var member = source.candidate().toBuilder().clearParts().setDestination(DocumentRevisionCondition.newBuilder()
                    .setAddress(original.getAddress()).setExpectedMutationRevision(current.mutationRevision));
            var fragments = new HashMap<Integer, ByteString>();
            final List<PartManifestEntry> originalParts;
            try (var use = history.use()) {
                originalParts = use.plan().manifest().getPartsList();
                for (var entry : use.plan().entries()) {
                    var part = entry.part().part(); var binding = entry.part().binding();
                    var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()).build();
                    var object = PublicationObjectIdentity.newBuilder().setObjectId(entry.objectId().toString())
                            .setBackendGeneration(binding.generation()).setStorageRealm(binding.profile().storageRealm())
                            .setNamespace(binding.namespace()).setObjectKey(part.key()).setProviderVersion(part.providerVersion())
                            .setSizeBytes(part.size()).setSha256(part.sha256()).setContentType(part.contentType());
                    fragments.put(member.getPartsCount(), ByteString.copyFrom(provider.store().getBounded(
                            binding.namespace(), part.key(), part.providerVersion(), Math.toIntExact(part.size())).data()));
                    member.addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setHistoricalReuse(
                            PublicationHistoricalReuse.newBuilder().setSource(original.getAddress()).setRevisionId(original.getRevisionId())
                                    .setRevisionOrdinal(entry.revisionOrdinal()).setSourceSlot(slot).setObject(object)));
                }
            }
            var command = AssessmentMixedReuseProbe.command(member.build());
            var policy = new DocumentSchemaPolicies(tx).read("account", () -> {});
            final DocumentPublicationResult result;
            try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, policy,
                    Map.of("a", DocumentPublicationCandidate.Mode.TYPED), Map.of("a", fragments), Optional.empty(),
                    (selected, occurrence) -> { throw new AssertionError("Historical publication used current registry"); }, budget,
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(), caller,
                    List.of(history), RepositoryReadControl.NONE)) {
                var prepared = assessment.preparePhysical(Map.of(source.placement().drive().id(), source.placement()),
                        Map.of(), Duration.ofMinutes(5), Map.of(), RepositoryReadControl.NONE);
                var owner = new RepositoryOperationLedger(tx).admitHistorical(caller,
                        new RepositoryOperationLedger.Key("account", "principal", command.operationId()), prepared,
                        UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                new DocumentOperationUploadAdmission(tx, new DriveLedger(tx)).admit(caller, owner, prepared);
                history.close(); require(!history.isDrained(), "publication owns original pin through commit");
                if (lostAck) {
                    AssessmentStageFaultProbe.loseAcknowledgement(database, faultTx -> {
                        try {
                            assessment.publish(caller, owner, prepared, Map.of(), new RepositorySchemaArtifacts(tx),
                                    new DocumentPublicationCommit(faultTx, new DriveLedger(faultTx), true, false), RepositoryReadControl.NONE);
                        } catch (com.google.protobuf.InvalidProtocolBufferException invalid) { throw new IllegalStateException(invalid); }
                    });
                    result = new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow();
                } else result = assessment.publish(caller, owner, prepared, Map.of(), new RepositorySchemaArtifacts(tx),
                        new DocumentPublicationCommit(tx, new DriveLedger(tx), true, false), RepositoryReadControl.NONE);
            }
            require(budget.reservedBytes() == 0, "publication releases its bytes after success or lost acknowledgment");
            require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(result),
                    "authorized replay returns the exact durable result without publishing again");
            var published = result.getMembers(0);
            require(published.getMutationRevision() == current.mutationRevision + 1, "restore creates next current revision");
            require(!published.getRevisionId().equals(original.getRevisionId()), "restore never rewrites source revision");
            require(new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow().readManifest().getPartsList().equals(originalParts),
                    "original provider identities and producer timestamps remain unchanged");
            long attempts = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_part_attempts WHERE operation_id=:operation")
                    .setParameter("operation", command.operationId()).getSingleResult()).longValue());
            require(attempts == 0, "historical-only publication creates no provider upload attempt");
            var objects = tx.readOnly(em -> em.createNativeQuery("""
                    SELECT object_id FROM document_revision_parts WHERE revision_id=:revision ORDER BY revision_ordinal
                    """).setParameter("revision", UUID.fromString(published.getRevisionId())).getResultList());
            var expectedObjects = command.intent().getMembers(0).getPartsList().stream()
                    .map(part -> UUID.fromString(part.getHistoricalReuse().getObject().getObjectId())).toList();
            require(objects.equals(expectedObjects), "new revision retains exact original physical object IDs in ordinal order");
            try (var reader = new DocumentPartReader((generation, profile) -> {
                require(generation.equals(source.placement().generation()) && profile.equals(provider.profile()), "original backend identity");
                return provider.store();
            }, 2, 16_000_000, budget)) {
                var operations = new DocumentHistoricalOperations(reads, reader, budget);
                try (var restored = operations.readValidated(caller, published.getAddress(), UUID.fromString(published.getRevisionId()),
                        RepositoryReadControl.NONE)) {
                    require(restored.commandSha256().equals(ByteString.copyFrom(HexFormat.of().parseHex(command.sha256()))),
                            "new revision retains its own admission binding");
                    require(restored.document().getStructuredData().unpack(com.google.protobuf.StringValue.class)
                            .getValue().equals("retained payload"), "restored original version decodes with retained schemas");
                }
            }
            require(budget.reservedBytes() == 0, "restored decode releases budget");
        } finally {
            history.close(); require(history.awaitDrained(Duration.ofSeconds(1)), "historical publication source drains");
            history.release(); reads.fence(); reads.attestLocalQuiescence();
        }
        System.out.println(lostAck ? "HISTORICAL_PUBLICATION_LOST_ACK_OK" : "HISTORICAL_PUBLICATION_OK");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
