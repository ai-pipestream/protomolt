package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import java.time.*;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL/proof checking; provider observations are synthetic and explicitly supplied by the fixture. */
@Testcontainers
class DocumentHistoricalMultiRevisionPublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void sameSourceNodeAndSlotCanSelectHistoricalAndCurrentRevisionsInOneAtomicCommand(boolean currentSecond) throws Exception {
        try (var c = context(POSTGRES)) {
            var oldDocument = Document.newBuilder().setDocId("multi-revision").setOwnership(OwnershipContext.newBuilder()
                    .setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()))
                    .setStructuredData(Any.pack(StringValue.of("old payload"), "type.test")).build();
            var oldProducer = WriteProvenance.newBuilder().setNodeId("old-producer").build();
            var first = DocumentSchemaRetentionFixture.prepare(c, true, false, oldDocument, "source", "first-drive", oldProducer);
            new DocumentSchemaPolicies(c.tx()).activate(first.batch().policy().policy(), 0, () -> {});
            var firstRevision = DocumentSchemaRetentionFixture.publishBound(c, first, (em, candidate) -> {},
                    (em, revision, manifest) -> first.retention().write(em, first.owner(), revision, () -> {}));
            var newDocument = oldDocument.toBuilder().setStructuredData(Any.pack(StringValue.of("new payload"), "type.test")).build();
            var newProducer = WriteProvenance.newBuilder().setNodeId("new-producer").build();
            var second = DocumentSchemaRetentionFixture.prepare(c, true, false, newDocument, "source", "second-drive", newProducer, 1L);
            var publication = new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), true, false);
            var advanced = publication.commit(CALLER, second.owner(), second.prepared(), Map.of(), Map.of("member", second.selected()),
                    second.batch(), () -> {}).getMembers(0);
            assertThat(advanced.getMutationRevision()).isEqualTo(2);
            var oldFixture = new Fixture(first, firstRevision);
            var newFixture = new Fixture(second, UUID.fromString(advanced.getRevisionId()));
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var oldHistory = reads.captureHistorical(CALLER, oldFixture.address(), oldFixture.revision());
            var newHistory = reads.captureHistorical(CALLER, newFixture.address(), newFixture.revision());
            var budget = new PayloadBudget(128L * 1024 * 1024);
            try {
                var oldMember = destination(member(oldFixture, oldHistory), "old");
                var newMember = destination(member(newFixture, newHistory), "new");
                var oldSelector = oldMember.getParts(0).getHistoricalReuse();
                var newSelector = newMember.getParts(0).getHistoricalReuse();
                assertThat(oldSelector.getSource()).isEqualTo(newSelector.getSource());
                assertThat(oldSelector.getSourceSlot()).isEqualTo(newSelector.getSourceSlot());
                assertThat(oldSelector.getRevisionId()).isNotEqualTo(newSelector.getRevisionId());
                assertThat(oldSelector.getObject().getObjectId()).isNotEqualTo(newSelector.getObject().getObjectId());
                assertThat(oldSelector.getObject().getSha256()).isNotEqualTo(newSelector.getObject().getSha256());
                if (currentSecond) newMember = newMember.toBuilder().setParts(0, newMember.getParts(0).toBuilder().setReuse(
                        PublicationReuse.newBuilder().setSource(DocumentRevisionCondition.newBuilder()
                                .setAddress(newSelector.getSource()).setExpectedMutationRevision(2))
                                .setSourceSlot(newSelector.getSourceSlot()).setObject(newSelector.getObject()))).build();
                var command = new DocumentPublicationCommand(first.command().intent().toBuilder().clearMembers()
                        .setOperationId(UUID.randomUUID().toString()).addMembers(oldMember).addMembers(newMember).build());
                var resolutions = new java.util.concurrent.atomic.AtomicInteger();
                try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, first.batch().policy(),
                        Map.of("old", DocumentPublicationCandidate.Mode.TYPED, "new", DocumentPublicationCandidate.Mode.TYPED),
                        Map.of("old", oldFixture.fragments(), "new", newFixture.fragments()),
                        currentSecond ? Optional.of(DocumentSchemaRetentionFixture.definition(Document.getDescriptor())) : Optional.empty(),
                        (m, occurrence) -> {
                            assertThat(currentSecond).isTrue(); assertThat(m.getMemberId()).isEqualTo("new");
                            resolutions.incrementAndGet(); return DocumentSchemaRetentionFixture.definition(StringValue.getDescriptor());
                        }, budget,
                        new DocumentRevisionAssembly.Limits(4_000_000, 32, 100, 100, 1_000_000), Instant.now(), CALLER,
                        currentSecond ? List.of(oldHistory) : List.of(newHistory, oldHistory), RepositoryReadControl.NONE)) {
                    var firstPlacement = first.prepared().members().getFirst().placement();
                    var secondPlacement = second.prepared().members().getFirst().placement();
                    var prepared = assessment.preparePhysical(Map.of(firstPlacement.drive().id(), firstPlacement,
                            secondPlacement.drive().id(), secondPlacement), Map.of(), Duration.ofMinutes(5), Map.of(), RepositoryReadControl.NONE);
                    var owner = new RepositoryOperationLedger(c.tx()).admitHistorical(CALLER,
                            new RepositoryOperationLedger.Key("account", "principal", command.operationId()), prepared,
                            UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                    new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, prepared);
                    oldHistory.close(); newHistory.close();
                    assertThat(oldHistory.isDrained()).isFalse(); assertThat(newHistory.isDrained()).isEqualTo(currentSecond);
                    var result = assessment.publish(CALLER, owner, prepared, Map.of(), new RepositorySchemaArtifacts(c.tx()),
                            publication, RepositoryReadControl.NONE);
                    assertThat(result.getMembersList()).extracting(DocumentPublishedRevision::getMemberId).containsExactlyElementsOf(
                            command.intent().getMembersList().stream().map(DocumentPublicationMember::getMemberId).toList());
                    assertThat(resolutions.get()).isEqualTo(currentSecond ? 1 : 0);
                    for (var published : result.getMembersList()) {
                        var expected = published.getMemberId().equals("old") ? oldSelector : newSelector;
                        var producer = published.getMemberId().equals("old") ? oldProducer : newProducer;
                        var target = command.intent().getMembersList().stream().filter(m -> m.getMemberId().equals(published.getMemberId()))
                                .findFirst().orElseThrow();
                        assertThat(published.getAddress()).isEqualTo(target.getDestination().getAddress());
                        var row = new DocumentLedger(c.tx()).findByNodeId(DocumentIds.nodeId(published.getAddress())).orElseThrow();
                        assertThat(row.driveName).isEqualTo(published.getMemberId().equals("old") ? "first-drive" : "second-drive");
                        assertThat(row.readManifest().getParts(0).getWrittenBy()).isEqualTo(producer);
                        assertThat(row.readManifest().getParts(0).getSha256()).isEqualTo(expected.getObject().getSha256());
                        var object = c.tx().readOnly(em -> em.createNativeQuery(
                                "SELECT object_id FROM document_revision_parts WHERE revision_id=:revision AND revision_ordinal=0")
                                .setParameter("revision", UUID.fromString(published.getRevisionId())).getSingleResult());
                        assertThat(object.toString()).isEqualTo(expected.getObject().getObjectId());
                    }
                    assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, command).result()).contains(result);
                    assertThat(new DocumentLedger(c.tx()).findByNodeId(first.prepared().members().getFirst().nodeId()).orElseThrow().mutationRevision)
                            .isEqualTo(2);
                }
            } finally {
                assertThat(budget.reservedBytes()).isZero();
                oldHistory.close(); newHistory.close();
                assertThat(oldHistory.awaitDrained(Duration.ofSeconds(1))).isTrue();
                assertThat(newHistory.awaitDrained(Duration.ofSeconds(1))).isTrue();
                oldHistory.release(); newHistory.release(); reads.fence(); reads.attestLocalQuiescence();
            }
        }
    }

    private static DocumentPublicationMember destination(DocumentPublicationMember source, String member) {
        return source.toBuilder().setMemberId(member).setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true)
                .setAddress(source.getDestination().getAddress().toBuilder().setGraphAddressId("destination-" + member))).build();
    }
}
