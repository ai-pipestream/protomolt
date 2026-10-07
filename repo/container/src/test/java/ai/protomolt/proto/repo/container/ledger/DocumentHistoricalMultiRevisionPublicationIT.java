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
                var placements = Map.of(first.prepared().members().getFirst().placement().drive().id(),
                        first.prepared().members().getFirst().placement(), second.prepared().members().getFirst().placement().drive().id(),
                        second.prepared().members().getFirst().placement());
                verifyRetentionProjection(c, command, oldMember, oldHistory, newHistory, currentSecond, placements);
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

    private static void verifyRetentionProjection(Context c, DocumentPublicationCommand command,
            DocumentPublicationMember oldMember, DocumentReadLedger.PinnedHistory oldHistory,
            DocumentReadLedger.PinnedHistory newHistory, boolean currentSecond,
            Map<UUID, DocumentUploadPlan.Placement> placements) {
        // Repeated use in a different destination retains one source revision.
        // A different revision of the same node must remain a distinct root.
        var repeated = new DocumentPublicationCommand(command.intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString())
                .addMembers(destination(oldMember, "repeat-old")).build());
        var roots = DocumentPreparationHistoryRoots.roots(command);
        assertThat(roots).hasSize(currentSecond ? 1 : 2);
        assertThat(DocumentPreparationHistoryRoots.roots(repeated)).isEqualTo(roots);
        assertThat(roots.stream().map(DocumentPreparationHistoryRoots.Root::node).distinct()).hasSize(1);
        var revisions = roots.stream().map(root -> UUID.fromString(root.revision())).toList();
        byte[] sqlDigest = c.tx().readOnly(em -> (byte[]) em.createNativeQuery("""
                SELECT sha256(convert_to('protomolt/preparation-history/v1' || chr(10) ||
                  string_agg(node_id::text || '/' || revision_id::text || chr(10),'' ORDER BY node_id,revision_id),'UTF8'))
                FROM document_revision_publications WHERE revision_id IN (:revisions)
                """).setParameter("revisions", revisions).getSingleResult());
        assertThat(DocumentPreparationHistoryRoots.digest(roots)).isEqualTo(sqlDigest);
        List<DocumentHistoricalReferenceAdmission.Prepared> borrowed;
        try (var sources = DocumentHistoricalAssessmentSources.open(repeated, CALLER,
                currentSecond ? List.of(oldHistory) : List.of(newHistory, oldHistory), RepositoryReadControl.NONE)) {
            borrowed = sources.references(repeated, () -> {});
            assertThat(borrowed).hasSize(roots.size());
            assertThat(borrowed.stream().flatMap(source -> source.pins().stream()).toList())
                    .hasSize(roots.size())
                    .extracting(DocumentHistoricalSourcePin::revision).containsExactlyInAnyOrderElementsOf(revisions);
            assertThat(DocumentHistoricalReferenceAdmission.requireComplete(repeated, borrowed, () -> {}))
                    .isEqualTo(borrowed);
            var incomplete = borrowed.subList(1, borrowed.size());
            assertThatThrownBy(() -> DocumentHistoricalReferenceAdmission.requireComplete(repeated, incomplete, () -> {}))
                    .hasMessageContaining("Historical preparations differ from complete command");
            verifyHistoricalRegistration(c, repeated, sources, placements, roots.size());
        }
        // The projection is data; source admission still requires live Uses.
        assertThatThrownBy(() -> DocumentHistoricalReferenceAdmission.requireComplete(repeated, borrowed, () -> {}))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> borrowed.getFirst().pins()).isInstanceOf(IllegalStateException.class);
    }

    private static void verifyHistoricalRegistration(Context c, DocumentPublicationCommand command,
            DocumentHistoricalAssessmentSources sources, Map<UUID, DocumentUploadPlan.Placement> placements, int roots) {
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var seeds = DocumentPublicationSeeds.mint(key, command);
        var record = new DocumentPublicationPreparationRecord(key, command, seeds, placements, Duration.ofMinutes(5), 0);
        var budget = new PayloadBudget(64L * 1024 * 1024);
        var fault = new java.util.concurrent.atomic.AtomicInteger();
        var acknowledged = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
            if (fault.get() == 1 && registrationRows(c, "repository_operation_owners", key) == 1
                    && fault.compareAndSet(1, 2))
                throw new java.sql.SQLException("Historical registration acknowledgment lost", "08006");
        });
        var datasource = DocumentJdbcFaults.beforeCommit(acknowledged, connection -> {
            if (fault.get() != 0) return;
            try (var statement = connection.prepareStatement("SELECT count(*) FROM repository_operation_owners WHERE operation_id=?")) {
                statement.setObject(1, key.operationId());
                try (var rows = statement.executeQuery()) {
                    rows.next();
                    if (rows.getInt(1) == 1 && fault.compareAndSet(0, 1))
                        throw new java.sql.SQLException("Historical registration commit refused", "08006");
                }
            }
        });
        try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"))) {
            var registration = DocumentPublicationRegistration.historical(new Tx(emf), budget, record, sources,
                    UUID.randomUUID(), new DocumentPublicationScopeCalls(), new DriveLedger(c.tx()), RepositoryReadControl.NONE);
            var modes = command.intent().getMembersList().stream().collect(java.util.stream.Collectors.toMap(
                    DocumentPublicationMember::getMemberId, ignored -> DocumentPublicationCandidate.Mode.TYPED));
            var tables = List.of("repository_execution_claims", "repository_coordinator_bindings",
                    "repository_publication_preparations", "repository_preparation_history_sets",
                    "repository_preparation_history_roots", "repository_publication_modes", "repository_operations",
                    "repository_operation_owners");
            assertThatThrownBy(() -> registration.admitInitial(CALLER, modes, RepositoryReadControl.NONE))
                    .hasStackTraceContaining("Historical registration commit refused");
            for (var table : tables) assertThat(registrationRows(c, table, key)).as(table).isZero();
            assertThat(registration.mayHaveCommitted()).isTrue();
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(() -> registration.admitInitial(CALLER, modes, RepositoryReadControl.NONE))
                    .hasStackTraceContaining("Historical registration acknowledgment lost");
            for (var table : tables) assertThat(registrationRows(c, table, key)).as(table)
                    .isEqualTo(table.equals("repository_preparation_history_roots") ? roots : 1);
            var leases = registrationLeases(c, key);
            var owner = registration.admitInitial(CALLER, modes, RepositoryReadControl.NONE).orElseThrow();
            assertThat(owner.executionClaim()).isPresent();
            assertThat(registration.admitInitial(CALLER, modes, RepositoryReadControl.NONE)).contains(owner);
            c.tx().readOnly(em -> {
                assertThat(DocumentPreparationHistoryRoots.coverage(em, record,
                        DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record))))
                        .isEqualTo(DocumentPreparationHistoryRoots.Coverage.EXACT);
                assertThat(((Number) em.createNativeQuery("SELECT count(*) FROM repository_preparation_history_roots WHERE operation_id=:id")
                        .setParameter("id", key.operationId()).getSingleResult()).intValue()).isEqualTo(roots);
                return null;
            });
            assertThatThrownBy(() -> registration.start(CALLER, owner, Duration.ofMinutes(5), RepositoryReadControl.NONE))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(record::prepare).isInstanceOf(UnsupportedOperationException.class);
            sources.close();
            assertThatThrownBy(() -> registration.admitInitial(CALLER, modes, RepositoryReadControl.NONE))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(registrationLeases(c, key)).isEqualTo(leases);
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static long registrationRows(Context c, String table, RepositoryOperationLedger.Key key) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                .setParameter("id", key.operationId()).getSingleResult()).longValue());
    }

    private static List<?> registrationLeases(Context c, RepositoryOperationLedger.Key key) {
        return c.tx().readOnly(em -> Arrays.asList((Object[]) em.createNativeQuery("""
                SELECT c.lease_until,o.lease_until FROM repository_execution_claims c
                JOIN repository_operation_owners o USING(account_id,principal,operation_id) WHERE c.operation_id=:id
                """).setParameter("id", key.operationId()).getSingleResult()));
    }

    private static DocumentPublicationMember destination(DocumentPublicationMember source, String member) {
        return source.toBuilder().setMemberId(member).setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true)
                .setAddress(source.getDestination().getAddress().toBuilder().setGraphAddressId("destination-" + member))).build();
    }
}
