package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
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

/** Real SQL, descriptors and admission; provider observations are explicitly synthetic fixture input. */
@Testcontainers
class DocumentHistoricalMixedMemberPublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void historicalAndFreshPartsPublishAsOneTypedMemberOnlyAfterUploadVerification(boolean verified) throws Exception {
        try (var c = context(POSTGRES)) {
            var document = Document.newBuilder().setDocId("mixed-member").setOwnership(OwnershipContext.newBuilder()
                    .setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()))
                    .setStructuredData(Any.pack(StringValue.of("retained value"), "type.test")).build();
            var oldProducer = WriteProvenance.newBuilder().setNodeId("retained-producer").build();
            var newProducer = WriteProvenance.newBuilder().setNodeId("parsed-producer").build();
            var original = DocumentSchemaRetentionFixture.prepare(c, true, false, document, "source", "storage", oldProducer);
            new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {}));
            var fixture = new Fixture(original, revision);
            var source = new DocumentLedger(c.tx()).findByNodeId(DocumentIds.nodeId(fixture.address())).orElseThrow();
            var sourceManifest = source.readManifest();
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), revision);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try {
                var historical = member(fixture, history);
                var parsed = Document.newBuilder().setDocId(document.getDocId()).putParserResults("parsed",
                        ParserResult.newBuilder().setDocument(ParserDocument.newBuilder().setShape(
                                Any.pack(StringValue.of("new parser value"), "type.test"))).build()).build().toByteString();
                int parsedOrdinal = historical.getPartsCount();
                var target = fixture.address().toBuilder().setGraphAddressId("mixed-" + UUID.randomUUID()).build();
                var mixed = historical.toBuilder().setDestination(DocumentRevisionCondition.newBuilder().setAddress(target).setIfAbsent(true))
                        .addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                                .setUpload(PublicationUpload.newBuilder().setSizeBytes(parsed.size()).setSha256(DocumentPartCodec.sha256Hex(parsed.toByteArray()))
                                        .setContentType("application/protobuf").setWrittenBy(newProducer))).build();
                var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).setMembers(0, mixed).build());
                var fragments = new HashMap<>(fixture.fragments()); fragments.put(parsedOrdinal, parsed);
                var lookups = new java.util.concurrent.atomic.AtomicInteger();
                try (var assessment = DocumentPublicationAssessment.prepareHistorical(command, original.batch().policy(),
                        Map.of("member", DocumentPublicationCandidate.Mode.TYPED), Map.of("member", fragments),
                        Optional.of(DocumentSchemaRetentionFixture.definition(Document.getDescriptor())),
                        (m, occurrence) -> {
                            assertThat(occurrence.ordinal()).isEqualTo(parsedOrdinal); lookups.incrementAndGet();
                            return DocumentSchemaRetentionFixture.definition(StringValue.getDescriptor(), true);
                        }, budget, new DocumentRevisionAssembly.Limits(4_000_000, 32, 100, 100, 100_000),
                        Instant.now(), CALLER, List.of(history), RepositoryReadControl.NONE)) {
                    assessment.inspect(access -> assertThat(access.snapshot().typed().get("member").rootCount()).isEqualTo(2), RepositoryReadControl.NONE);
                    var placement = original.prepared().members().getFirst().placement();
                    var prepared = assessment.preparePhysical(Map.of(placement.drive().id(), placement), Map.of("member", UUID.randomUUID()),
                            Duration.ofMinutes(5), Map.of("member", UUID.randomUUID()), RepositoryReadControl.NONE);
                    var owner = new RepositoryOperationLedger(c.tx()).admitHistorical(CALLER,
                            new RepositoryOperationLedger.Key("account", CALLER.principalName(), command.operationId()),
                            prepared, UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
                    var attempts = new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(CALLER, owner, prepared);
                    assertThat(attempts).hasSize(1);
                    var attempt = attempts.getFirst();
                    var selected = new DocumentSelectedAttemptLedger.Selected("member", 1, attempt.id(), attempt.token());
                    var uploads = prepared.members().getFirst().attempt().orElseThrow().uploads();
                    assertThat(uploads).hasSize(1); assertThat(uploads.getFirst().revisionOrdinal()).isEqualTo(parsedOrdinal);
                    var upload = uploads.getFirst().object();
                    if (verified) new DocumentSelectedAttemptLedger(c.tx()).verifyBatch(owner, selected,
                            List.of(new DocumentSelectedAttemptLedger.Observation(upload.objectKey(), upload.size(), upload.sha256(),
                                    upload.contentType(), "synthetic-parsed-version", "synthetic-parsed-etag")));
                    history.close(); assertThat(history.isDrained()).isFalse();
                    var commit = new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), true, false);
                    if (!verified) {
                        assertThatThrownBy(() -> assessment.publish(CALLER, owner, prepared, Map.of("member", selected),
                                new RepositorySchemaArtifacts(c.tx()), commit, RepositoryReadControl.NONE))
                                .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                        assertThat(new DocumentLedger(c.tx()).findByNodeId(DocumentIds.nodeId(target))).isEmpty();
                        assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, command).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
                        long admissions = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM document_revision_schema_admissions WHERE operation_id=:operation")
                                .setParameter("operation", command.operationId()).getSingleResult()).longValue());
                        assertThat(admissions).isZero();
                    } else {
                        var result = assessment.publish(CALLER, owner, prepared, Map.of("member", selected),
                                new RepositorySchemaArtifacts(c.tx()), commit, RepositoryReadControl.NONE);
                        assertThat(result.getMembersCount()).isEqualTo(1);
                        assertThat(result.getMembers(0).getAddress()).isEqualTo(target);
                        assertThat(new DocumentPublicationReplay(c.tx()).observe(CALLER, command).result()).contains(result);
                        var row = new DocumentLedger(c.tx()).findByNodeId(DocumentIds.nodeId(target)).orElseThrow();
                        assertThat(row.readManifest().getPartsCount()).isEqualTo(parsedOrdinal + 1);
                        for (int i = 0; i < parsedOrdinal; i++)
                            assertThat(row.readManifest().getParts(i)).isEqualTo(sourceManifest.getParts(i));
                        assertThat(row.readManifest().getParts(parsedOrdinal).getWrittenBy()).isEqualTo(newProducer);
                        assertThat(row.readManifest().getParts(parsedOrdinal).getObjectKey()).isEqualTo(upload.objectKey());
                        assertThat(row.readManifest().getParts(parsedOrdinal).getSizeBytes()).isEqualTo(parsed.size());
                        assertThat(row.readManifest().getParts(parsedOrdinal).getSha256()).isEqualTo(upload.sha256());
                        UUID published = UUID.fromString(result.getMembers(0).getRevisionId());
                        var reread = new DocumentHistoricalSchemas(c.tx()).check(CALLER, target, published, fragments, () -> {});
                        assertThat(reread.document().getStructuredData()).isEqualTo(document.getStructuredData());
                        assertThat(reread.document().getParserResultsOrThrow("parsed").getDocument().getShape())
                                .isEqualTo(Any.pack(StringValue.of("new parser value"), "type.test"));
                        assertThat(reread.fragments()).isEqualTo(fragments);
                        assertThat(reread.roots()).hasSize(2);
                        List<?> origins = c.tx().readOnly(em -> em.createNativeQuery("""
                                SELECT p.object_id,o.attempt_id,o.provider_version,p.revision_ordinal,o.content_type
                                FROM document_revision_parts p JOIN document_part_attempt_objects o ON o.physical_object_id=p.object_id
                                WHERE p.revision_id=:revision ORDER BY p.revision_ordinal
                                """).setParameter("revision", published).getResultList());
                        assertThat(origins).hasSize(parsedOrdinal + 1);
                        for (int i = 0; i < origins.size(); i++) {
                            var origin = (Object[]) origins.get(i); assertThat(((Number) origin[3]).intValue()).isEqualTo(i);
                            if (i == parsedOrdinal) {
                                assertThat(origin[1]).isEqualTo(selected.attempt()); assertThat(origin[2]).isEqualTo("synthetic-parsed-version");
                                assertThat(origin[4]).isEqualTo(upload.contentType());
                            } else assertThat(origin[0].toString()).isEqualTo(historical.getParts(i).getHistoricalReuse().getObject().getObjectId());
                        }
                        List<?> roots = c.tx().readOnly(em -> em.createNativeQuery(
                                "SELECT revision_ordinal FROM document_revision_schema_evidence WHERE revision_id=:revision ORDER BY revision_ordinal")
                                .setParameter("revision", published).getResultList());
                        assertThat(roots.stream().map(value -> ((Number) value).intValue()).toList()).containsExactly(0, parsedOrdinal);
                    }
                    assertThat(lookups.get()).isEqualTo(1);
                    assertThat(new DocumentLedger(c.tx()).findByNodeId(source.nodeId).orElseThrow().mutationRevision).isEqualTo(source.mutationRevision);
                }
                assertThat(budget.reservedBytes()).isZero();
            } finally { release(reads, history); }
        }
    }
}
