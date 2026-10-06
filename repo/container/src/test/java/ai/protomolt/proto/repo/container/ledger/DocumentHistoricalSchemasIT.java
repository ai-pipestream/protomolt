package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DocumentSchemaPolicy;
import ai.protomolt.proto.repo.v1.DocumentSchemaPolicyMode;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Historical schema replay reads retained bytes only; provider observations are synthetic fixtures. */
@Testcontainers
class DocumentHistoricalSchemasIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("historical-reader", true);

    @Test void replaysRetainedProofWithFreshReaderAndKeepsItsHistoricalPolicy() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            activate(c, f);
            var revision = publishTyped(c, f);
            var expected = f.batch().proofs().get("member");
            c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb)")
                        .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                        .executeUpdate();
            });
            var readerCaller = new RepositoryCaller("different-reader", false, java.util.Set.of("account"), java.util.Set.of());

            var advanced = DocumentAdmissionPolicy.of(expectedPolicy(f).toBuilder()
                    .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).build(), () -> {});
            new DocumentSchemaPolicies(c.tx()).activate(advanced, 1, () -> {});
            var freshReader = new DocumentHistoricalSchemas(new Tx(c.emf()));
            var actual = freshReader.check(readerCaller, f.command().intent().getMembers(0).getDestination().getAddress(),
                    revision, expected.fragments(), () -> {});

            assertThat(actual.commandSha256()).isEqualTo(expected.commandSha256());
            assertThat(actual.policySha256()).isEqualTo(expected.policySha256());
            assertThat(actual.policySha256()).isEqualTo(f.batch().policy().policy().sha256());
            assertThat(actual.document()).isEqualTo(expected.document());
            assertThat(actual.roots()).isEqualTo(expected.roots());
            assertThat(actual.references()).containsExactlyInAnyOrderElementsOf(expected.references());
            assertThat(actual.artifacts()).isEqualTo(expected.artifacts());
            assertThat(new DocumentSchemaPolicies(c.tx()).read("account", () -> {}).revision()).isEqualTo(2);
            long retainedPolicyRevision = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT policy_revision FROM document_revision_schema_admissions WHERE revision_id=:revision")
                    .setParameter("revision", revision).getSingleResult()).longValue());
            assertThat(retainedPolicyRevision).isEqualTo(1);
        }
    }

    @Test void replaysSameUrlOccurrencesFromTheirDistinctRetainedDefinitions() throws Exception {
        try (var c = context(POSTGRES)) {
            var coreType = recordType("historical_core.proto", "core_label");
            var parsedType = recordType("historical_parsed.proto", "parsed_label");
            String url = "type.test/archive.Record";
            var coreAny = com.google.protobuf.Any.newBuilder().setTypeUrl(url).setValue(
                    com.google.protobuf.DynamicMessage.newBuilder(coreType)
                            .setField(coreType.findFieldByName("core_label"), "core occurrence").build().toByteString()).build();
            var parsedAny = com.google.protobuf.Any.newBuilder().setTypeUrl(url).setValue(
                    com.google.protobuf.DynamicMessage.newBuilder(parsedType)
                            .setField(parsedType.findFieldByName("parsed_label"), "parsed occurrence").build().toByteString()).build();
            var ownership = ai.protomolt.proto.repo.v1.OwnershipContext.newBuilder()
                    .setAccountId("account").setDatasourceId("source")
                    .setSecurity(ai.protomolt.proto.repo.v1.DocumentSecurity.getDefaultInstance()).build();
            var document = ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("same-url-history")
                    .setOwnership(ownership).setStructuredData(coreAny)
                    .putParserResults("parser", ai.protomolt.proto.repo.v1.ParserResult.newBuilder()
                            .setParserName("parser").setParserVersion("1")
                            .setStatus(ai.protomolt.proto.repo.v1.ParseStatus.PARSE_STATUS_OK)
                            .setDocument(ai.protomolt.proto.repo.v1.ParserDocument.newBuilder().setShape(parsedAny)).build())
                    .build();
            var coreDefinition = DocumentSchemaRetentionFixture.definition(coreType, true);
            var parsedDefinition = DocumentSchemaRetentionFixture.definition(parsedType, true);
            assertThat(coreDefinition.metadata().getTypeUrl()).isEqualTo(parsedDefinition.metadata().getTypeUrl());
            assertThat(coreDefinition.metadata().getArtifactSha256()).isNotEqualTo(parsedDefinition.metadata().getArtifactSha256());
            var fixture = DocumentSchemaRetentionFixture.prepare(c, true, false, document, "same-url-node",
                    "same-url-history", null, null, occurrence -> switch (occurrence.root().getSlot().getPart()) {
                        case DOCUMENT_PART_CORE -> coreDefinition;
                        case DOCUMENT_PART_PARSED -> parsedDefinition;
                        default -> throw new AssertionError("Unexpected typed root part: " + occurrence.root().getSlot().getPart());
                    });
            activate(c, fixture);
            UUID revision = publishTyped(c, fixture);
            var prepared = fixture.batch().proofs().get("member");
            assertThat(prepared.roots()).hasSize(2);
            assertThat(prepared.references()).filteredOn(reference -> reference.typeUrl().equals(url))
                    .extracting(ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Reference::descriptorSha256)
                    .containsExactlyInAnyOrder(coreDefinition.metadata().getArtifactSha256(), parsedDefinition.metadata().getArtifactSha256());

            // A new reader can replay only the sealed SQL artifacts, even after active policy changes.
            var advanced = DocumentAdmissionPolicy.of(expectedPolicy(fixture).toBuilder()
                    .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).build(), () -> {});
            new DocumentSchemaPolicies(c.tx()).activate(advanced, 1, () -> {});
            var address = fixture.command().intent().getMembers(0).getDestination().getAddress();
            // DocumentHistoricalSchemas has no resolver dependency; this fresh instance can only load retained SQL artifacts.
            var replay = new DocumentHistoricalSchemas(new Tx(c.emf())).check(ADMIN, address, revision, prepared.fragments(), () -> {});
            assertThat(replay.roots()).hasSize(2);
            assertThat(replay.artifacts()).containsEntry(coreDefinition.metadata().getArtifactSha256(), coreDefinition.descriptors())
                    .containsEntry(parsedDefinition.metadata().getArtifactSha256(), parsedDefinition.descriptors());
            assertThat(replay.references()).filteredOn(reference -> reference.typeUrl().equals(url))
                    .extracting(ai.protomolt.proto.repo.admission.DocumentSchemaAdmission.Reference::descriptorSha256)
                    .containsExactlyInAnyOrder(coreDefinition.metadata().getArtifactSha256(), parsedDefinition.metadata().getArtifactSha256());

            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, address, revision);
            var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(32L * 1024 * 1024);
            try {
                for (var root : replay.roots()) {
                    var evidence = ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence.parseFrom(root.encoded().bytes());
                    var path = evidence.getOccurrences(0);
                    String descriptorHash = root.locator().getSlot().getPart() == ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE
                            ? coreDefinition.metadata().getArtifactSha256() : parsedDefinition.metadata().getArtifactSha256();
                    var selection = new ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Selection(
                            root.locatorSha256(), ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(path.toByteArray()));
                    try (var materialized = history.materializeFragment(root.ordinal(), replay.fragments().get(root.ordinal()), selection,
                            new ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Limits(
                                    4_000_000, 4_000_000, 16_000_000, 64, 4_000_000, 64), budget,
                            ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE)) {
                        var view = materialized.view(ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE);
                        assertThat(ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(view.descriptorArtifact().toByteArray()))
                                .isEqualTo(descriptorHash);
                        String field = root.locator().getSlot().getPart() == ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE
                                ? "core_label" : "parsed_label";
                        assertThat(view.value().getAllFields().keySet())
                                .extracting(com.google.protobuf.Descriptors.FieldDescriptor::getName).containsExactly(field);
                        assertThat(view.value().getField(view.value().getDescriptorForType().findFieldByName(field)))
                                .isEqualTo(field.equals("core_label") ? "core occurrence" : "parsed occurrence");
                    }
                }
                assertThat(budget.reservedBytes()).isZero();
            } finally {
                history.close();
                assertThat(history.awaitDrained(java.time.Duration.ofSeconds(5))).isTrue();
                history.release(); ledger.fence(); ledger.attestLocalQuiescence();
            }
        }
    }

    private static com.google.protobuf.Descriptors.Descriptor recordType(String fileName, String fieldName) throws Exception {
        var field = com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder().setName(fieldName).setNumber(1)
                .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
        var proto = com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder().setName(fileName)
                .setPackage("archive").setSyntax("proto3").addMessageType(
                        com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("Record").addField(field)).build();
        return com.google.protobuf.Descriptors.FileDescriptor.buildFrom(proto, new com.google.protobuf.Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Record");
    }

    @Test void wrongOrMissingFragmentsFailHistoricalReplay() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            var revision = publishTyped(c, f);
            var fragments = f.batch().proofs().get("member").fragments();
            Integer ordinal = fragments.keySet().iterator().next();
            var wrong = new HashMap<>(fragments);
            wrong.put(ordinal, ByteString.copyFromUtf8("different raw fragment"));
            var reader = new DocumentHistoricalSchemas(new Tx(c.emf()));
            assertDataLoss(() -> reader.check(ADMIN, address(f), revision, wrong, () -> {}));
            var missing = new HashMap<>(fragments);
            missing.remove(ordinal);
            assertDataLoss(() -> reader.check(ADMIN, address(f), revision, missing, () -> {}));
        }
    }

    @Test void deniedAndCrossAccountReadersCannotDistinguishExistingFromRandomRevision() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            var revision = publishTyped(c, f);
            var reader = new DocumentHistoricalSchemas(new Tx(c.emf()));
            var denied = new RepositoryCaller("denied", false, java.util.Set.of("account"), java.util.Set.of());
            assertSameNotFound(reader, denied, address(f), revision);
            assertSameNotFound(reader, denied, address(f), UUID.randomUUID());

            var wrongAccount = new RepositoryCaller("other-account-reader", false, java.util.Set.of("elsewhere"), java.util.Set.of());
            assertSameNotFound(reader, wrongAccount, address(f), revision);
            assertSameNotFound(reader, wrongAccount, address(f), UUID.randomUUID());
            var otherAddress = address(f).toBuilder().setAccountId("elsewhere").build();
            assertSameNotFound(reader, wrongAccount, otherAddress, revision);
            assertSameNotFound(reader, wrongAccount, otherAddress, UUID.randomUUID());
        }
    }

    @Test void opaqueHistoricalAdmissionIsExplicitlyUnsupported() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c, false);
            activate(c, f);
            var revision = DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                    (em, id, manifest) -> {});
            assertUnsupported(() -> new DocumentHistoricalSchemas(new Tx(c.emf())).check(ADMIN, address(f), revision,
                    Map.of(), () -> {}));
        }
    }

    @Test void historicalAdmissionWithoutV61ContainerRoleIsUnsupportedAfterV62Upgrade() throws Exception {
        try (var c = context(POSTGRES, "61")) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            activate(c, f);
            var revision = DocumentSchemaRetentionFixture.publishV61(c, f);
            String schema = c.tx().readOnly(em -> (String) em.createNativeQuery("SELECT current_schema()").getSingleResult());
            Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target("62").load().migrate();
            assertUnsupported(() -> new DocumentHistoricalSchemas(new Tx(c.emf())).check(ADMIN, address(f), revision,
                    f.batch().proofs().get("member").fragments(), () -> {}));
        }
    }

    @Test void cancellationNeverReturnsAHistoricalProof() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            var revision = publishTyped(c, f);
            var calls = new AtomicInteger();
            assertThatThrownBy(() -> new DocumentHistoricalSchemas(new Tx(c.emf())).check(ADMIN, address(f), revision,
                    f.batch().proofs().get("member").fragments(), () -> {
                        if (calls.incrementAndGet() >= 4) throw new CancellationException("test cancellation");
                    })).isInstanceOf(CancellationException.class);
            assertThat(calls.get()).isGreaterThanOrEqualTo(4);
        }
    }

    @Test void missingRetainedArtifactIsReportedAsHistoricalDataLoss() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            var revision = publishTyped(c, f);
            c.tx().inTransaction(em -> {
                // Simulate catalog loss while bypassing only PostgreSQL's test-schema integrity triggers.
                // Production application connections retain all guards and constraints.
                em.createNativeQuery("SET LOCAL session_replication_role='replica'").executeUpdate();
                em.createNativeQuery("""
                        DELETE FROM repository_schema_artifacts a USING document_revision_schema_artifacts r
                        WHERE r.revision_id=:revision AND a.account_id=r.account_id AND a.artifact_sha256=r.artifact_sha256
                        """).setParameter("revision", revision).executeUpdate();
            });
            assertDataLoss(() -> new DocumentHistoricalSchemas(new Tx(c.emf())).check(ADMIN, address(f), revision,
                    f.batch().proofs().get("member").fragments(), () -> {}));
        }
    }

    private static void activate(Context c, DocumentSchemaRetentionFixture.Fixture f) {
        new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
    }

    private static DocumentSchemaPolicy expectedPolicy(DocumentSchemaRetentionFixture.Fixture f) {
        return f.batch().policy().policy().definition();
    }

    private static UUID publishTyped(Context c, DocumentSchemaRetentionFixture.Fixture f) {
        return DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                (em, revision, manifest) -> f.retention().write(em, f.owner(), revision, () -> {}));
    }

    private static NodeAddress address(DocumentSchemaRetentionFixture.Fixture f) {
        return f.command().intent().getMembers(0).getDestination().getAddress();
    }

    private static void assertSameNotFound(DocumentHistoricalSchemas reader, RepositoryCaller caller,
            NodeAddress address, UUID revision) {
        var existing = catchThrowable(() -> reader.check(caller, address, revision, Map.of(), () -> {}));
        var random = catchThrowable(() -> reader.check(caller, address, UUID.randomUUID(), Map.of(), () -> {}));
        assertThat(existing).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertThat(random).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertThat(((RepositoryException) existing).getMessage()).isEqualTo(((RepositoryException) random).getMessage());
    }

    private static void assertDataLoss(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
    }

    private static void assertUnsupported(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(RepositoryException.class,
                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
    }
}
