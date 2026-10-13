package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrencePath;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Historical schema qualification: retained descriptors must decode published revisions
 * with fresh host state and no live registry on the retained-only path. Provider
 * observations remain the synthetic fixture family used by the neighboring suites.
 */
@Testcontainers
class DocumentHistoricalRetainedQualificationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("historical-reader", true);
    private static final RepositoryCaller PRINCIPAL = new RepositoryCaller("principal", true);
    private static final DocumentSchemaMaterialization.Limits LIMITS =
            new DocumentSchemaMaterialization.Limits(4_000_000, 4_000_000, 16_000_000, 64, 4_000_000, 64);

    // Scenarios 1, 3 and 6: fresh factory/ledger state decodes nested retained occurrences
    // (repeated under CORE, map under PARSED) whose custom types span a multi-file closure
    // and have no generated classes. The publish-time resolver is never consulted again.
    @Test void freshHostDecodesNestedRetainedClosureWithoutLiveResolution() throws Exception {
        try (var c = context(POSTGRES)) {
            var types = walkTypes(false);
            var core = rootAny(types, "core-single", "core-first", "core-second");
            var parsed = rootAny(types, "parsed-single", "parsed-first", "parsed-second");
            var document = document("qual-fresh", core).toBuilder()
                    .putParserResults("parser", ParserResult.newBuilder().setParserName("parser").setParserVersion("1")
                            .setStatus(ParseStatus.PARSE_STATUS_OK)
                            .setDocument(ParserDocument.newBuilder().setShape(parsed)).build())
                    .build();
            var resolver = new CountingResolver(types);
            var f = DocumentSchemaRetentionFixture.prepare(c, true, false, document, "qual-fresh-node",
                    "qual-fresh-drive", null, null, resolver);
            activate(c, f);
            var revision = publishTyped(c, f);
            var proof = f.batch().proofs().get("member");
            assertThat(proof.roots()).hasSize(2);
            assertThat(resolver.calls()).isPositive();
            int resolutionsAtPublish = resolver.calls();

            try (var fresh = freshHost(c)) {
                var ledger = new DocumentReadLedger(fresh.tx(), UUID.randomUUID());
                var wrongAccount = new RepositoryCaller("other", false, Set.of("elsewhere"), Set.of());
                assertCode(() -> ledger.captureHistorical(wrongAccount, address(f), revision),
                        RepositoryException.Code.NOT_FOUND);
                var history = ledger.captureHistorical(ADMIN, address(f), revision);
                var budget = new PayloadBudget(32L * 1024 * 1024);
                try {
                    var coreRepeated = materialize(history, proof, DocumentPart.DOCUMENT_PART_CORE,
                            path -> isRepeatedChild(path, 1), budget);
                    assertChild(coreRepeated, "core-second");
                    var parsedMap = materialize(history, proof, DocumentPart.DOCUMENT_PART_PARSED,
                            path -> isMapChild(path, "two"), budget);
                    assertChild(parsedMap, "parsed-second");
                    var coreSingle = materialize(history, proof, DocumentPart.DOCUMENT_PART_CORE,
                            DocumentHistoricalRetainedQualificationIT::isSingleChild, budget);
                    assertChild(coreSingle, "core-single");
                } finally {
                    release(ledger, history);
                }
                assertThat(budget.reservedBytes()).isZero();
                // Retained-only path: the live resolver boundary saw no lookup after publication.
                assertThat(resolver.calls()).isEqualTo(resolutionsAtPublish);
            }
        }
    }

    // Scenario 2: two revisions of one address under one type URL keep distinct pinned definitions.
    @Test void twoRevisionsSameTypeUrlKeepTheirOwnPinnedDefinitions() throws Exception {
        try (var c = context(POSTGRES)) {
            var v1 = recordType("record_v1.proto", "first_label");
            var v2 = recordType("record_v2.proto", "second_label");
            var definitionV1 = DocumentSchemaRetentionFixture.definition(v1, true);
            var definitionV2 = DocumentSchemaRetentionFixture.definition(v2, true);
            assertThat(definitionV1.metadata().getTypeUrl()).isEqualTo(definitionV2.metadata().getTypeUrl());
            assertThat(definitionV1.metadata().getArtifactSha256())
                    .isNotEqualTo(definitionV2.metadata().getArtifactSha256());

            var first = DocumentSchemaRetentionFixture.prepare(c, true, false,
                    document("qual-two-revisions", recordAny(v1, "first_label", "revision one value")),
                    "source", "first-drive", null, null, occurrence -> definitionV1);
            activate(c, first);
            var revisionOne = publishTyped(c, first);

            var second = DocumentSchemaRetentionFixture.prepare(c, true, false,
                    document("qual-two-revisions", recordAny(v2, "second_label", "revision two value")),
                    "source", "second-drive", null, 1L, occurrence -> definitionV2);
            var advanced = new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), true, false)
                    .commit(PRINCIPAL, second.owner(), second.prepared(), Map.of(),
                            Map.of("member", second.selected()), second.batch(), () -> {})
                    .getMembers(0);
            var revisionTwo = UUID.fromString(advanced.getRevisionId());
            assertThat(revisionTwo).isNotEqualTo(revisionOne);

            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());
            var budget = new PayloadBudget(32L * 1024 * 1024);
            assertRecord(ledger, address(first), revisionOne, first.batch().proofs().get("member"),
                    "first_label", "revision one value", definitionV1.metadata().getArtifactSha256(), budget);
            assertRecord(ledger, address(second), revisionTwo, second.batch().proofs().get("member"),
                    "second_label", "revision two value", definitionV2.metadata().getArtifactSha256(), budget);
            var wrongAccount = new RepositoryCaller("other", false, Set.of("elsewhere"), Set.of());
            for (var revision : List.of(revisionOne, revisionTwo)) {
                assertCode(() -> ledger.captureHistorical(wrongAccount, address(first), revision),
                        RepositoryException.Code.NOT_FOUND);
            }
            ledger.fence(); ledger.attestLocalQuiescence();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    // Scenario 5: one missing nested retained artifact fails materialization as data loss,
    // even though a live resolver can still resolve the same type URL for new admission.
    // The live (different) definition is never substituted for the pinned historical proof.
    @Test void missingNestedArtifactFailsMaterializationAndLiveDefinitionCannotSubstitute() throws Exception {
        try (var c = context(POSTGRES)) {
            var types = walkTypes(false);
            var childDefinition = DocumentSchemaRetentionFixture.definition(types.child(), true);
            var f = DocumentSchemaRetentionFixture.prepare(c, true, false,
                    document("qual-damaged", rootAny(types, "single", "first", "second")),
                    "qual-damaged-node", "qual-damaged-drive", null, null, new CountingResolver(types));
            activate(c, f);
            var revision = publishTyped(c, f);
            var proof = f.batch().proofs().get("member");

            // A working live resolver resolves the same type URL to a different current definition.
            var liveTypes = walkTypes(true);
            var liveResolver = new CountingResolver(liveTypes);
            var live = DocumentSchemaRetentionFixture.prepare(c, true, false,
                    document("qual-live", rootAny(liveTypes, "live-single", "live-first", "live-second")),
                    "qual-live-node", "qual-live-drive", null, null, liveResolver);
            assertThat(liveResolver.calls()).isPositive();
            var liveChildSha = DocumentSchemaRetentionFixture.definition(liveTypes.child(), true)
                    .metadata().getArtifactSha256();
            assertThat(liveChildSha).isNotEqualTo(childDefinition.metadata().getArtifactSha256());
            assertThat(live.batch().proofs().get("member").references())
                    .anySatisfy(reference -> assertThat(reference.descriptorSha256()).isEqualTo(liveChildSha));

            // Damage exactly the nested child artifact payload pinned by the published revision.
            // Since V120 the descriptor bytes live in the payload relation keyed by identity.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SET LOCAL session_replication_role='replica'").executeUpdate();
                int deleted = em.createNativeQuery("""
                        DELETE FROM repository_schema_artifact_payloads
                        WHERE account_id='account' AND artifact_sha256=decode(:sha,'hex')
                        """).setParameter("sha", childDefinition.metadata().getArtifactSha256()).executeUpdate();
                assertThat(deleted).isEqualTo(1);
            });

            try (var fresh = freshHost(c)) {
                assertCode(() -> new DocumentHistoricalSchemas(fresh.tx())
                                .check(ADMIN, address(f), revision, proof.fragments(), () -> {}),
                        RepositoryException.Code.DATA_LOSS);
                var ledger = new DocumentReadLedger(fresh.tx(), UUID.randomUUID());
                var history = ledger.captureHistorical(ADMIN, address(f), revision);
                var budget = new PayloadBudget(32L * 1024 * 1024);
                try {
                    assertCode(() -> materialize(history, proof, DocumentPart.DOCUMENT_PART_CORE,
                                    path -> isRepeatedChild(path, 0), budget),
                            RepositoryException.Code.DATA_LOSS);
                    assertThat(budget.reservedBytes()).isZero();
                    // Snapshot capture requires the complete retained artifact set, so even the
                    // root occurrence refuses to decode from an incomplete historical proof.
                    assertCode(() -> materialize(history, proof, DocumentPart.DOCUMENT_PART_CORE,
                                    DocumentHistoricalRetainedQualificationIT::isRootOnly, budget),
                            RepositoryException.Code.DATA_LOSS);
                    assertThat(budget.reservedBytes()).isZero();
                } finally {
                    release(ledger, history);
                }
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    // Scenario 4: an explicitly opaque Any whose type has no generated class and no retained
    // definition is archived byte-exactly, while typed historical proof for that revision is
    // refused, never downgraded. The retention fixture computes a proof it discards for
    // opaque members, so the resolver below only feeds that discarded computation; the
    // assertions prove the published revision retained no schema rows at all.
    @Test void explicitOpaqueUnknownAnyIsArchivedExactlyAndTypedHistoricalReadRefuses() throws Exception {
        try (var c = context(POSTGRES)) {
            var mysteryType = mysteryType();
            var mysteryValue = DynamicMessage.newBuilder(mysteryType)
                    .setField(mysteryType.findFieldByName("blob"),
                            ByteString.copyFrom(new byte[] {(byte) 0xFF, 0x00, 0x7F, 'x'})).build();
            var mystery = Any.newBuilder().setTypeUrl("type.test/unknown.Mystery")
                    .setValue(mysteryValue.toByteString()).build();
            var document = document("qual-opaque", mystery);
            var discardedProofDefinition = DocumentSchemaRetentionFixture.definition(mysteryType, true);
            var f = DocumentSchemaRetentionFixture.prepare(c, false, false, document,
                    "qual-opaque-node", "qual-opaque-drive", null, null, occurrence -> discardedProofDefinition);
            activate(c, f);
            var result = new DocumentPublicationCommit(c.tx(), new DriveLedger(c.tx()), false, false)
                    .commit(PRINCIPAL, f.owner(), f.prepared(), Map.of("member", f.content()),
                            Map.of("member", f.selected()), f.batch(), () -> {});
            var revision = UUID.fromString(result.getMembers(0).getRevisionId());
            for (String table : List.of("document_revision_schema_artifacts", "document_revision_schema_assets",
                    "document_revision_schema_evidence")) {
                long retained = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM " + table + " WHERE revision_id=:revision")
                        .setParameter("revision", revision).getSingleResult()).longValue());
                assertThat(retained).as(table).isZero();
            }

            var fragments = DocumentPartCodec.split(document, PartLayouts.document());
            var coreFragment = fragments.stream()
                    .filter(part -> part.part() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
            try (var fresh = freshHost(c)) {
                var ledger = new DocumentReadLedger(fresh.tx(), UUID.randomUUID());
                var wrongAccount = new RepositoryCaller("other", false, Set.of("elsewhere"), Set.of());
                assertCode(() -> ledger.captureHistorical(wrongAccount, address(f), revision),
                        RepositoryException.Code.NOT_FOUND);
                var history = ledger.captureHistorical(ADMIN, address(f), revision);
                try (var use = history.use()) {
                    // Explicit opaque admission is required and present; it is never inferred.
                    history.requireOpaqueAdmission(use, () -> {});
                    var coreEntry = use.plan().entries().stream()
                            .filter(entry -> entry.part().part().part() == DocumentPart.DOCUMENT_PART_CORE)
                            .findFirst().orElseThrow();
                    // The retained plan pins the exact archival fragment bytes by digest and size.
                    assertThat(coreEntry.part().part().sha256())
                            .isEqualTo(DocumentPartCodec.sha256Hex(coreFragment.bytes()));
                    assertThat(coreEntry.part().part().size()).isEqualTo(coreFragment.bytes().length);
                } finally {
                    release(ledger, history);
                }
                // Raw round-trip of the pinned fragments preserves the opaque Any exactly.
                var assembled = DocumentRevisionAssembly.assemble(fragments.stream()
                                .map(part -> new DocumentRevisionAssembly.Fragment(part.part(), part.subKey(),
                                        ByteString.copyFrom(part.bytes()))).toList(),
                        "qual-opaque", new DocumentRevisionAssembly.Limits(4_000_000, 100, 100, 100, 1_000_000),
                        () -> {});
                assertThat(assembled.document().getStructuredData().getTypeUrl()).isEqualTo(mystery.getTypeUrl());
                assertThat(assembled.document().getStructuredData().getValue()).isEqualTo(mystery.getValue());
                // Typed historical proof is refused outright for the opaque revision.
                assertCode(() -> new DocumentHistoricalSchemas(fresh.tx())
                                .check(ADMIN, address(f), revision, Map.of(), () -> {}),
                        RepositoryException.Code.FAILED_PRECONDITION);
            }
        }
    }

    // Scenario 4 (admission side): typed-required admission with an unavailable registry
    // fails and stages nothing; it never falls back to an opaque admission.
    @Test void typedAdmissionWithUnavailableRegistryFailsWithoutOpaqueFallback() throws Exception {
        try (var c = context(POSTGRES)) {
            var mystery = Any.newBuilder().setTypeUrl("type.test/unknown.Mystery")
                    .setValue(ByteString.copyFromUtf8("unresolvable payload")).build();
            var unavailable = new AtomicInteger();
            DocumentSchemaAdmission.Resolver rejecting = occurrence -> {
                unavailable.incrementAndGet();
                throw new IllegalStateException("live registry unavailable for " + occurrence.typeUrl());
            };
            assertThatThrownBy(() -> DocumentSchemaRetentionFixture.prepare(c, true, false,
                    document("qual-unavailable", mystery), "qual-unavailable-node",
                    "qual-unavailable-drive", null, null, rejecting))
                    .hasStackTraceContaining("live registry unavailable");
            assertThat(unavailable.get()).isPositive();
            assertThat(count(c, "documents")).isZero();
            assertThat(count(c, "document_revision_commits")).isZero();
            assertThat(count(c, "document_revision_schema_admissions")).isZero();
            assertThat(count(c, "repository_schema_artifacts")).isZero();
        }
    }

    // ---- Fresh host state: close the original factory, build a new one on the same schema/pool. ----

    private record FreshHost(EntityManagerFactory emf, Tx tx) implements AutoCloseable {
        public void close() { emf.close(); }
    }

    private static FreshHost freshHost(Context c) {
        c.emf().close();
        var emf = Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", c.pool(), "hibernate.hbm2ddl.auto", "validate"));
        return new FreshHost(emf, new Tx(emf));
    }

    // ---- Publication helpers (same shape as the neighboring historical suites). ----

    private static void activate(Context c, DocumentSchemaRetentionFixture.Fixture f) {
        new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
    }

    private static UUID publishTyped(Context c, DocumentSchemaRetentionFixture.Fixture f) {
        return DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                (em, revision, manifest) -> f.retention().write(em, f.owner(), revision, () -> {}));
    }

    private static NodeAddress address(DocumentSchemaRetentionFixture.Fixture f) {
        return f.command().intent().getMembers(0).getDestination().getAddress();
    }

    private static Document document(String docId, Any structuredData) {
        return Document.newBuilder().setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                        .setSecurity(ai.protomolt.proto.repo.v1.DocumentSecurity.getDefaultInstance()).build())
                .setStructuredData(structuredData).build();
    }

    private static void release(DocumentReadLedger ledger, DocumentReadLedger.PinnedHistory history) throws Exception {
        history.close();
        assertThat(history.awaitDrained(Duration.ofSeconds(5))).isTrue();
        history.release();
        ledger.fence(); ledger.attestLocalQuiescence();
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action,
            RepositoryException.Code code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(RepositoryException.class,
                failure -> assertThat(failure.code()).isEqualTo(code));
    }

    // ---- Materialization through retained evidence occurrences. ----

    private static DynamicMessage materialize(DocumentReadLedger.PinnedHistory history,
            DocumentSchemaAdmission.Proof proof, DocumentPart part,
            Predicate<RepositorySchemaOccurrencePath> select, PayloadBudget budget) throws Exception {
        var root = proof.roots().stream()
                .filter(candidate -> candidate.locator().getSlot().getPart() == part)
                .findFirst().orElseThrow(() -> new AssertionError("No retained root for " + part));
        var evidence = DocumentRootSchemaEvidence.parseFrom(root.encoded().bytes());
        var path = evidence.getOccurrencesList().stream().filter(select).findFirst()
                .orElseThrow(() -> new AssertionError("No retained occurrence matches under " + part));
        var selection = new DocumentSchemaMaterialization.Selection(root.locatorSha256(),
                DocumentPartCodec.sha256Hex(path.toByteArray()));
        try (var materialized = history.materializeFragment(root.ordinal(), proof.fragments().get(root.ordinal()),
                selection, LIMITS, budget, RepositoryReadControl.NONE)) {
            var view = materialized.view(RepositoryReadControl.NONE);
            // The decoded occurrence is bound to the exact recorded coordinate, not a search result.
            assertThat(view.occurrence().prefix())
                    .containsExactlyElementsOf(path.getStepsList().subList(0, path.getStepsCount() - 1));
            assertThat(view.occurrence().typeUrl())
                    .isEqualTo(path.getSteps(path.getStepsCount() - 1).getAnyBoundary().getTypeUrl());
            return view.value();
        }
    }

    private static void assertRecord(DocumentReadLedger ledger, NodeAddress address, UUID revision,
            DocumentSchemaAdmission.Proof proof, String field, String expected, String descriptorSha,
            PayloadBudget budget) throws Exception {
        var history = ledger.captureHistorical(ADMIN, address, revision);
        try {
            var root = proof.roots().getFirst();
            var evidence = DocumentRootSchemaEvidence.parseFrom(root.encoded().bytes());
            var path = evidence.getOccurrences(0);
            var selection = new DocumentSchemaMaterialization.Selection(root.locatorSha256(),
                    DocumentPartCodec.sha256Hex(path.toByteArray()));
            try (var materialized = history.materializeFragment(root.ordinal(), proof.fragments().get(root.ordinal()),
                    selection, LIMITS, budget, RepositoryReadControl.NONE)) {
                var view = materialized.view(RepositoryReadControl.NONE);
                assertThat(DocumentPartCodec.sha256Hex(view.descriptorArtifact().toByteArray()))
                        .isEqualTo(descriptorSha);
                var value = view.value();
                assertThat(value.getAllFields().keySet())
                        .extracting(Descriptors.FieldDescriptor::getName).containsExactly(field);
                assertThat(value.getField(value.getDescriptorForType().findFieldByName(field))).isEqualTo(expected);
            }
        } finally {
            history.close();
            assertThat(history.awaitDrained(Duration.ofSeconds(5))).isTrue();
            history.release();
        }
    }

    private static void assertChild(DynamicMessage child, String label) {
        var type = child.getDescriptorForType();
        assertThat(type.getFullName()).isEqualTo("qual.Child");
        assertThat(child.getField(type.findFieldByName("label"))).isEqualTo(label);
        var leaf = (DynamicMessage) child.getField(type.findFieldByName("leaf"));
        assertThat(leaf.getField(leaf.getDescriptorForType().findFieldByName("note")))
                .isEqualTo(label + " leaf");
    }

    // ---- Occurrence path predicates. ----

    private static boolean isRootOnly(RepositorySchemaOccurrencePath path) {
        return path.getStepsCount() == 1 && isBoundary(path.getSteps(0), "qual.Root");
    }

    private static boolean isSingleChild(RepositorySchemaOccurrencePath path) {
        return path.getStepsCount() == 3 && isBoundary(path.getSteps(0), "qual.Root")
                && isField(path.getSteps(1), 1) && isBoundary(path.getSteps(2), "qual.Child");
    }

    private static boolean isRepeatedChild(RepositorySchemaOccurrencePath path, int index) {
        return path.getStepsCount() == 4 && isBoundary(path.getSteps(0), "qual.Root")
                && isField(path.getSteps(1), 2)
                && path.getSteps(2).getStepCase() == RepositorySchemaOccurrenceStep.StepCase.REPEATED_INDEX
                && path.getSteps(2).getRepeatedIndex() == index
                && isBoundary(path.getSteps(3), "qual.Child");
    }

    private static boolean isMapChild(RepositorySchemaOccurrencePath path, String key) {
        return path.getStepsCount() == 4 && isBoundary(path.getSteps(0), "qual.Root")
                && isField(path.getSteps(1), 3)
                && path.getSteps(2).getStepCase() == RepositorySchemaOccurrenceStep.StepCase.MAP_KEY
                && path.getSteps(2).getMapKey().getStringValue().equals(key)
                && isBoundary(path.getSteps(3), "qual.Child");
    }

    private static boolean isField(RepositorySchemaOccurrenceStep step, int number) {
        return step.getStepCase() == RepositorySchemaOccurrenceStep.StepCase.FIELD_NUMBER
                && step.getFieldNumber() == number;
    }

    private static boolean isBoundary(RepositorySchemaOccurrenceStep step, String typeName) {
        return step.getStepCase() == RepositorySchemaOccurrenceStep.StepCase.ANY_BOUNDARY
                && step.getAnyBoundary().getTypeUrl().endsWith("/" + typeName);
    }

    // ---- Dynamic descriptor fixtures: no generated classes exist for these types. ----

    private record WalkTypes(Descriptors.Descriptor root, Descriptors.Descriptor child) {}

    private static final class CountingResolver implements DocumentSchemaAdmission.Resolver {
        private final WalkTypes types;
        private final Map<String, DocumentSchemaAdmission.Definition> definitions = new ConcurrentHashMap<>();
        private final AtomicInteger calls = new AtomicInteger();

        CountingResolver(WalkTypes types) { this.types = types; }

        int calls() { return calls.get(); }

        public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) {
            calls.incrementAndGet();
            return definitions.computeIfAbsent(occurrence.typeUrl(), url -> {
                if (url.endsWith("/qual.Root")) return DocumentSchemaRetentionFixture.definition(types.root(), true);
                if (url.endsWith("/qual.Child")) return DocumentSchemaRetentionFixture.definition(types.child(), true);
                throw new IllegalStateException("Unexpected occurrence type URL " + url);
            });
        }
    }

    /** Root (imports any.proto) holding nested Child Anys; Child's file imports Leaf's file. */
    private static WalkTypes walkTypes(boolean liveVariant) throws Exception {
        var leafFile = Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                .setName("qual_leaf.proto").setPackage("qual").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Leaf").addField(stringField("note", 1)))
                .build(), new Descriptors.FileDescriptor[0]);
        var childBuilder = DescriptorProto.newBuilder().setName("Child").addField(stringField("label", 1))
                .addField(messageField("leaf", 2, ".qual.Leaf"));
        if (liveVariant) childBuilder.addField(stringField("extra", 3));
        var childFile = Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                .setName(liveVariant ? "qual_child_live.proto" : "qual_child.proto")
                .setPackage("qual").setSyntax("proto3").addDependency("qual_leaf.proto")
                .addMessageType(childBuilder).build(), new Descriptors.FileDescriptor[] {leafFile});
        var entry = DescriptorProto.newBuilder().setName("EntriesEntry")
                .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(stringField("key", 1)).addField(messageField("value", 2, ".google.protobuf.Any"));
        var rootFile = Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder()
                .setName("qual_root.proto").setPackage("qual").setSyntax("proto3")
                .addDependency(Any.getDescriptor().getFile().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Root").addNestedType(entry)
                        .addField(messageField("single", 1, ".google.protobuf.Any"))
                        .addField(repeatedMessageField("children", 2, ".google.protobuf.Any"))
                        .addField(repeatedMessageField("entries", 3, ".qual.Root.EntriesEntry")))
                .build(), new Descriptors.FileDescriptor[] {Any.getDescriptor().getFile()});
        return new WalkTypes(rootFile.findMessageTypeByName("Root"), childFile.findMessageTypeByName("Child"));
    }

    private static Any rootAny(WalkTypes types, String single, String first, String second) {
        var root = types.root();
        var builder = DynamicMessage.newBuilder(root)
                .setField(root.findFieldByName("single"), childAny(types, single))
                .addRepeatedField(root.findFieldByName("children"), childAny(types, first))
                .addRepeatedField(root.findFieldByName("children"), childAny(types, second));
        var entryType = root.findFieldByName("entries").getMessageType();
        for (var keyed : Map.of("one", first, "two", second).entrySet()) {
            builder.addRepeatedField(root.findFieldByName("entries"), DynamicMessage.newBuilder(entryType)
                    .setField(entryType.findFieldByName("key"), keyed.getKey())
                    .setField(entryType.findFieldByName("value"), childAny(types, keyed.getValue())).build());
        }
        return Any.newBuilder().setTypeUrl("type.test/qual.Root")
                .setValue(builder.build().toByteString()).build();
    }

    private static Any childAny(WalkTypes types, String label) {
        var child = types.child();
        var leafType = child.findFieldByName("leaf").getMessageType();
        var value = DynamicMessage.newBuilder(child)
                .setField(child.findFieldByName("label"), label)
                .setField(child.findFieldByName("leaf"), DynamicMessage.newBuilder(leafType)
                        .setField(leafType.findFieldByName("note"), label + " leaf").build())
                .build();
        return Any.newBuilder().setTypeUrl("type.test/qual.Child").setValue(value.toByteString()).build();
    }

    private static Descriptors.Descriptor mysteryType() throws Exception {
        var proto = FileDescriptorProto.newBuilder().setName("mystery.proto").setPackage("unknown")
                .setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName("Mystery")
                        .addField(FieldDescriptorProto.newBuilder().setName("blob").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_BYTES)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))).build();
        return Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Mystery");
    }

    private static Descriptors.Descriptor recordType(String fileName, String fieldName) throws Exception {
        var proto = FileDescriptorProto.newBuilder().setName(fileName).setPackage("archive").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Record").addField(stringField(fieldName, 1)))
                .build();
        return Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("Record");
    }

    private static Any recordAny(Descriptors.Descriptor type, String field, String value) {
        return Any.newBuilder().setTypeUrl("type.test/archive.Record")
                .setValue(DynamicMessage.newBuilder(type)
                        .setField(type.findFieldByName(field), value).build().toByteString())
                .build();
    }

    private static FieldDescriptorProto.Builder stringField(String name, int number) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static FieldDescriptorProto.Builder messageField(String name, int number, String typeName) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(typeName)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static FieldDescriptorProto.Builder repeatedMessageField(String name, int number, String typeName) {
        return messageField(name, number, typeName).setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);
    }
}
