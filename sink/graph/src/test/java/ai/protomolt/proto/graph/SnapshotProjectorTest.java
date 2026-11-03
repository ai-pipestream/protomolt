package ai.protomolt.proto.graph;

import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.skos.SkosConverter;
import ai.protomolt.proto.types.ConceptScheme;
import com.google.protobuf.Timestamp;
import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.junit.jupiter.api.Test;

class SnapshotProjectorTest {
    private static final class DatasetScope implements AutoCloseable {
        final Dataset value = DatasetFactory.createTxnMem();
        @Override public void close() { value.close(); }
    }
    private static final String NS = SnapshotProjector.NS;
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder().setAccountId("account")
            .setGraphId("graph").setGraphAddressId("coordinate").setDocId("document").build();
    private static final PartSelector CORE = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
    // Synthetic retained references: fixtures exercise preservation, not repository artifact verification.
    private static final RepositorySchemaAssetReference SCHEMA = RepositorySchemaAssetReference.newBuilder()
            .setTypeUrl("type.googleapis.com/example.Core").setDescriptorSha256("a".repeat(64))
            .setMetadataCodec("repository-schema-asset").setMetadataVersion(1).setMetadataSha256("b".repeat(64)).build();
    private static final SnapshotProjector.Evidence EVIDENCE = new SnapshotProjector.Evidence(
            PartSchemaBinding.newBuilder().setPart(CORE).setFragmentSchema(SCHEMA).build(), List.of());
    private static final Map<String, String> RELATIONS = Map.of("references", "http://purl.org/dc/terms/references");

    private static SourceRevision source(long revision) {
        return SourceRevision.newBuilder().setAddress(ADDRESS).setRevision(revision).build();
    }
    private static SnapshotProjector projector(Dataset dataset) {
        return new SnapshotProjector(dataset, "test-v1", RELATIONS, (source, message, evidence, selected) -> {});
    }
    private static ResolvedLink link(String path, int ordinal) {
        return ResolvedLink.newBuilder().setFieldPath(path).setOrdinal(ordinal).setRelation("references")
                .setStaticTarget(StaticLinkState.newBuilder().setAddress(ADDRESS.toBuilder().setDocId("target"))
                        .setPinnedRevision(9007199254740993L)).build();
    }
    private static ListLinksResponse links(long revision, ResolvedLink... links) {
        return ListLinksResponse.newBuilder().setRevision(revision).addAllLinks(List.of(links)).build();
    }
    private static Model read(Dataset dataset, String graph) {
        dataset.begin(ReadWrite.READ);
        try { return ModelFactory.createDefaultModel().add(dataset.getNamedModel(graph)); }
        finally { dataset.end(); }
    }

    @Test void replacementReplayStaleAndExactRevision() {
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            var projector = projector(dataset);
            long revision = 9007199254740993L;
            var snapshot = links(revision, link("structured_data.cites", 0), link("sections[1].cites", 1));
            assertThat(projector.replaceLinks(source(revision), snapshot, EVIDENCE)).isEqualTo(SnapshotProjector.Result.REPLACED);
            var graph = SnapshotProjector.graphIri(ADDRESS);
            var original = read(dataset, graph);
            var root = original.getResource(SnapshotProjector.sourceIri(source(revision)));
            assertThat(root.getProperty(ResourceFactory.createProperty(NS + "revision")).getLong()).isEqualTo(revision);
            assertThat(original.listStatements(null, ResourceFactory.createProperty(NS + "fieldPath"), (String) null).toList()).hasSize(2);
            assertThat(projector.replaceLinks(source(revision), snapshot, EVIDENCE)).isEqualTo(SnapshotProjector.Result.REPLAYED);
            assertThat(read(dataset, graph).isIsomorphicWith(original)).isTrue();
            assertThatThrownBy(() -> projector.replaceLinks(source(revision - 1), links(revision - 1), EVIDENCE))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Stale");
            assertThatThrownBy(() -> projector.replaceLinks(source(revision), links(revision), EVIDENCE))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("Conflicting");
            projector.replaceLinks(source(revision + 1), links(revision + 1), EVIDENCE);
            assertThat(read(dataset, graph).contains(null, ResourceFactory.createProperty(NS + "fieldPath"))).isFalse();
            assertThat(read(dataset, graph).containsResource(root)).isFalse();
        }
    }

    @Test void targetFlagsAreReaderRelativeAndSelectorsStayProtobuf() throws Exception {
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            var projector = projector(dataset);
            var selector = DynamicLinkSelector.newBuilder().setFilter(DocumentFilter.newBuilder().setTitle("example")).build();
            var dynamic = ResolvedLink.newBuilder().setFieldPath("dynamic").setRelation("alternate").setDynamicTarget(selector).build();
            var fixed = link("cites", 0);
            var unavailable = fixed.toBuilder().setStaticTarget(fixed.getStaticTarget().toBuilder().setTargetUnavailable(true)).build();
            projector.replaceLinks(source(1), links(1, fixed, dynamic), EVIDENCE);
            assertThat(projector.replaceLinks(source(1), links(1, unavailable, dynamic), EVIDENCE)).isEqualTo(SnapshotProjector.Result.REPLAYED);
            var graph = read(dataset, SnapshotProjector.graphIri(ADDRESS));
            var literal = graph.listObjectsOfProperty(ResourceFactory.createProperty(NS + "selectorProto")).next().asLiteral().getString();
            assertThat(DynamicLinkSelector.parseFrom(Base64.getDecoder().decode(literal))).isEqualTo(selector);
            assertThat(graph.listStatements(null, ResourceFactory.createProperty(RELATIONS.get("references")), (org.apache.jena.rdf.model.RDFNode) null).toList()).hasSize(1);
            assertThat(graph.contains(null, ResourceFactory.createProperty(NS + "targetUnavailable"))).isFalse();
        }
    }

    @Test void allAddressComponentsSeparateGraphsAndRevisionSources() {
        var variants = List.of(ADDRESS, ADDRESS.toBuilder().setAccountId("other").build(),
                ADDRESS.toBuilder().setGraphId("other").build(), ADDRESS.toBuilder().setGraphAddressId("other").build(),
                ADDRESS.toBuilder().setDocId("other").build());
        assertThat(variants.stream().map(SnapshotProjector::graphIri).distinct().count()).isEqualTo(5);
        assertThat(SnapshotProjector.sourceIri(source(1))).isNotEqualTo(SnapshotProjector.sourceIri(source(2)));
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            var projector = projector(dataset);
            for (var address : variants) projector.replaceLinks(source(1).toBuilder().setAddress(address).build(), links(1), EVIDENCE);
            projector.replaceLinks(source(2), links(2), EVIDENCE);
            for (var address : variants.subList(1, variants.size()))
                assertThat(read(dataset, SnapshotProjector.graphIri(address)).containsResource(ResourceFactory.createResource(
                        SnapshotProjector.sourceIri(source(1).toBuilder().setAddress(address).build())))).isTrue();
        }
    }

    @Test void tombstoneDeletesOnlyItsGraphAndPreventsStaleResurrection() {
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            var projector = projector(dataset);
            projector.replaceLinks(source(1), links(1, link("cites", 0)), EVIDENCE);
            var tombstone = DocumentManifest.newBuilder().setAddress(ADDRESS).setRevision(2).setTombstone(true)
                    .setCommittedAt(Timestamp.newBuilder().setSeconds(1)).build();
            assertThat(projector.delete(tombstone)).isEqualTo(SnapshotProjector.Result.REPLACED);
            assertThat(read(dataset, SnapshotProjector.graphIri(ADDRESS)).isEmpty()).isTrue();
            assertThat(projector.delete(tombstone)).isEqualTo(SnapshotProjector.Result.REPLAYED);
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1), EVIDENCE)).hasMessageContaining("Stale");
            assertThatThrownBy(() -> projector.replaceLinks(source(2), links(2), EVIDENCE)).hasMessageContaining("Conflicting");
            assertThatThrownBy(() -> projector.delete(tombstone.toBuilder().setRevision(3).setTombstone(false).build()))
                    .hasMessageContaining("explicit tombstone");
        }
    }

    @Test void failureAfterBothGraphAndGuardWritesRollsBackAndCanRetry() {
        try (var scope = new DatasetScope()) {
            var real = scope.value;
            var failCommit = new AtomicBoolean(false);
            // Only inject failure before commit; every graph operation and abort is real Jena.
            var failing = (Dataset) Proxy.newProxyInstance(Dataset.class.getClassLoader(), new Class<?>[]{Dataset.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("commit") && failCommit.get()) throw new IllegalStateException("injected commit failure");
                        try { return method.invoke(real, args); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var projector = projector(failing);
            projector.replaceLinks(source(1), links(1, link("old", 0)), EVIDENCE);
            var graph = read(real, SnapshotProjector.graphIri(ADDRESS));
            var control = read(real, SnapshotProjector.CONTROL_GRAPH);
            failCommit.set(true);
            assertThatThrownBy(() -> projector.replaceLinks(source(2), links(2, link("new", 0)), EVIDENCE))
                    .hasMessage("injected commit failure");
            assertThat(read(real, SnapshotProjector.graphIri(ADDRESS)).isIsomorphicWith(graph)).isTrue();
            assertThat(read(real, SnapshotProjector.CONTROL_GRAPH).isIsomorphicWith(control)).isTrue();
            failCommit.set(false);
            assertThat(projector.replaceLinks(source(2), links(2, link("new", 0)), EVIDENCE)).isEqualTo(SnapshotProjector.Result.REPLACED);
        }
    }

    @Test void rejectsMissingEvidenceUnknownMappingDuplicateOccurrencesAndCrossAccountTargets() {
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            var projector = projector(dataset);
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1), null)).hasMessageContaining("evidence");
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(2), EVIDENCE)).hasMessageContaining("revision mismatch");
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1, link("a", 0), link("a", 0)), EVIDENCE))
                    .hasMessageContaining("Duplicate");
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1, link("a", 0).toBuilder().setRelation("alternate").build()), EVIDENCE))
                    .hasMessageContaining("Unmapped");
            var cross = link("a", 0).toBuilder().setStaticTarget(StaticLinkState.newBuilder()
                    .setAddress(ADDRESS.toBuilder().setAccountId("other"))).build();
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1, cross), EVIDENCE)).hasMessageContaining("Cross-account");
            assertThatThrownBy(() -> projector.replaceLinks(source(0), links(1), EVIDENCE)).hasMessageContaining("positive revision");
            assertThat(read(dataset, SnapshotProjector.CONTROL_GRAPH).isEmpty()).isTrue();
        }
    }

    @Test void authorizationRunsOnReplayAndRefusalsPreserveCause() {
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            var deny = new AtomicBoolean(false);
            var cause = new IllegalStateException("reader unavailable");
            var projector = new SnapshotProjector(dataset, "v1", RELATIONS, (source, snapshot, evidence, selected) -> {
                assertThat(source.getAddress()).isEqualTo(ADDRESS);
                if (deny.get()) throw cause;
            });
            projector.replaceLinks(source(1), links(1), EVIDENCE);
            deny.set(true);
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1), EVIDENCE)).isSameAs(cause);
            assertThatThrownBy(() -> projector.replaceLinks(source(2), links(2), EVIDENCE)).isSameAs(cause);
        }
    }

    @Test void schemeUsesStrictConverterAndRetainsOccurrenceEvidence() {
        var converter = new SkosConverter("subjects", "https://example.org/scheme", Map.of());
        var scheme = converter.read(new ByteArrayInputStream(("""
                @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
                <https://example.org/scheme> a skos:ConceptScheme; skos:prefLabel "Subjects"@en .
                <https://example.org/a> a skos:Concept; skos:inScheme <https://example.org/scheme>; skos:prefLabel "Alpha"@en .
                """).getBytes(StandardCharsets.UTF_8)), SkosConverter.Format.TURTLE);
        var occurrence = ResolvedOccurrence.newBuilder().setSchema(SCHEMA.toBuilder().setTypeUrl("type.googleapis.com/" + ConceptScheme.getDescriptor().getFullName()))
                .setOccurrence(OccurrenceLocator.newBuilder().setPart(CORE).addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4)))
                .setPayloadSha256(MessageFingerprints.fingerprint(scheme)).setPayloadLength(scheme.getSerializedSize()).build();
        var evidence = new SnapshotProjector.Evidence(EVIDENCE.core(), List.of(occurrence));
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            var projector = projector(dataset);
            projector.replaceScheme(source(1), scheme, evidence, occurrence, converter);
            var graph = read(dataset, SnapshotProjector.graphIri(ADDRESS));
            assertThat(graph.contains(ResourceFactory.createResource("https://example.org/a"),
                    ResourceFactory.createProperty("http://www.w3.org/2004/02/skos/core#prefLabel"), "Alpha", "en")).isTrue();
            String literal = graph.listObjectsOfProperty(ResourceFactory.createProperty(NS + "resolvedOccurrenceProto")).next().asLiteral().getString();
            assertThat(Base64.getDecoder().decode(literal)).isEqualTo(MessageFingerprints.deterministicBytes(occurrence));
            assertThat(projector.replaceScheme(source(1), scheme, evidence, occurrence, converter)).isEqualTo(SnapshotProjector.Result.REPLAYED);
            assertThatThrownBy(() -> projector.replaceScheme(source(2), scheme, EVIDENCE, occurrence, converter)).hasMessageContaining("absent");
            var wrongType = occurrence.toBuilder().setSchema(SCHEMA).build();
            assertThatThrownBy(() -> projector.replaceScheme(source(2), scheme,
                    new SnapshotProjector.Evidence(EVIDENCE.core(), List.of(wrongType)), wrongType, converter))
                    .hasMessageContaining("must name ConceptScheme");
            var reserved = new SkosConverter("subjects", NS + "spoof", Map.of());
            assertThatThrownBy(() -> projector.replaceScheme(source(2), scheme, evidence, occurrence, reserved))
                    .hasMessageContaining("reserved projection namespace");
            assertThat(read(dataset, SnapshotProjector.graphIri(ADDRESS)).isIsomorphicWith(graph)).isTrue();
        }
    }
    @Test void schemeLiteralsThatTurtleEscapesSurviveTheProjectionExactly() {
        String label = "She said \"caf\u00e9\"\nline two \u2713 \ud83d\ude00";
        var converter = new SkosConverter("subjects", "https://example.org/scheme", Map.of());
        var scheme = converter.read(new ByteArrayInputStream(("""
                @prefix skos: <http://www.w3.org/2004/02/skos/core#> .
                <https://example.org/scheme> a skos:ConceptScheme; skos:prefLabel "Subjects"@en .
                <https://example.org/a> a skos:Concept; skos:inScheme <https://example.org/scheme>;
                    skos:prefLabel "She said \\"caf\u00e9\\"\\nline two \u2713 \ud83d\ude00"@en .
                """).getBytes(StandardCharsets.UTF_8)), SkosConverter.Format.TURTLE);
        var occurrence = ResolvedOccurrence.newBuilder().setSchema(SCHEMA.toBuilder().setTypeUrl("type.googleapis.com/" + ConceptScheme.getDescriptor().getFullName()))
                .setOccurrence(OccurrenceLocator.newBuilder().setPart(CORE).addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(1)))
                .setPayloadSha256(MessageFingerprints.fingerprint(scheme)).setPayloadLength(scheme.getSerializedSize()).build();
        var evidence = new SnapshotProjector.Evidence(EVIDENCE.core(), List.of(occurrence));
        try (var scope = new DatasetScope()) {
            projector(scope.value).replaceScheme(source(1), scheme, evidence, occurrence, converter);
            var graph = read(scope.value, SnapshotProjector.graphIri(ADDRESS));
            assertThat(graph.contains(ResourceFactory.createResource("https://example.org/a"),
                    ResourceFactory.createProperty("http://www.w3.org/2004/02/skos/core#prefLabel"), label, "en")).isTrue();
        }
    }
    @Test void concurrentReaderSeesWholeOldSnapshotUntilCommitThenWholeNewSnapshot() throws Exception {
        try (var scope = new DatasetScope(); var executor = Executors.newSingleThreadExecutor()) {
            var real = scope.value;
            projector(real).replaceLinks(source(1), links(1, link("old", 0)), EVIDENCE);
            var reachedCommit = new CountDownLatch(1);
            var releaseCommit = new CountDownLatch(1);
            var paused = (Dataset) Proxy.newProxyInstance(Dataset.class.getClassLoader(), new Class<?>[]{Dataset.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("commit")) {
                            reachedCommit.countDown();
                            if (!releaseCommit.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("test commit timeout");
                        }
                        try { return method.invoke(real, args); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var result = executor.submit(() -> projector(paused).replaceLinks(source(2), links(2, link("new", 0)), EVIDENCE));
            try {
                assertThat(reachedCommit.await(10, TimeUnit.SECONDS)).isTrue();
                real.begin(ReadWrite.READ);
                try {
                    var graph = real.getNamedModel(SnapshotProjector.graphIri(ADDRESS));
                    assertThat(graph.contains(null, ResourceFactory.createProperty(NS + "fieldPath"), "old")).isTrue();
                    assertThat(graph.contains(null, ResourceFactory.createProperty(NS + "fieldPath"), "new")).isFalse();
                    assertThat(real.getNamedModel(SnapshotProjector.CONTROL_GRAPH).getResource(SnapshotProjector.graphIri(ADDRESS))
                            .getRequiredProperty(ResourceFactory.createProperty(NS + "revision")).getLong()).isEqualTo(1);
                    releaseCommit.countDown();
                    assertThat(result.get(10, TimeUnit.SECONDS)).isEqualTo(SnapshotProjector.Result.REPLACED);
                    // Existing read transaction retains its coherent snapshot even after commit.
                    assertThat(graph.contains(null, ResourceFactory.createProperty(NS + "fieldPath"), "old")).isTrue();
                } finally { real.end(); }
                assertThat(read(real, SnapshotProjector.graphIri(ADDRESS))
                        .contains(null, ResourceFactory.createProperty(NS + "fieldPath"), "new")).isTrue();
                assertThat(read(real, SnapshotProjector.CONTROL_GRAPH).getResource(SnapshotProjector.graphIri(ADDRESS))
                        .getRequiredProperty(ResourceFactory.createProperty(NS + "revision")).getLong()).isEqualTo(2);
            } finally { releaseCommit.countDown(); }
        }
    }

    @Test void malformedEvidenceUnknownFieldsAndNestedTransactionsFailWithoutWriting() {
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            var projector = projector(dataset);
            var absent = new SnapshotProjector.Evidence(PartSchemaBinding.getDefaultInstance(), List.of());
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1), absent)).isInstanceOf(IllegalArgumentException.class);
            var malformed = new SnapshotProjector.Evidence(EVIDENCE.core().toBuilder()
                    .setFragmentSchema(SCHEMA.toBuilder().setDescriptorSha256("missing")).build(), List.of());
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1), malformed)).isInstanceOf(IllegalArgumentException.class);
            var unknown = com.google.protobuf.UnknownFieldSet.newBuilder().addField(2000,
                    com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
            assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1).toBuilder().setUnknownFields(unknown).build(), EVIDENCE))
                    .hasMessageContaining("Unknown fields");
            assertThatThrownBy(() -> SnapshotProjector.documentIri(ADDRESS.toBuilder().setAccountId("").build()))
                    .hasMessageContaining("account");
            dataset.begin(ReadWrite.WRITE);
            try {
                assertThatThrownBy(() -> projector.replaceLinks(source(1), links(1), EVIDENCE)).hasMessageContaining("own its transaction");
                assertThat(dataset.isInTransaction()).isTrue();
                dataset.abort();
            } finally { dataset.end(); }
            assertThat(read(dataset, SnapshotProjector.CONTROL_GRAPH).isEmpty()).isTrue();
        }
    }

    @Test void changedMappingOrEvidenceCannotRewriteSameRevision() {
        try (var scope = new DatasetScope()) {
            var dataset = scope.value;
            projector(dataset).replaceLinks(source(1), links(1), EVIDENCE);
            var changedVersion = new SnapshotProjector(dataset, "test-v2", RELATIONS, (source, message, evidence, selected) -> {});
            assertThatThrownBy(() -> changedVersion.replaceLinks(source(1), links(1), EVIDENCE)).hasMessageContaining("Conflicting");
            var changedTable = new SnapshotProjector(dataset, "test-v1", Map.of("references", "https://example.org/ref"),
                    (source, message, evidence, selected) -> {});
            assertThatThrownBy(() -> changedTable.replaceLinks(source(1), links(1), EVIDENCE)).hasMessageContaining("Conflicting");
            var evidence = new SnapshotProjector.Evidence(EVIDENCE.core().toBuilder()
                    .setFragmentSchema(SCHEMA.toBuilder().setDescriptorSha256("c".repeat(64))).build(), List.of());
            assertThatThrownBy(() -> projector(dataset).replaceLinks(source(1), links(1), evidence)).hasMessageContaining("Conflicting");
            assertThatThrownBy(() -> new SnapshotProjector(dataset, "v1", Map.of("references", "relative"),
                    (source, message, ev, selected) -> {})).hasMessageContaining("absolute URI");
            assertThatThrownBy(() -> new SnapshotProjector(dataset, "v1", Map.of("references", NS + "revision"),
                    (source, message, ev, selected) -> {})).hasMessageContaining("absolute URI");
        }
    }

}
