package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentFragmentSchemaReplayTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    private static final DocumentSchemaBinding CONTAINER = bind(Document.getDescriptor());
    private static final DocumentAnyRootInventory.Limits FRAGMENT_LIMITS =
            new DocumentAnyRootInventory.Limits(1_000_000, 10_000, 30, 100, 100);
    private static final DocumentSchemaOccurrences.Limits OCCURRENCE_LIMITS = DocumentSchemaOccurrences.Limits.DEFAULT;
    private static final DocumentSchemaReplay.Limits REPLAY_LIMITS =
            new DocumentSchemaReplay.Limits(4096, 4_000_000, 10_000);

    @Test
    void replaysCoreAndParsedFragmentsFromActualInventoryWithImmutableResults() throws Exception {
        var fixture = fixture();
        var core = part(fixture, DocumentPart.DOCUMENT_PART_CORE);
        var parsed = part(fixture, DocumentPart.DOCUMENT_PART_PARSED);
        var coreBundles = bundles(core, fixture);
        var parsedBundles = bundles(parsed, fixture);

        var coreChecked = check(core, coreBundles, fixture, limits(1_000_000), retained(fixture), () -> {});
        var parsedChecked = check(parsed, parsedBundles, fixture, limits(1_000_000), retained(fixture), () -> {});
        assertThat(coreChecked).hasSize(1);
        assertThat(parsedChecked).hasSize(2);
        assertThat(coreChecked.getFirst().root()).isEqualTo(coreBundles.getFirst().getRoot());
        assertThat(parsedChecked).extracting(item -> item.root().getSlot().getPart())
                .containsOnly(DocumentPart.DOCUMENT_PART_PARSED);
        assertThatThrownBy(() -> parsedChecked.clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> parsedChecked.getFirst().checked().payload().resolvedSchemas().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void releasesCanonicalScratchBeforeLoadingSchemasAndReturnsTheSameValidatedRoots() throws Exception {
        var fixture = fixture();
        var parsed = part(fixture, DocumentPart.DOCUMENT_PART_PARSED);
        var bundles = bundles(parsed, fixture);
        var live = new java.util.concurrent.atomic.AtomicLong();
        var peak = new java.util.concurrent.atomic.AtomicLong();
        DocumentAdmissionReservations reservations = bytes -> {
            peak.accumulateAndGet(live.addAndGet(bytes), Math::max);
            var closed = new java.util.concurrent.atomic.AtomicBoolean();
            return () -> {
                if (closed.compareAndSet(false, true)) live.addAndGet(-bytes);
            };
        };
        var reads = new AtomicInteger();
        var reader = new DocumentRetainedSchemaAssets(hash -> {
            assertThat(live.get()).as("scratch released before retained descriptor lookup").isZero();
            reads.incrementAndGet();
            return Optional.ofNullable(fixture.descriptors().get(hash));
        }, new DocumentRetainedSchemaAssets.Limits(20, 16_000_000,
                new ClosedDescriptorSet.Limits(4_000_000, 1024, 10_000, 100)));
        var checked = DocumentFragmentSchemaReplay.check(CONTAINER, slot(parsed.part()), parsed.bytes(), "doc",
                bundles, fixture.metadata(), reader, VALIDATOR, limits(1_000_000), reservations, () -> {});
        var expected = check(parsed, bundles, fixture, limits(1_000_000), retained(fixture), () -> {});
        assertThat(checked).extracting(DocumentFragmentSchemaReplay.CheckedRoot::root)
                .containsExactlyElementsOf(expected.stream().map(DocumentFragmentSchemaReplay.CheckedRoot::root).toList());
        assertThat(reads).hasValue(2);
        assertThat(live.get()).isZero();
        long maximumBundleBytes = bundles.stream().mapToLong(bundle ->
                DocumentRootSchemaEvidenceCodec.encode(bundle, () -> {}).bytes().size()).max().orElseThrow();
        assertThat(peak.get()).isEqualTo(maximumBundleBytes);
    }

    @Test
    void scratchRefusalAndCancellationCannotAdvanceToSchemaLoading() throws Exception {
        var fixture = fixture();
        var parsed = part(fixture, DocumentPart.DOCUMENT_PART_PARSED);
        var bundles = bundles(parsed, fixture);
        var reads = new AtomicInteger();
        var reader = new DocumentRetainedSchemaAssets(hash -> {
            reads.incrementAndGet();
            return Optional.ofNullable(fixture.descriptors().get(hash));
        }, new DocumentRetainedSchemaAssets.Limits(20, 16_000_000,
                new ClosedDescriptorSet.Limits(4_000_000, 1024, 10_000, 100)));
        var refused = new IllegalArgumentException("host capacity refused");
        assertThatThrownBy(() -> DocumentFragmentSchemaReplay.check(CONTAINER, slot(parsed.part()), parsed.bytes(),
                "doc", bundles, fixture.metadata(), reader, VALIDATOR, limits(1_000_000), bytes -> {
                    throw refused;
                }, () -> {})).isSameAs(refused);
        var live = new java.util.concurrent.atomic.AtomicLong();
        var stopped = new CancellationException("cancel after reservation");
        assertThatThrownBy(() -> DocumentFragmentSchemaReplay.check(CONTAINER, slot(parsed.part()), parsed.bytes(),
                "doc", bundles, fixture.metadata(), reader, VALIDATOR, limits(1_000_000), bytes -> {
                    live.addAndGet(bytes);
                    return () -> live.addAndGet(-bytes);
                }, () -> {
                    if (live.get() > 0) throw stopped;
                })).isSameAs(stopped);
        assertThat(live.get()).isZero();
        assertThat(reads).hasValue(0);
    }

    @Test
    void requiresExactOneToOneEvidenceCoverageAndSelectedFragmentIdentity() throws Exception {
        var fixture = fixture();
        var core = part(fixture, DocumentPart.DOCUMENT_PART_CORE);
        var coreBundles = bundles(core, fixture);
        assertThatThrownBy(() -> check(core, List.of(), fixture, limits(1_000_000), retained(fixture), () -> {}))
                .hasMessageContaining("cover every discovered");
        assertThatThrownBy(() -> check(core, List.of(coreBundles.getFirst(), coreBundles.getFirst()), fixture,
                limits(1_000_000), retained(fixture), () -> {})).hasMessageContaining("duplicate fragment root");

        var parsed = part(fixture, DocumentPart.DOCUMENT_PART_PARSED);
        var parsedBundles = bundles(parsed, fixture);
        var extra = parsedBundles.getFirst().toBuilder().setRoot(parsedBundles.getFirst().getRoot().toBuilder()
                .setFragmentSha256("f".repeat(64))).build();
        assertThatThrownBy(() -> check(parsed, List.of(parsedBundles.getFirst(), parsedBundles.getLast(), extra),
                fixture, limits(1_000_000), retained(fixture), () -> {})).hasMessageContaining("cover every discovered");

        assertThatThrownBy(() -> check(core, List.of(parsedBundles.getFirst()), fixture,
                limits(1_000_000), retained(fixture), () -> {}))
                .hasMessageContaining("slot differs");

        var changed = document("doc", Any.pack(StringValue.of("changed")),
                Any.pack(StringValue.of("one")), Any.pack(Timestamp.newBuilder().setSeconds(2).build()));
        var changedCore = part(changed, DocumentPart.DOCUMENT_PART_CORE);
        assertThatThrownBy(() -> check(changedCore, coreBundles, fixture, limits(1_000_000), retained(fixture), () -> {}))
                .hasMessageContaining("differs from exact fragment inventory");
        assertThatThrownBy(() -> check(core, coreBundles, fixture, limits(1_000_000), retained(fixture), () -> {}, "other-doc"))
                .hasMessageContaining("document identity differs");
    }

    @Test
    void enforcesAggregateEvidenceAndSharedDecodedPayloadByteBudgets() throws Exception {
        var fixture = fixture();
        var parsed = part(fixture, DocumentPart.DOCUMENT_PART_PARSED);
        var bundles = bundles(parsed, fixture);
        assertThatThrownBy(() -> check(parsed, bundles, fixture,
                new DocumentFragmentSchemaReplay.Limits(FRAGMENT_LIMITS, payloadLimits(1_000_000), OCCURRENCE_LIMITS,
                        REPLAY_LIMITS, 1), retained(fixture), () -> {}))
                .hasMessageContaining("aggregate fragment evidence bytes");

        // StringValue("one") and Timestamp with seconds=2 consume a combined seven decoded bytes.
        assertThatThrownBy(() -> check(parsed, bundles, fixture, limits(6), retained(fixture), () -> {}))
                .hasMessageContaining("aggregate payload bytes");
        var inventory = DocumentAnyRootInventory.inspect(CONTAINER, slot(parsed.part()), parsed.bytes(), "doc",
                FRAGMENT_LIMITS, () -> {});
        int firstRootBytes = inventory.roots().getFirst().envelope().getValue().size();
        assertThatThrownBy(() -> check(parsed, bundles, fixture, limits(firstRootBytes), retained(fixture), () -> {}))
                .hasMessageContaining("aggregate fragment decoded bytes exceed limit");
        assertThat(check(parsed, bundles, fixture, limits(7), retained(fixture), () -> {})).hasSize(2);
    }

    @Test
    void cancellationOnLaterRootFailsTheWholeReplayWithoutReturningPartialResults() throws Exception {
        var fixture = fixture();
        var parsed = part(fixture, DocumentPart.DOCUMENT_PART_PARSED);
        var bundles = bundles(parsed, fixture);
        var reads = new AtomicInteger();
        var stopped = new CancellationException("cancel while loading later root schema");
        var reader = new DocumentRetainedSchemaAssets(hash -> {
            reads.incrementAndGet();
            return Optional.ofNullable(fixture.descriptors().get(hash));
        }, new DocumentRetainedSchemaAssets.Limits(20, 16_000_000,
                new ClosedDescriptorSet.Limits(4_000_000, 1024, 10_000, 100)));
        assertThatThrownBy(() -> check(parsed, bundles, fixture, limits(1_000_000), reader, () -> {
            if (reads.get() == 2) throw stopped;
        })).isSameAs(stopped);
        assertThat(reads).hasValue(2);
    }

    private static List<DocumentRootSchemaEvidence> bundles(Fragment fragment, Fixture fixture) throws Exception {
        var inventory = DocumentAnyRootInventory.inspect(CONTAINER, slot(fragment.part()), fragment.bytes(), "doc",
                FRAGMENT_LIMITS, () -> {});
        var bundles = new ArrayList<DocumentRootSchemaEvidence>();
        for (var root : inventory.roots()) {
            var rootAsset = fixture.assetsByUrl().get(root.envelope().getTypeUrl());
            var checked = DocumentPayloadCheck.checkAssets(rootAsset, root.envelope(), root.envelope().getTypeUrl(),
                    VALIDATOR, payloadLimits(1_000_000), () -> {}, url -> fixture.assetsByUrl().get(url));
            var paths = DocumentSchemaOccurrenceProjection.project(checked.payload(), () -> {});
            bundles.add(DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1)
                    .setRoot(DocumentSchemaRootProjection.project(inventory, root, () -> {}))
                    .addAllOccurrences(paths).build());
        }
        return List.copyOf(bundles);
    }

    private static List<DocumentFragmentSchemaReplay.CheckedRoot> check(Fragment fragment,
            List<DocumentRootSchemaEvidence> bundles, Fixture fixture, DocumentFragmentSchemaReplay.Limits limits,
            DocumentRetainedSchemaAssets retained, Runnable control) throws Exception {
        return check(fragment, bundles, fixture, limits, retained, control, "doc");
    }

    private static List<DocumentFragmentSchemaReplay.CheckedRoot> check(Fragment fragment,
            List<DocumentRootSchemaEvidence> bundles, Fixture fixture, DocumentFragmentSchemaReplay.Limits limits,
            DocumentRetainedSchemaAssets retained, Runnable control, String docId) throws Exception {
        return DocumentFragmentSchemaReplay.check(CONTAINER, slot(fragment.part()), fragment.bytes(), docId,
                bundles, fixture.metadata(), retained, VALIDATOR, limits, control);
    }

    private static DocumentFragmentSchemaReplay.Limits limits(int maxBytes) {
        return new DocumentFragmentSchemaReplay.Limits(FRAGMENT_LIMITS, payloadLimits(maxBytes), OCCURRENCE_LIMITS,
                REPLAY_LIMITS, 1_000_000);
    }

    private static DocumentPayloadCheck.Limits payloadLimits(int maxBytes) {
        return new DocumentPayloadCheck.Limits(maxBytes, 10_000, 30, 100, 10_000);
    }

    private static DocumentRetainedSchemaAssets retained(Fixture fixture) {
        return new DocumentRetainedSchemaAssets(hash -> Optional.ofNullable(fixture.descriptors().get(hash)),
                new DocumentRetainedSchemaAssets.Limits(20, 16_000_000,
                        new ClosedDescriptorSet.Limits(4_000_000, 1024, 10_000, 100)));
    }

    private static Fragment part(Fixture fixture, DocumentPart part) {
        return part(fixture.document(), part);
    }

    private static Fragment part(Document document, DocumentPart part) {
        var fragment = DocumentPartCodec.split(document, PartLayouts.document()).stream()
                .filter(candidate -> candidate.part() == part).findFirst().orElseThrow();
        return new Fragment(part, ByteString.copyFrom(fragment.bytes()));
    }

    private static DocumentPublicationSlot slot(DocumentPart part) {
        return DocumentPublicationSlot.newBuilder().setPart(part).build();
    }

    private static Fixture fixture() throws Exception {
        var stringAsset = asset(StringValue.getDescriptor());
        var timestampAsset = asset(Timestamp.getDescriptor());
        var byUrl = Map.of(stringAsset.metadata().getTypeUrl(), stringAsset.bound(),
                timestampAsset.metadata().getTypeUrl(), timestampAsset.bound());
        var metadata = new HashMap<DocumentPayloadCheck.SchemaKey, RepositorySchemaAsset>();
        var descriptors = new HashMap<String, ByteString>();
        for (var asset : List.of(stringAsset, timestampAsset)) {
            var meta = asset.metadata();
            var key = new DocumentPayloadCheck.SchemaKey(meta.getTypeUrl(), meta.getArtifactSha256());
            metadata.put(key, meta);
            descriptors.put(meta.getArtifactSha256(), asset.bytes());
        }
        var document = document("doc", Any.pack(StringValue.of("one")),
                Any.pack(StringValue.of("one")), Any.pack(Timestamp.newBuilder().setSeconds(2).build()));
        return new Fixture(document, byUrl, Map.copyOf(metadata), Map.copyOf(descriptors));
    }

    private static Document document(String id, Any core, Any firstShape, Any secondShape) {
        return Document.newBuilder().setDocId(id).setStructuredData(core)
                .putParserResults("one", ParserResult.newBuilder().setDocument(ParserDocument.newBuilder().setShape(firstShape)).build())
                .putParserResults("two", ParserResult.newBuilder().setDocument(ParserDocument.newBuilder().setShape(secondShape)).build())
                .build();
    }

    private static Asset asset(com.google.protobuf.Descriptors.Descriptor type) throws Exception {
        var closure = DescriptorFingerprints.closure(type);
        var bytes = closure.toByteString();
        var condition = PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build();
        var schema = DocumentSchemaBinding.bind(condition, bytes,
                new ClosedDescriptorSet.Limits(4_000_000, 1024, 10_000, 100), () -> {});
        String typeUrl = "type.googleapis.com/" + type.getFullName();
        var metadata = RepositorySchemaAsset.newBuilder().setSchema(condition).setTypeUrl(typeUrl)
                .setArtifactSha256(schema.artifactSha256())
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("fixture producer did not identify its compiler")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1")))
                .build();
        var bound = DocumentSchemaAssetBinding.bind(metadata, bytes,
                new ClosedDescriptorSet.Limits(4_000_000, 1024, 10_000, 100), () -> {});
        return new Asset(bytes, metadata, bound);
    }

    private static DocumentSchemaBinding bind(com.google.protobuf.Descriptors.Descriptor type) {
        var closure = DescriptorFingerprints.closure(type);
        return DocumentSchemaBinding.bind(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build(), closure.toByteString(),
                new ClosedDescriptorSet.Limits(16_000_000, 1024, 10_000, 100), () -> {});
    }

    private record Fragment(DocumentPart part, ByteString bytes) {}
    private record Asset(ByteString bytes, RepositorySchemaAsset metadata, DocumentSchemaAssetBinding bound) {}
    private record Fixture(Document document, Map<String, DocumentSchemaAssetBinding> assetsByUrl,
            Map<DocumentPayloadCheck.SchemaKey, RepositorySchemaAsset> metadata, Map<String, ByteString> descriptors) {}
}
