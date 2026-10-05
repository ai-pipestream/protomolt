package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.*;
import com.google.protobuf.DescriptorProtos.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.*;

class DocumentRetainedPathMaterializationTest {
    private static final ClosedDescriptorSet.Limits DESCRIPTORS = new ClosedDescriptorSet.Limits(16_000_000, 256, 4096, 100);
    private static final DocumentRetainedPathMaterialization.Limits LIMITS = new DocumentRetainedPathMaterialization.Limits(
            100_000, 10, 10, new DocumentAnyMaterialization.Limits(100_000, 1000, 20));
    @TempDir Path store;

    private static final DocumentHistoricalResponseMaterialization.Limits WIRE_LIMITS =
            new DocumentHistoricalResponseMaterialization.Limits(100_000, 1000, 20);

    @Test void selectedWireDecodeOwnsReservationsAndUsesRetainedDefinition() throws Exception {
        var response = wireResponse();
        var reservations = new WireReservations();
        var result = DocumentHistoricalResponseMaterialization.read(wireRequest(response), response,
                WIRE_LIMITS, reservations, () -> {});
        assertThat(reservations.held).isEqualTo(response.getSerializedSize() + response.getOriginal().getValue().size());
        var view = result.view(() -> {});
        assertThat(view.response()).isSameAs(response);
        assertThat(view.value().getAllFields().keySet()).extracting(Descriptors.FieldDescriptor::getName)
                .containsExactly("first_label");
        assertThat(view.value().getField(view.value().getDescriptorForType().findFieldByNumber(1))).isEqualTo("first");
        var cancelled = new CancellationException();
        assertThatThrownBy(() -> result.view(() -> { throw cancelled; })).isSameAs(cancelled);
        result.close(); result.close();
        assertThat(reservations.held).isZero();
        assertThatThrownBy(() -> result.view(() -> {})).isInstanceOf(IllegalStateException.class);
    }

    @Test void selectedWireDecodeReleasesAtEveryCancellationAndReservationRefusal() throws Exception {
        var response = wireResponse(); var request = wireRequest(response);
        var reservations = new WireReservations();
        var total = new java.util.concurrent.atomic.AtomicInteger();
        try (var result = DocumentHistoricalResponseMaterialization.read(request, response, WIRE_LIMITS,
                reservations, total::incrementAndGet)) { result.view(() -> {}); }
        for (int point = 1; point <= total.get(); point++) {
            int stop = point;
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var cancelled = new CancellationException();
            assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(request, response,
                    WIRE_LIMITS, reservations, () -> { if (calls.incrementAndGet() == stop) throw cancelled; }))
                    .isSameAs(cancelled);
            assertThat(reservations.held).isZero();
        }
        var count = new java.util.concurrent.atomic.AtomicInteger();
        try (var result = DocumentHistoricalResponseMaterialization.read(request, response, WIRE_LIMITS,
                bytes -> { count.incrementAndGet(); return reservations.reserve(bytes); }, () -> {})) {}
        for (int point = 1; point <= count.get(); point++) {
            int stop = point;
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var refused = new IllegalStateException("capacity refused");
            assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(request, response, WIRE_LIMITS,
                    bytes -> { if (calls.incrementAndGet() == stop) throw refused; return reservations.reserve(bytes); }, () -> {}))
                    .isSameAs(refused);
            assertThat(reservations.held).isZero();
        }
    }

    @Test void selectedWireDecodeRejectsInvalidRequestsAndByteLimits() throws Exception {
        var response = wireResponse(); var request = wireRequest(response);
        var reservations = new WireReservations();
        var unknown = UnknownFieldSet.newBuilder().addField(999,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        for (var invalid : List.of(request.toBuilder().clearLimits().build(),
                request.toBuilder().setLimits(request.getLimits().toBuilder().setMaxDecodedBytes(0)).build(),
                request.toBuilder().setSelection(request.getSelection().toBuilder().setUnknownFields(unknown)).build())) {
            assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(invalid, response,
                    WIRE_LIMITS, reservations, () -> {})).isInstanceOf(IllegalArgumentException.class);
            assertThat(reservations.held).isZero();
        }
        assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(request, response,
                new DocumentHistoricalResponseMaterialization.Limits(0, 1000, 20), reservations, () -> {}))
                .isInstanceOf(DocumentHistoricalResponseMaterialization.ResourceLimit.class);
        assertThat(reservations.held).isZero();
    }

    @Test void selectedWireDecodeDistinguishesMalformedPayloadFromWireCapacity() throws Exception {
        var response = wireResponse(); var reservations = new WireReservations();
        var malformed = withWireValue(response, ByteString.copyFrom(new byte[] {0}));
        assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(wireRequest(malformed), malformed,
                WIRE_LIMITS, reservations, () -> {})).isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(DocumentHistoricalResponseMaterialization.ResourceLimit.class)
                .hasMessageContaining("MALFORMED_PAYLOAD");
        assertThat(reservations.held).isZero();
        var repeated = withWireValue(response, response.getOriginal().getValue().concat(response.getOriginal().getValue()));
        assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(wireRequest(repeated), repeated,
                new DocumentHistoricalResponseMaterialization.Limits(100_000, 1, 20), reservations, () -> {}))
                .isInstanceOf(DocumentHistoricalResponseMaterialization.ResourceLimit.class);
        assertThat(reservations.held).isZero();
        var corrupt = response.toBuilder().setDefinition(response.getDefinition().toBuilder()
                .setDescriptorArtifact(ByteString.copyFromUtf8("corrupt"))).build();
        assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(wireRequest(corrupt), corrupt,
                WIRE_LIMITS, reservations, () -> {})).isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(DocumentHistoricalResponseMaterialization.ResourceLimit.class);
        assertThat(reservations.held).isZero();
    }

    @Test void selectedWireDecodeClassifiesDescriptorCapacityAndPreservesControlFailure() throws Exception {
        var response = wireResponse(); var request = wireRequest(response);
        var files = FileDescriptorSet.parseFrom(response.getDefinition().getDescriptorArtifact());
        var oversized = FileDescriptorSet.newBuilder();
        for (int i = 0; i < 257; i++) oversized.addFile(files.getFile(0).toBuilder().setName("file" + i + ".proto"));
        var changed = response.toBuilder().setDefinition(response.getDefinition().toBuilder()
                .setDescriptorArtifact(oversized.build().toByteString())).build();
        var reservations = new WireReservations();
        assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(request, changed,
                WIRE_LIMITS, reservations, () -> {}))
                .isInstanceOf(DocumentHistoricalResponseMaterialization.ResourceLimit.class)
                .hasCauseInstanceOf(ClosedDescriptorSet.LimitExceededException.class);
        assertThat(reservations.held).isZero();
        var injected = new ClosedDescriptorSet.LimitExceededException("host control failure");
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> DocumentHistoricalResponseMaterialization.read(request, response,
                WIRE_LIMITS, reservations, () -> { if (calls.incrementAndGet() == 3) throw injected; }))
                .isSameAs(injected);
        assertThat(reservations.held).isZero();
    }

    private static ReadHistoricalOccurrenceResponse withWireValue(ReadHistoricalOccurrenceResponse response,
            ByteString value) {
        var path = response.getPath().toBuilder();
        int last = path.getStepsCount() - 1;
        path.setSteps(last, path.getSteps(last).toBuilder().setAnyBoundary(path.getSteps(last).getAnyBoundary()
                .toBuilder().setValueSizeBytes(value.size()).setValueSha256(DocumentSchemaOccurrences.sha256(value, () -> {}))));
        return response.toBuilder().setOriginal(response.getOriginal().toBuilder().setValue(value)).setPath(path)
                .setSelection(response.getSelection().toBuilder()
                        .setPathSha256(DocumentSchemaEvidenceCodec.encode(path.build(), () -> {}).sha256())).build();
    }

    @Test void selectedWireEvidenceRejectsChangedRequestContentAndArtifacts() throws Exception {
        var response = wireResponse();
        var request = wireRequest(response);
        var reservations = new WireReservations();
        DocumentHistoricalResponseVerifier.verify(request, response, reservations, () -> {});
        assertThat(reservations.held).isZero();
        for (var changed : List.of(
                response.toBuilder().setRevisionId(UUID.randomUUID().toString()).build(),
                response.toBuilder().setAddress(response.getAddress().toBuilder().setAccountId("another-account")).build(),
                response.toBuilder().setSelection(response.getSelection().toBuilder().setRevisionOrdinal(1)).build(),
                response.toBuilder().setOriginal(response.getOriginal().toBuilder().setValue(ByteString.copyFromUtf8("changed"))).build(),
                response.toBuilder().setDefinition(response.getDefinition().toBuilder()
                        .setDescriptorArtifact(ByteString.copyFromUtf8("not descriptors"))).build(),
                response.toBuilder().setDefinition(response.getDefinition().toBuilder()
                        .setMetadataArtifact(ByteString.copyFromUtf8("not metadata"))).build(),
                response.toBuilder().setPath(response.getPath().toBuilder().setSteps(2, index(1))).build())) {
            assertThatThrownBy(() -> DocumentHistoricalResponseVerifier.verify(request, changed, reservations, () -> {}))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(reservations.held).isZero();
        }
    }

    @Test void selectedWireVerificationReleasesReservationsAtEveryCancellationPoint() throws Exception {
        var response = wireResponse();
        var request = wireRequest(response);
        var reservations = new WireReservations();
        var total = new java.util.concurrent.atomic.AtomicInteger();
        DocumentHistoricalResponseVerifier.verify(request, response, reservations, total::incrementAndGet);
        for (int point = 1; point <= total.get(); point++) {
            int stopAt = point;
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var cancelled = new CancellationException("wire verification cancelled");
            assertThatThrownBy(() -> DocumentHistoricalResponseVerifier.verify(request, response, reservations,
                    () -> { if (calls.incrementAndGet() == stopAt) throw cancelled; })).isSameAs(cancelled);
            assertThat(reservations.held).isZero();
        }
    }

    private ReadHistoricalOccurrenceResponse wireResponse() throws Exception {
        var f = fixture(false);
        var path = path(f, 0);
        var selection = HistoricalOccurrenceSelection.newBuilder()
                .setRootSha256(DocumentSchemaEvidenceCodec.encode(f.locator, () -> {}).sha256())
                .setPathSha256(DocumentSchemaEvidenceCodec.encode(path, () -> {}).sha256());
        return ReadHistoricalOccurrenceResponse.newBuilder()
                .setAddress(NodeAddress.newBuilder().setAccountId("account").setDocId("doc")
                        .setGraphId("graph").setGraphAddressId("node"))
                .setRevisionId(UUID.randomUUID().toString()).setSelection(selection)
                .setOriginal(f.first.value).setRoot(f.locator).setPath(path)
                .setDefinition(HistoricalRetainedDefinition.newBuilder().setReference(f.first.reference.toProto())
                        .setDescriptorArtifact(ByteString.copyFrom(Files.readAllBytes(store.resolve(f.first.reference.descriptorSha256()))))
                        .setMetadataArtifact(ByteString.copyFrom(Files.readAllBytes(store.resolve(f.first.reference.metadataSha256())))))
                .build();
    }

    private static ReadHistoricalOccurrenceRequest wireRequest(ReadHistoricalOccurrenceResponse response) {
        return ReadHistoricalOccurrenceRequest.newBuilder().setAddress(response.getAddress())
                .setRevisionId(response.getRevisionId()).setSelection(response.getSelection())
                .setLimits(HistoricalMaterializationLimits.newBuilder().setMaxFragmentBytes(100_000)
                        .setMaxEvidenceBytes(100_000).setMaxRetainedBytes(100_000).setMaxReferences(10)
                        .setMaxDecodedBytes(100_000).setMaxBoundaries(10)).build();
    }

    private static final class WireReservations implements DocumentAdmissionReservations {
        private long held;
        @Override public Lease reserve(long bytes) {
            held += bytes;
            var closed = new java.util.concurrent.atomic.AtomicBoolean();
            return () -> { if (closed.compareAndSet(false, true)) held -= bytes; };
        }
    }

    @Test void repeatedChildrenWithTheSameUrlDecodeThroughIndependentPinnedDefinitions() throws Exception {
        var f = fixture(false);
        for (int index = 0; index < 2; index++) {
            var path = path(f, index);
            var result = read(f, path, references(f), LIMITS, () -> {});
            assertThat(result.value().getAllFields().keySet()).extracting(Descriptors.FieldDescriptor::getName)
                    .containsExactly(index == 0 ? "first_label" : "second_label");
            assertThat(result.occurrence().prefix()).containsExactlyElementsOf(path.getStepsList().subList(0, 3));
            assertThat(result.original()).isEqualTo(index == 0 ? f.first.value : f.second.value);
        }
    }

    @Test void singularAndSemanticMapCoordinatesSelectTheActualChild() throws Exception {
        var f = fixture(false);
        var singular = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary(f.root))
                .addSteps(field(1)).addSteps(boundary(f.first)).build();
        assertThat(read(f, singular, references(f), LIMITS, () -> {}).original()).isEqualTo(f.first.value);
        var map = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary(f.root))
                .addSteps(field(3)).addSteps(mapKey("two")).addSteps(boundary(f.second)).build();
        assertThat(read(f, map, references(f), LIMITS, () -> {}).original()).isEqualTo(f.second.value);
    }

    @Test void changedIndexesWrongStepKindsAndUnknownFieldsAreDataLoss() throws Exception {
        var f = fixture(false);
        var valid = path(f, 0);
        for (var bad : List.of(valid.toBuilder().setSteps(2, index(9)).build(),
                valid.toBuilder().setSteps(2, index(1)).build(), // Different value digest and schema selection.
                valid.toBuilder().setSteps(1, field(99)).build(),
                valid.toBuilder().setSteps(2, mapKey("one")).build(),
                valid.toBuilder().setSteps(1, field(3)).build())) {
            assertThatThrownBy(() -> read(f, bad, references(f), LIMITS, () -> {}))
                    .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class);
        }
        var unknown = fixture(true);
        assertThatThrownBy(() -> read(unknown, path(unknown, 0), references(unknown), LIMITS, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("unknown fields");
    }

    @Test void duplicateMapKeysFailEvenWhenTheRequestedKeyIsElsewhere() throws Exception {
        var f = fixture(false, true);
        var map = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary(f.root))
                .addSteps(field(3)).addSteps(mapKey("two")).addSteps(boundary(f.second)).build();
        assertThatThrownBy(() -> read(f, map, references(f), LIMITS, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("duplicate map keys");
    }

    @Test void malformedMapEntryDescriptorCannotHideAnExtraRoute() throws Exception {
        var f = fixture(false, false, FieldDescriptorProto.Type.TYPE_STRING, "one", "two", true);
        var map = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary(f.root))
                .addSteps(field(3)).addSteps(mapKey("two")).addSteps(boundary(f.second)).build();
        assertThatThrownBy(() -> read(f, map, references(f), LIMITS, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("map entry descriptor");
    }

    @ParameterizedTest
    @EnumSource(value = FieldDescriptorProto.Type.class, names = {"TYPE_STRING", "TYPE_BOOL", "TYPE_INT32", "TYPE_SINT32",
            "TYPE_SFIXED32", "TYPE_UINT32", "TYPE_FIXED32", "TYPE_INT64", "TYPE_SINT64", "TYPE_SFIXED64", "TYPE_UINT64", "TYPE_FIXED64"})
    void mapKeyTypesPreserveDefaultAndUnsignedIdentities(FieldDescriptorProto.Type type) throws Exception {
        Object zero = switch (type) {
            case TYPE_STRING -> "";
            case TYPE_BOOL -> false;
            case TYPE_INT32, TYPE_SINT32, TYPE_SFIXED32, TYPE_UINT32, TYPE_FIXED32 -> 0;
            default -> 0L;
        };
        Object wanted = switch (type) {
            case TYPE_STRING -> "key";
            case TYPE_BOOL -> true;
            case TYPE_INT32, TYPE_SINT32, TYPE_SFIXED32, TYPE_UINT32, TYPE_FIXED32 -> -1;
            default -> -1L;
        };
        var key = RepositoryOccurrenceMapKey.newBuilder().setType(RepositoryOccurrenceKeyType.valueOf(
                "REPOSITORY_OCCURRENCE_KEY_TYPE_" + type.name().substring(5)));
        switch (type) {
            case TYPE_STRING -> key.setStringValue((String) wanted);
            case TYPE_BOOL -> key.setBoolValue((Boolean) wanted);
            case TYPE_UINT32, TYPE_FIXED32 -> key.setUnsignedValue(Integer.toUnsignedLong((Integer) wanted));
            case TYPE_UINT64, TYPE_FIXED64 -> key.setUnsignedValue((Long) wanted);
            default -> key.setSignedValue(((Number) wanted).longValue());
        }
        var f = fixture(false, false, type, zero, wanted);
        var path = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary(f.root))
                .addSteps(field(3)).addSteps(RepositorySchemaOccurrenceStep.newBuilder().setMapKey(key))
                .addSteps(boundary(f.second)).build();
        assertThat(read(f, path, references(f), LIMITS, () -> {}).original()).isEqualTo(f.second.value);
        var duplicate = fixture(false, true, type, zero, wanted);
        var duplicatedPath = path.toBuilder().setSteps(0, boundary(duplicate.root)).build();
        assertThatThrownBy(() -> read(duplicate, duplicatedPath, references(duplicate), LIMITS, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("duplicate map keys");
    }

    @Test void missingChildArtifactAndConflictingFullReferencesCannotFallBack() throws Exception {
        var f = fixture(false);
        var references = new ArrayList<>(references(f));
        var original = f.first.reference;
        references.add(new DocumentSchemaAdmission.Reference(original.typeUrl(), original.descriptorSha256(),
                original.metadataCodec(), original.metadataVersion(), "a".repeat(64), Optional.empty()));
        assertThatThrownBy(() -> read(f, path(f, 0), references, LIMITS, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("conflicting");
        Files.delete(store.resolve(f.first.reference.descriptorSha256()));
        assertThatThrownBy(() -> read(f, path(f, 0), references(f), LIMITS, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("missing");
        assertThat(read(f, path(f, 1), references(f), LIMITS, () -> {}).original()).isEqualTo(f.second.value);
    }

    @Test void threeBoundariesRequireEveryParentAndNeverReturnAPartialTarget() throws Exception {
        var f = fixture(false);
        var outer = asset(Any.getDescriptor(), f.root.value);
        var wrapped = inventory(outer, f.first, f.second);
        var path = path(f, 0).toBuilder().clearSteps().addSteps(boundary(outer))
                .addAllSteps(path(f, 0).getStepsList()).build();
        var refs = List.of(outer.reference, f.root.reference, f.first.reference, f.second.reference);
        assertThat(read(wrapped, path, refs, LIMITS, () -> {}).original()).isEqualTo(f.first.value);
        assertThatThrownBy(() -> read(wrapped, path, refs,
                new DocumentRetainedPathMaterialization.Limits(100_000, 2, 4, LIMITS.perBoundary()), () -> {}))
                .isInstanceOf(DocumentRetainedPathMaterialization.LimitExceeded.class);
        Files.delete(store.resolve(f.root.reference.descriptorSha256()));
        assertThatThrownBy(() -> read(wrapped, path, refs, LIMITS, () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("missing");
    }

    @Test void aggregateBytesDecodeCountAndReferenceCountsAreBoundedBeforeReturningAnyChild() throws Exception {
        var f = fixture(false);
        long bytes = f.root.value.getValue().size() + f.first.value.getValue().size();
        var exact = new DocumentRetainedPathMaterialization.Limits(bytes, 2, 3, LIMITS.perBoundary());
        assertThat(read(f, path(f, 0), references(f), exact, () -> {}).original()).isEqualTo(f.first.value);
        for (var limit : List.of(new DocumentRetainedPathMaterialization.Limits(bytes - 1, 2, 3, LIMITS.perBoundary()),
                new DocumentRetainedPathMaterialization.Limits(bytes, 1, 3, LIMITS.perBoundary()),
                new DocumentRetainedPathMaterialization.Limits(bytes, 2, 2, LIMITS.perBoundary()),
                new DocumentRetainedPathMaterialization.Limits(bytes, 2, 3, new DocumentAnyMaterialization.Limits(1, 100, 10)))) {
            assertThatThrownBy(() -> read(f, path(f, 0), references(f), limit, () -> {}))
                    .isInstanceOf(DocumentRetainedPathMaterialization.LimitExceeded.class);
        }
    }

    @Test void cancellationAtEveryControlPointPropagatesWithoutReturningPartialParents() throws Exception {
        var f = fixture(false);
        var total = new java.util.concurrent.atomic.AtomicInteger();
        read(f, path(f, 0), references(f), LIMITS, total::incrementAndGet);
        for (int point = 1; point <= total.get(); point++) {
            int at = point;
            var calls = new java.util.concurrent.atomic.AtomicInteger();
            var stop = new CancellationException("cancel retained path");
            assertThatThrownBy(() -> read(f, path(f, 0), references(f), LIMITS,
                    () -> { if (calls.incrementAndGet() == at) throw stop; })).isSameAs(stop);
        }
    }

    private DocumentAnyMaterialization.Decoded read(Fixture f, RepositorySchemaOccurrencePath path,
            List<DocumentSchemaAdmission.Reference> references, DocumentRetainedPathMaterialization.Limits limits, Runnable control) {
        var reader = new DocumentRetainedSchemaAssets(hash -> {
            try {
                var file = store.resolve(hash);
                if (!Files.exists(file)) return Optional.empty();
                if (Files.size(file) > 16_000_000) throw new IllegalStateException("fixture exceeds load bound");
                return Optional.of(ByteString.copyFrom(Files.readAllBytes(file)));
            } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
        }, new DocumentRetainedSchemaAssets.Limits(10, 32_000_000, DESCRIPTORS));
        return DocumentRetainedPathMaterialization.read(0, f.inventory, f.locator, path, references, reader, limits, control);
    }
    private static List<DocumentSchemaAdmission.Reference> references(Fixture f) { return List.of(f.root.reference, f.first.reference, f.second.reference); }
    private static RepositorySchemaOccurrencePath path(Fixture f, int index) {
        return RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary(f.root))
                .addSteps(field(2)).addSteps(index(index)).addSteps(boundary(index == 0 ? f.first : f.second)).build();
    }
    private static RepositorySchemaOccurrenceStep field(int number) { return RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(number).build(); }
    private static RepositorySchemaOccurrenceStep index(int number) { return RepositorySchemaOccurrenceStep.newBuilder().setRepeatedIndex(number).build(); }
    private static RepositorySchemaOccurrenceStep mapKey(String key) {
        return RepositorySchemaOccurrenceStep.newBuilder().setMapKey(RepositoryOccurrenceMapKey.newBuilder()
                .setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING).setStringValue(key)).build();
    }
    private static RepositorySchemaOccurrenceStep boundary(Asset asset) {
        return RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                .setTypeUrl(asset.value.getTypeUrl()).setValueSha256(sha(asset.value.getValue())).setValueSizeBytes(asset.value.getValue().size())
                .setResolved(RepositoryResolvedSchema.newBuilder().setSchema(asset.schema).setArtifactSha256(asset.reference.descriptorSha256()))).build();
    }
    private Fixture fixture(boolean unknown) throws Exception { return fixture(unknown, false); }
    private Fixture fixture(boolean unknown, boolean duplicate) throws Exception {
        return fixture(unknown, duplicate, FieldDescriptorProto.Type.TYPE_STRING, "one", "two");
    }
    private Fixture fixture(boolean unknown, boolean duplicate, FieldDescriptorProto.Type keyType, Object firstKey, Object secondKey) throws Exception {
        return fixture(unknown, duplicate, keyType, firstKey, secondKey, false);
    }
    private Fixture fixture(boolean unknown, boolean duplicate, FieldDescriptorProto.Type keyType, Object firstKey, Object secondKey, boolean extraField) throws Exception {
        var firstType = child("first_label"); var secondType = child("second_label");
        var first = asset(firstType, DynamicMessage.newBuilder(firstType).setField(firstType.findFieldByNumber(1), "first").build());
        var second = asset(secondType, DynamicMessage.newBuilder(secondType).setField(secondType.findFieldByNumber(1), "second").build());
        var entry = DescriptorProto.newBuilder().setName("EntriesEntry").setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(keyType))
                .addField(messageField("value", 2, ".google.protobuf.Any", false));
        if (extraField) entry.addField(messageField("extra", 3, ".google.protobuf.Any", false));
        var rootProto = DescriptorProto.newBuilder().setName("Root").addNestedType(entry)
                .addField(messageField("single", 1, ".google.protobuf.Any", false))
                .addField(messageField("children", 2, ".google.protobuf.Any", true))
                .addField(messageField("entries", 3, ".walk.Root.EntriesEntry", true));
        var rootType = Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("root.proto").setPackage("walk")
                .setSyntax("proto3").addDependency(Any.getDescriptor().getFile().getName()).addMessageType(rootProto).build(),
                new Descriptors.FileDescriptor[] {Any.getDescriptor().getFile()}).getMessageTypes().getFirst();
        var builder = DynamicMessage.newBuilder(rootType).setField(rootType.findFieldByNumber(1), first.value)
                .addRepeatedField(rootType.findFieldByNumber(2), first.value).addRepeatedField(rootType.findFieldByNumber(2), second.value);
        var mapField = rootType.findFieldByNumber(3); var entryType = mapField.getMessageType();
        for (var key : duplicate ? List.of(firstKey, firstKey, secondKey) : List.of(firstKey, secondKey))
            builder.addRepeatedField(mapField, DynamicMessage.newBuilder(entryType).setField(entryType.findFieldByNumber(1), key)
                    .setField(entryType.findFieldByNumber(2), key.equals(firstKey) ? first.value : second.value).build());
        if (unknown) builder.setUnknownFields(UnknownFieldSet.newBuilder().addField(999, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build());
        var root = asset(rootType, builder.build());
        return inventory(root, first, second);
    }
    private Fixture inventory(Asset root, Asset first, Asset second) throws Exception {
        var document = Document.newBuilder().setDocId("doc").setStructuredData(root.value).build();
        var part = DocumentPartCodec.split(document, PartLayouts.document()).stream().filter(p -> p.part() == DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
        var closure = DescriptorFingerprints.closure(Document.getDescriptor());
        var container = DocumentSchemaBinding.bind(condition(Document.getDescriptor(), closure), closure.toByteString(), DESCRIPTORS, () -> {});
        var inventory = DocumentAnyRootInventory.inspect(container, DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build(),
                ByteString.copyFrom(part.bytes()), "doc", new DocumentAnyRootInventory.Limits(1_000_000, 100_000, 30, 100, 100), () -> {});
        return new Fixture(root, first, second, inventory, DocumentSchemaRootProjection.project(inventory, inventory.roots().getFirst(), () -> {}));
    }
    private static FieldDescriptorProto.Builder messageField(String name, int number, String type, boolean repeated) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(type).setLabel(repeated ? FieldDescriptorProto.Label.LABEL_REPEATED : FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }
    private static Descriptors.Descriptor child(String fieldName) throws Exception {
        return Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("child.proto").setPackage("walk").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Child").addField(FieldDescriptorProto.newBuilder()
                        .setName(fieldName).setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))).build(), new Descriptors.FileDescriptor[0]).getMessageTypes().getFirst();
    }
    private Asset asset(Descriptors.Descriptor descriptor, Message message) throws Exception {
        var closure = DescriptorFingerprints.closure(descriptor); var bytes = closure.toByteString();
        var condition = condition(descriptor, closure); var value = Any.pack(message);
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl(value.getTypeUrl()).setSchema(condition).setArtifactSha256(sha(bytes))
                .setCompilation(SchemaCompilationProvenance.newBuilder().setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN).setUnknownCompilerReason("fixture compiler not recorded")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("fixture").setVersion("1"))).build();
        var encoded = DocumentSchemaAssetCodec.encode(metadata, () -> {});
        Files.write(store.resolve(sha(bytes)), bytes.toByteArray()); Files.write(store.resolve(encoded.sha256()), encoded.bytes().toByteArray());
        return new Asset(value, condition, new DocumentSchemaAdmission.Reference(value.getTypeUrl(), sha(bytes),
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, encoded.sha256(), Optional.empty()));
    }
    private static PublicationSchemaCondition condition(Descriptors.Descriptor type, FileDescriptorSet closure) {
        return PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName()).setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build();
    }
    private static String sha(ByteString bytes) { return DocumentPartCodec.sha256Hex(bytes.toByteArray()); }
    private record Asset(Any value, PublicationSchemaCondition schema, DocumentSchemaAdmission.Reference reference) {}
    private record Fixture(Asset root, Asset first, Asset second, DocumentAnyRootInventory.Result inventory, DocumentSchemaRootLocator locator) {}
}
