package ai.protomolt.proto.projection;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.projection.fixture.ProjectionErrorKind;
import ai.protomolt.proto.projection.fixture.ProjectionExpectedError;
import ai.protomolt.proto.projection.fixture.ProjectionExpectedSuccess;
import ai.protomolt.proto.projection.fixture.ProjectionFixtureBundle;
import ai.protomolt.proto.projection.fixture.ProjectionFixtureCase;
import ai.protomolt.proto.projection.fixture.ProjectionPhase;
import ai.protomolt.proto.projection.test.AllKeyMapView;
import ai.protomolt.proto.projection.test.BadMapLiteralDoc;
import ai.protomolt.proto.projection.test.Blob;
import ai.protomolt.proto.projection.test.BlobChecksumType;
import ai.protomolt.proto.projection.test.BrokenMapChildParent;
import ai.protomolt.proto.projection.test.DuplicateCoercedKeyDoc;
import ai.protomolt.proto.projection.test.MapChildMaskView;
import ai.protomolt.proto.projection.test.MapCycleSource;
import ai.protomolt.proto.projection.test.MapCycleTarget;
import ai.protomolt.proto.projection.test.MapFallbackAndCelView;
import ai.protomolt.proto.projection.test.MapFromRepeatedDoc;
import ai.protomolt.proto.projection.test.MapFromStructPathDoc;
import ai.protomolt.proto.projection.test.MapProjectionTestProto;
import ai.protomolt.proto.projection.test.MapZoo;
import ai.protomolt.proto.projection.test.NegativeIntoUnsignedDoc;
import ai.protomolt.proto.projection.test.Note;
import ai.protomolt.proto.projection.test.NullMapValueDoc;
import ai.protomolt.proto.projection.test.OverflowUint64IntoInt64Doc;
import ai.protomolt.proto.projection.test.PartState;
import ai.protomolt.proto.projection.test.Rendition;
import ai.protomolt.proto.projection.test.RepeatedFromMapDoc;
import ai.protomolt.proto.projection.test.SingularFromMapDoc;
import ai.protomolt.proto.projection.test.UndeclaredMapChildDoc;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deterministic builder for the language-neutral map-projection conformance bundle (#981, #988).
 */
final class ProjectionFixtureCatalog {

    private static final int HIGH_UINT32 = (int) 3_000_000_000L;
    private static final long MAX_UINT64_BITS = -1L;

    private static final List<String> ZOO_RESOLVABLE = List.of(
            MapZoo.getDescriptor().getFullName(),
            Blob.getDescriptor().getFullName());

    private ProjectionFixtureCatalog() {
    }

    static byte[] buildDeterministicBundleBytes() {
        return MessageFingerprints.deterministicBytes(buildBundle());
    }

    static ProjectionFixtureBundle buildBundle() {
        FileDescriptorSet descriptorSet = collectClosure(MapProjectionTestProto.getDescriptor());
        ByteString descriptorBytes = ByteString.copyFrom(
                MessageFingerprints.deterministicBytes(descriptorSet));

        MapZoo fullZoo = buildFullZoo();
        MapZoo secondaryZoo = MapZoo.newBuilder().putSecondaryLabels("fallback_key", 7).build();
        MapZoo emptyZoo = MapZoo.getDefaultInstance();

        return ProjectionFixtureBundle.newBuilder()
                .setDescriptorSetBytes(descriptorBytes)
                .addCases(successCase(
                        "all_key_domains_and_nested_children",
                        "Projects all 12 protobuf map key types, unsigned boundaries, enums by wire number, WKTs, pass-through messages, and child-projected map values",
                        AllKeyMapView.getDescriptor(),
                        ZOO_RESOLVABLE,
                        fullZoo,
                        List.of("by_bool", "by_i32_widened", "by_si32", "by_sf32",
                                "by_i64_text", "by_si64", "by_sf64", "by_u32", "by_f32",
                                "by_u64", "by_f64", "parsed_numbers", "states",
                                "renditions", "carried", "timestamps", "widened_u32",
                                "u64_text", "author"),
                        List.of()))
                .addCases(successCase(
                        "empty_primary_falls_through_to_secondary_and_explicit_empty_wins",
                        "Empty protobuf map on primary path falls through to secondary path while explicit empty CEL and struct_value literals skip default_from",
                        MapFallbackAndCelView.getDescriptor(),
                        ZOO_RESOLVABLE,
                        secondaryZoo,
                        List.of("resolved_labels", "cel_unsigned_map",
                                "cel_dynamic_values", "literal_int_keys"),
                        List.of("explicit_empty_cel", "explicit_empty_literal")))
                .addCases(successCase(
                        "all_candidate_paths_empty_triggers_default_from",
                        "When all candidate map paths are empty on source, default_from struct_value populates the target map",
                        MapFallbackAndCelView.getDescriptor(),
                        ZOO_RESOLVABLE,
                        emptyZoo,
                        List.of("resolved_labels", "cel_unsigned_map",
                                "cel_dynamic_values", "literal_int_keys"),
                        List.of("explicit_empty_cel", "explicit_empty_literal")))
                .addCases(successCase(
                        "child_map_mask_stays_complete_and_struct_subpath_truncates",
                        "Whole-map path feeding a CEL-bearing child projection keeps parent sourceMask complete and truncates Struct sub-paths",
                        MapChildMaskView.getDescriptor(),
                        ZOO_RESOLVABLE,
                        fullZoo,
                        List.of("renditions", "author"),
                        List.of()))
                .addCases(errorCase(
                        "broken_map_child_cel_fails_at_compile_time",
                        "Child projection on map value type validates CEL eagerly when compiling parent",
                        BrokenMapChildParent.getDescriptor(),
                        ZOO_RESOLVABLE,
                        emptyZoo,
                        ProjectionPhase.PROJECTION_PHASE_COMPILE,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_CEL_COMPILES_NOWHERE,
                        "BrokenRendition.boom"))
                .addCases(errorCase(
                        "duplicate_coerced_map_key_refused",
                        "Distinct source keys ('1' and '01') colliding after int32 coercion fail projection",
                        DuplicateCoercedKeyDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        MapZoo.newBuilder().putTextMap("1", "a").putTextMap("01", "b").build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_DUPLICATE_MAP_KEY,
                        "DuplicateCoercedKeyDoc.by_int"))
                .addCases(errorCase(
                        "null_map_value_refused_for_non_value_target",
                        "CEL map containing null entry value fails on non-Value target map",
                        NullMapValueDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        emptyZoo,
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_NULL_MAP_ENTRY,
                        "NullMapValueDoc.bad"))
                .addCases(errorCase(
                        "undeclared_child_message_type_in_map_value_refused",
                        "Map value message outside target type and declared child sources fails projection",
                        UndeclaredMapChildDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        MapZoo.newBuilder().putNotesByKey("k", Note.newBuilder().setText("x").build()).build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_UNDECLARED_CHILD_SOURCE,
                        "UndeclaredMapChildDoc.renditions"))
                .addCases(errorCase(
                        "repeated_list_into_map_target_refused",
                        "Repeated list source cannot populate a protobuf map target",
                        MapFromRepeatedDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        MapZoo.newBuilder().addRawScores(1).build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_TYPE_COERCION,
                        "MapFromRepeatedDoc.bad"))
                .addCases(errorCase(
                        "map_source_into_repeated_target_refused",
                        "Protobuf map source cannot populate an ordinary repeated field",
                        RepeatedFromMapDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        MapZoo.newBuilder().putByI32(1, "one").build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_TYPE_COERCION,
                        "RepeatedFromMapDoc.bad"))
                .addCases(errorCase(
                        "map_source_into_singular_target_refused",
                        "Protobuf map source cannot populate a singular field",
                        SingularFromMapDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        MapZoo.newBuilder().putByI32(1, "one").build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_TYPE_COERCION,
                        "SingularFromMapDoc.bad"))
                .addCases(errorCase(
                        "struct_source_path_into_typed_map_refused",
                        "google.protobuf.Struct source field via paths is not implicitly unpacked into typed map",
                        MapFromStructPathDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        MapZoo.newBuilder().setMetadata(Struct.newBuilder()
                                .putFields("k", Value.newBuilder().setStringValue("v").build())).build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_TYPE_COERCION,
                        "MapFromStructPathDoc.bad"))
                .addCases(errorCase(
                        "non_struct_literal_on_map_target_refused",
                        "Scalar literal on a map target fails with ProjectionException",
                        BadMapLiteralDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        emptyZoo,
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_TYPE_COERCION,
                        "BadMapLiteralDoc.bad"))
                .addCases(errorCase(
                        "negative_integer_into_unsigned_refused",
                        "Negative signed integer fails unsigned uint32 range check",
                        NegativeIntoUnsignedDoc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        MapZoo.newBuilder().setSignedVal(-1).build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_TYPE_COERCION,
                        "NegativeIntoUnsignedDoc.bad_u32"))
                .addCases(errorCase(
                        "overflowing_uint64_into_signed_int64_refused",
                        "Unsigned uint64 above Long.MAX_VALUE fails signed int64 range check",
                        OverflowUint64IntoInt64Doc.getDescriptor(),
                        ZOO_RESOLVABLE,
                        MapZoo.newBuilder().setHighU64(MAX_UINT64_BITS).build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_TYPE_COERCION,
                        "OverflowUint64IntoInt64Doc.bad_i64"))
                .addCases(errorCase(
                        "order_independent_map_cycle_detected",
                        "Recursive map child projection catches (target, source) re-entry regardless of map entry order",
                        MapCycleTarget.getDescriptor(),
                        List.of(MapCycleSource.getDescriptor().getFullName()),
                        MapCycleSource.newBuilder().putTags("a", 1).putTags("b", 2).build(),
                        ProjectionPhase.PROJECTION_PHASE_PROJECT,
                        ProjectionErrorKind.PROJECTION_ERROR_KIND_CYCLE_DETECTED,
                        "MapCycleTarget re-enters itself"))
                .build();
    }

    private static ProjectionFixtureCase successCase(
            String name,
            String description,
            Descriptor target,
            List<String> resolvableNames,
            Message source,
            List<String> presentFields,
            List<String> absentFields) {
        SourceResolver resolver = SourceResolver.of(
                MapZoo.getDescriptor(), Blob.getDescriptor(), MapCycleSource.getDescriptor());
        MessageProjection compiled = MessageProjection.forTarget(target, resolver).orElseThrow();
        DynamicMessage projected = compiled.project(source);
        MessageProjection.SourceMask sourceMask = compiled.sourceMask(source.getDescriptorForType());

        return ProjectionFixtureCase.newBuilder()
                .setName(name)
                .setDescription(description)
                .setTargetType(target.getFullName())
                .addAllResolvableSourceTypes(resolvableNames)
                .setSourceType(source.getDescriptorForType().getFullName())
                .setSourceBytes(ByteString.copyFrom(MessageFingerprints.deterministicBytes(source)))
                .setSuccess(ProjectionExpectedSuccess.newBuilder()
                        .setDeterministicTargetBytes(
                                ByteString.copyFrom(MessageFingerprints.deterministicBytes(projected)))
                        .setTargetMask(compiled.targetMask())
                        .setSourceMask(sourceMask.fieldMask())
                        .setSourceMaskComplete(sourceMask.complete())
                        .addAllPresentFields(presentFields)
                        .addAllAbsentFields(absentFields))
                .build();
    }

    private static ProjectionFixtureCase errorCase(
            String name,
            String description,
            Descriptor target,
            List<String> resolvableNames,
            Message source,
            ProjectionPhase phase,
            ProjectionErrorKind kind,
            String messageContains) {
        return ProjectionFixtureCase.newBuilder()
                .setName(name)
                .setDescription(description)
                .setTargetType(target.getFullName())
                .addAllResolvableSourceTypes(resolvableNames)
                .setSourceType(source.getDescriptorForType().getFullName())
                .setSourceBytes(ByteString.copyFrom(MessageFingerprints.deterministicBytes(source)))
                .setError(ProjectionExpectedError.newBuilder()
                        .setPhase(phase)
                        .setKind(kind)
                        .setMessageContains(messageContains))
                .build();
    }

    private static FileDescriptorSet collectClosure(FileDescriptor root) {
        Map<String, FileDescriptorProto> files = new LinkedHashMap<>();
        collectFile(root, files);
        return FileDescriptorSet.newBuilder().addAllFile(files.values()).build();
    }

    private static void collectFile(FileDescriptor fd, Map<String, FileDescriptorProto> out) {
        if (out.containsKey(fd.getName())) {
            return;
        }
        for (FileDescriptor dep : fd.getDependencies()) {
            collectFile(dep, out);
        }
        out.put(fd.getName(), fd.toProto());
    }

    private static MapZoo buildFullZoo() {
        Blob pdf = Blob.newBuilder()
                .setBlobId("b-1")
                .setStorageKey("s3://b-1")
                .setFilename("report.pdf")
                .setMimeType("application/pdf")
                .setSizeBytes(2048L)
                .setChecksum("deadbeef")
                .setChecksumType(BlobChecksumType.BLOB_CHECKSUM_TYPE_SHA256)
                .build();
        Blob raw = Blob.newBuilder()
                .setBlobId("b-2")
                .setData(ByteString.copyFromUtf8("hello"))
                .setSizeBytes(5L)
                .build();
        Rendition existing = Rendition.newBuilder()
                .setName("kept-verbatim")
                .setBlobId("b-9")
                .build();
        return MapZoo.newBuilder()
                .putByBool(true, "yes")
                .putByBool(false, "no")
                .putByI32(-42, "neg")
                .putBySi32(-7, 700L)
                .putBySf32(12, "dozen")
                .putByI64(-9_000_000_000L, "wide-neg")
                .putBySi64(-99L, "zigzag64")
                .putBySf64(1234567890123L, "fixed-signed")
                .putByU32(HIGH_UINT32, MAX_UINT64_BITS)
                .putByF32(HIGH_UINT32, "fixed-u32")
                .putByU64(MAX_UINT64_BITS, "max-u64")
                .putByF64(MAX_UINT64_BITS, ByteString.copyFrom(new byte[]{1, 2, 3}))
                .putTextMap("3000000000", "9007199254740993")
                .putPartStates("alpha", PartState.PART_STATE_WRITTEN)
                .putPartStates("beta", PartState.PART_STATE_CARRIED)
                .putBlobsByKey("pdf", pdf)
                .putBlobsByKey("raw", raw)
                .putCarriedByKey("existing", existing)
                .putTimestampsByKey("created", Timestamp.newBuilder().setSeconds(1_700_000_000L).build())
                .setMetadata(Struct.newBuilder()
                        .putFields("author", Value.newBuilder().setStringValue("Ada").build())
                        .build())
                .setHighU32(HIGH_UINT32)
                .setHighU64(MAX_UINT64_BITS)
                .build();
    }
}
