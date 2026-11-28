package ai.protomolt.proto.projection;

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
import ai.protomolt.proto.projection.test.MapZoo;
import ai.protomolt.proto.projection.test.NegativeIntoUnsignedDoc;
import ai.protomolt.proto.projection.test.Note;
import ai.protomolt.proto.projection.test.NullMapValueDoc;
import ai.protomolt.proto.projection.test.OverflowUint64IntoInt64Doc;
import ai.protomolt.proto.projection.test.PartState;
import ai.protomolt.proto.projection.test.Rendition;
import ai.protomolt.proto.projection.test.RenditionState;
import ai.protomolt.proto.projection.test.RepeatedFromMapDoc;
import ai.protomolt.proto.projection.test.SingularFromMapDoc;
import ai.protomolt.proto.projection.test.UndeclaredMapChildDoc;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.WireFormat;
import com.google.protobuf.util.FieldMaskUtil;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Shared Java/Rust map projection contract tests (#981) and unsigned/FieldMask parity tests (#987).
 */
class MapProjectionTest {

    private static final int HIGH_UINT32 = (int) 3_000_000_000L;
    private static final long MAX_UINT64_BITS = -1L;
    private static final String MAX_UINT64_TEXT = "18446744073709551615";

    private static final SourceResolver ZOO_SOURCES = SourceResolver.of(
            MapZoo.getDescriptor(), Blob.getDescriptor());

    private static MessageProjection projection(Descriptor target) {
        return MessageProjection.forTarget(target, ZOO_SOURCES).orElseThrow();
    }

    private static MapZoo richZoo() {
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

    @Test
    void projectsAllTwelveMapKeyDomainsAndNestedValuesFromGeneratedAndDynamicSources() throws Exception {
        MapZoo generatedSource = richZoo();
        DynamicMessage dynamicSource = DynamicMessage.parseFrom(
                MapZoo.getDescriptor(), generatedSource.toByteString());

        for (var source : java.util.List.of(generatedSource, dynamicSource)) {
            DynamicMessage projectedDynamic = projection(AllKeyMapView.getDescriptor()).project(source);
            AllKeyMapView view = AllKeyMapView.parseFrom(
                    projectedDynamic.toByteString(), ExtensionRegistry.getEmptyRegistry());

            assertThat(view.getByBoolMap()).containsEntry(true, "yes").containsEntry(false, "no");
            assertThat(view.getByI32WidenedMap()).containsEntry(-42L, "neg");
            assertThat(view.getBySi32Map()).containsEntry(-7, 700L);
            assertThat(view.getBySf32Map()).containsEntry(12, "dozen");
            assertThat(view.getByI64TextMap()).containsEntry("-9000000000", "wide-neg");
            assertThat(view.getBySi64Map()).containsEntry(-99L, "zigzag64");
            assertThat(view.getBySf64Map()).containsEntry(1234567890123L, "fixed-signed");
            assertThat(view.getByU32Map()).containsEntry(HIGH_UINT32, MAX_UINT64_BITS);
            assertThat(view.getByF32Map()).containsEntry(HIGH_UINT32, "fixed-u32");
            assertThat(view.getByU64Map()).containsEntry(MAX_UINT64_BITS, "max-u64");
            assertThat(view.getByF64Map())
                    .containsEntry(MAX_UINT64_BITS, ByteString.copyFrom(new byte[]{1, 2, 3}));
            assertThat(view.getParsedNumbersMap())
                    .containsEntry(HIGH_UINT32, 9_007_199_254_740_993L);
            assertThat(view.getStatesMap())
                    .containsEntry("alpha", RenditionState.RENDITION_STATE_STORED)
                    .containsEntry("beta", RenditionState.RENDITION_STATE_INHERITED);
            assertThat(view.getRenditionsMap())
                    .containsEntry("pdf", Rendition.newBuilder()
                            .setName("report.pdf")
                            .setBlobId("b-1")
                            .setMimeType("application/pdf")
                            .setSizeBytes(2048L)
                            .setChecksum("sha256:deadbeef")
                            .build())
                    .containsEntry("raw", Rendition.newBuilder()
                            .setName("b-2")
                            .setBlobId("b-2")
                            .setMimeType("application/octet-stream")
                            .setSizeBytes(5L)
                            .setInline(true)
                            .build());
            assertThat(view.getCarriedMap())
                    .containsEntry("existing", Rendition.newBuilder()
                            .setName("kept-verbatim")
                            .setBlobId("b-9")
                            .build());
            assertThat(view.getTimestampsMap())
                    .containsEntry("created", Timestamp.newBuilder().setSeconds(1_700_000_000L).build());
            assertThat(view.getWidenedU32()).isEqualTo(3_000_000_000L);
            assertThat(view.getU64Text()).isEqualTo(MAX_UINT64_TEXT);
            assertThat(view.getAuthor()).isEqualTo("Ada");
        }
    }

    @Test
    void mergesDuplicateRawWireMapKeysWithLastEntryWinsBeforeCoercing() throws Exception {
        // Encode two MapZoo.text_map entries (field #12) with the same wire key "10":
        // DynamicMessage.parseFrom retains both raw entries unless merged by raw key.
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(raw);
        writeStringMapEntry(out, 12, "10", "100");
        writeStringMapEntry(out, 12, "10", "250");
        out.flush();

        DynamicMessage dynamicSource = DynamicMessage.parseFrom(
                MapZoo.getDescriptor(), raw.toByteArray());
        AllKeyMapView view = AllKeyMapView.parseFrom(
                projection(AllKeyMapView.getDescriptor()).project(dynamicSource).toByteString(),
                ExtensionRegistry.getEmptyRegistry());

        assertThat(view.getParsedNumbersMap()).containsExactlyEntriesOf(java.util.Map.of(10, 250L));
    }

    private static void writeStringMapEntry(
            CodedOutputStream out, int fieldNumber, String key, String value) throws Exception {
        ByteArrayOutputStream entryBuf = new ByteArrayOutputStream();
        CodedOutputStream entryOut = CodedOutputStream.newInstance(entryBuf);
        entryOut.writeString(1, key);
        entryOut.writeString(2, value);
        entryOut.flush();
        byte[] entryBytes = entryBuf.toByteArray();
        out.writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        out.writeUInt32NoTag(entryBytes.length);
        out.writeRawBytes(entryBytes);
    }

    @Test
    void emptySourceMapPathsFallThroughWhileExplicitEmptyCelAndLiteralsWin() throws Exception {
        MessageProjection proj = projection(MapFallbackAndCelView.getDescriptor());

        // Case 1: primary_labels is empty, secondary_labels has entries -> secondary wins.
        MapFallbackAndCelView fromSecondary = MapFallbackAndCelView.parseFrom(
                proj.project(MapZoo.newBuilder().putSecondaryLabels("sec", 2).build()).toByteString());
        assertThat(fromSecondary.getResolvedLabelsMap())
                .containsExactlyEntriesOf(java.util.Map.of("sec", 2));
        assertThat(fromSecondary.getExplicitEmptyCelMap()).isEmpty();
        assertThat(fromSecondary.getExplicitEmptyLiteralMap()).isEmpty();
        assertThat(fromSecondary.getCelUnsignedMapMap())
                .containsEntry(HIGH_UINT32, "high")
                .containsEntry(7, "low");
        assertThat(fromSecondary.getCelDynamicValuesMap().get("present").getStringValue())
                .isEqualTo("ok");
        assertThat(fromSecondary.getCelDynamicValuesMap().get("missing").getKindCase())
                .isEqualTo(Value.KindCase.NULL_VALUE);
        assertThat(fromSecondary.getCelDynamicValuesMap().get("missing").getNullValue())
                .isEqualTo(NullValue.NULL_VALUE);
        assertThat(fromSecondary.getLiteralIntKeysMap())
                .containsEntry(10, "ten")
                .containsEntry(20, "twenty");

        // Case 2: both primary_labels and secondary_labels are empty -> default_from literal fires.
        MapFallbackAndCelView fromDefault = MapFallbackAndCelView.parseFrom(
                proj.project(MapZoo.getDefaultInstance()).toByteString());
        assertThat(fromDefault.getResolvedLabelsMap())
                .containsExactlyEntriesOf(java.util.Map.of("default", 99));
    }

    @Test
    void emitsValidSourceAndTargetMasksForMapProjectionsAndStructSubpaths() {
        MessageProjection childMaskProj = projection(MapChildMaskView.getDescriptor());
        assertThat(childMaskProj.targetMask().getPathsList())
                .containsExactly("renditions", "author");
        assertThat(FieldMaskUtil.isValid(MapChildMaskView.getDescriptor(), childMaskProj.targetMask()))
                .isTrue();

        MessageProjection.SourceMask sourceMask = childMaskProj.sourceMask(MapZoo.getDescriptor());
        // blobs_by_key stays whole even though Rendition's child rules use CEL,
        // and metadata.author truncates to metadata so the FieldMask is valid.
        assertThat(sourceMask.fieldMask().getPathsList())
                .containsExactly("blobs_by_key", "metadata");
        assertThat(sourceMask.complete()).isTrue();
        assertThat(FieldMaskUtil.isValid(MapZoo.getDescriptor(), sourceMask.fieldMask()))
                .isTrue();

        // Top-level CEL map rules mark sourceMask incomplete so callers do not prune reads.
        MessageProjection.SourceMask celMask =
                projection(MapFallbackAndCelView.getDescriptor()).sourceMask(MapZoo.getDescriptor());
        assertThat(celMask.fieldMask().getPathsList())
                .containsExactly("primary_labels", "secondary_labels");
        assertThat(celMask.complete()).isFalse();
        assertThat(FieldMaskUtil.isValid(MapZoo.getDescriptor(), celMask.fieldMask()))
                .isTrue();
    }

    @Test
    void validatesBrokenChildCelOnMapValueTargetAtCompileTime() {
        assertThatThrownBy(() -> MessageProjection.forTarget(BrokenMapChildParent.getDescriptor(), ZOO_SOURCES))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("BrokenRendition.boom")
                .hasMessageContaining("compiles against no declared source");
    }

    @Test
    void refusesDuplicateMapKeysProducedByKeyCoercion() {
        MapZoo colliding = MapZoo.newBuilder()
                .putTextMap("1", "10")
                .putTextMap("01", "20")
                .build();
        assertThatThrownBy(() -> projection(DuplicateCoercedKeyDoc.getDescriptor()).project(colliding))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("Duplicate coerced map key")
                .hasMessageContaining("DuplicateCoercedKeyDoc.by_int");
    }

    @Test
    void refusesNullValuesOnNonValueMapTargets() {
        assertThatThrownBy(() -> projection(NullMapValueDoc.getDescriptor()).project(MapZoo.getDefaultInstance()))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("Null value for map projection field")
                .hasMessageContaining("NullMapValueDoc.bad");
    }

    @Test
    void refusesUndeclaredMessageTypesInMapValues() {
        MapZoo withNotes = MapZoo.newBuilder()
                .putNotesByKey("n1", Note.newBuilder().setText("oops").build())
                .build();
        assertThatThrownBy(() -> projection(UndeclaredMapChildDoc.getDescriptor()).project(withNotes))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("ai.protomolt.proto.projection.test.v1.Note")
                .hasMessageContaining("UndeclaredMapChildDoc.renditions")
                .hasMessageContaining("declared: ai.protomolt.proto.projection.test.v1.Blob");
    }

    @Test
    void refusesShapeMismatchesBetweenMapsListsSingularsStructAndBadLiterals() {
        MapZoo source = MapZoo.newBuilder()
                .addRawScores(10)
                .putByI32(1, "one")
                .setMetadata(Struct.newBuilder()
                        .putFields("k", Value.newBuilder().setStringValue("v").build()))
                .build();

        assertThatThrownBy(() -> projection(MapFromRepeatedDoc.getDescriptor()).project(source))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("MapFromRepeatedDoc.bad")
                .hasMessageContaining("is not a map");

        assertThatThrownBy(() -> projection(RepeatedFromMapDoc.getDescriptor()).project(source))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("RepeatedFromMapDoc.bad")
                .hasMessageContaining("is not a list");

        assertThatThrownBy(() -> projection(SingularFromMapDoc.getDescriptor()).project(source))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("SingularFromMapDoc.bad")
                .hasMessageContaining("is a map");

        assertThatThrownBy(() -> projection(MapFromStructPathDoc.getDescriptor()).project(source))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("MapFromStructPathDoc.bad")
                .hasMessageContaining("is not a map");

        assertThatThrownBy(() -> projection(BadMapLiteralDoc.getDescriptor()).project(source))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("BadMapLiteralDoc.bad")
                .hasMessageContaining("struct_value");
    }

    @Test
    void refusesNegativeIntoUnsignedAndOverflowingUint64IntoSignedInt64() {
        MapZoo negative = MapZoo.newBuilder().setSignedVal(-5).build();
        assertThatThrownBy(() -> projection(NegativeIntoUnsignedDoc.getDescriptor()).project(negative))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("NegativeIntoUnsignedDoc.bad_u32");

        MapZoo overflow = MapZoo.newBuilder().setHighU64(MAX_UINT64_BITS).build();
        assertThatThrownBy(() -> projection(OverflowUint64IntoInt64Doc.getDescriptor()).project(overflow))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("OverflowUint64IntoInt64Doc.bad_i64");
    }

    @Test
    void detectsMapChildCyclesImmediatelyRegardlessOfEntryOrder() {
        MessageProjection cycleProj = MessageProjection.forTarget(
                MapCycleTarget.getDescriptor(),
                SourceResolver.of(MapCycleSource.getDescriptor())).orElseThrow();
        MapCycleSource source = MapCycleSource.newBuilder()
                .putTags("a", 1)
                .putTags("b", 2)
                .build();

        // MapCycleTarget rebuilds MapCycleSource with reversed entry order {'b': 2, 'a': 1}.
        // Order-independent map equality catches the cycle immediately instead of hitting depth 64.
        assertThatThrownBy(() -> cycleProj.project(source))
                .isInstanceOf(ProjectionException.class)
                .hasMessageContaining("MapCycleTarget re-enters itself");
    }
}
