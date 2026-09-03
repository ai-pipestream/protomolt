package ai.protomolt.proto.search.index.spi;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.types.DateRange;
import ai.protomolt.proto.types.DoubleRange;
import ai.protomolt.proto.types.LongRange;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

/**
 * The canonical types.v1 ranges resolve by name — begin/end bounds with
 * per-end presence and inclusivity — while duck-typed (gte,lte)/(min,max)
 * messages keep their always-present, always-included behavior, and a
 * field of a canonical type infers its range kind with no hint at all.
 */
class RangeBoundsTest {

    @Test
    void canonicalTypesResolveByNameForTheirKinds() {
        assertThat(RangeBounds.resolve(LongRange.getDescriptor(), IndexFieldKind.LONG_RANGE))
                .isPresent();
        // Epoch bounds: the long range also serves DATE_RANGE.
        assertThat(RangeBounds.resolve(LongRange.getDescriptor(), IndexFieldKind.DATE_RANGE))
                .isPresent();
        assertThat(RangeBounds.resolve(LongRange.getDescriptor(), IndexFieldKind.DOUBLE_RANGE))
                .isEmpty();
        assertThat(RangeBounds.resolve(DoubleRange.getDescriptor(), IndexFieldKind.DOUBLE_RANGE))
                .isPresent();
        assertThat(RangeBounds.resolve(DoubleRange.getDescriptor(), IndexFieldKind.LONG_RANGE))
                .isEmpty();
        assertThat(RangeBounds.resolve(DateRange.getDescriptor(), IndexFieldKind.DATE_RANGE))
                .isPresent();
        assertThat(RangeBounds.resolve(DateRange.getDescriptor(), IndexFieldKind.LONG_RANGE))
                .isEmpty();
    }

    @Test
    void onlyTheCanonicalDateRangeIsDayGrain() {
        assertThat(RangeBounds.resolve(DateRange.getDescriptor(), IndexFieldKind.DATE_RANGE)
                .orElseThrow().dayGrain()).isTrue();
        assertThat(RangeBounds.resolve(LongRange.getDescriptor(), IndexFieldKind.DATE_RANGE)
                .orElseThrow().dayGrain()).isFalse();
    }

    @Test
    void canonicalBoundsTrackPresenceAndAbsentFlagsMeanIncluded() {
        RangeBounds bounds = RangeBounds.resolve(
                LongRange.getDescriptor(), IndexFieldKind.LONG_RANGE).orElseThrow();

        LongRange headOnly = LongRange.newBuilder().setBegin(5).build();
        assertThat(bounds.hasLower(headOnly)).isTrue();
        assertThat(bounds.hasUpper(headOnly)).isFalse();
        assertThat(bounds.lowerIncluded(headOnly)).isTrue();
        assertThat(bounds.upperIncluded(headOnly)).isTrue();

        LongRange exclusiveHead = LongRange.newBuilder()
                .setBegin(5).setEnd(9).setIncludeHead(false).setIncludeTail(true).build();
        assertThat(bounds.lowerIncluded(exclusiveHead)).isFalse();
        assertThat(bounds.upperIncluded(exclusiveHead)).isTrue();
    }

    @Test
    void duckTypedPairsKeepTheirAlwaysPresentAlwaysIncludedContract() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("duck_bounds.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Bounds")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("gte").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("lte").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        var descriptor = FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Bounds");

        RangeBounds bounds = RangeBounds.resolve(descriptor, IndexFieldKind.LONG_RANGE)
                .orElseThrow();
        var empty = com.google.protobuf.DynamicMessage.getDefaultInstance(descriptor);
        assertThat(bounds.dayGrain()).isFalse();
        assertThat(bounds.hasLower(empty)).isTrue();
        assertThat(bounds.hasUpper(empty)).isTrue();
        assertThat(bounds.lowerIncluded(empty)).isTrue();
        assertThat(bounds.upperIncluded(empty)).isTrue();
    }

    @Test
    void aFieldOfACanonicalTypeInfersItsRangeKindWithNoHint() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("canonical_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency("ai/protomolt/proto/types/v1/ranges.proto")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("stay").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.DateRange")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("bytes_window").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.LongRange")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        var doc = FileDescriptor.buildFrom(
                        file, new FileDescriptor[]{DateRange.getDescriptor().getFile()})
                .findMessageTypeByName("Doc");

        assertThat(InferringIndexingHintSource.infer(doc.findFieldByName("stay")).type())
                .isEqualTo(IndexFieldKind.DATE_RANGE);
        assertThat(InferringIndexingHintSource.infer(doc.findFieldByName("bytes_window")).type())
                .isEqualTo(IndexFieldKind.LONG_RANGE);
    }

    @Test
    void dayArithmeticIsUtcAndMillisecondExact() {
        assertThat(RangeBounds.dayFirstMillis("1970-01-02")).isEqualTo(86_400_000L);
        assertThat(RangeBounds.DAY_MILLIS).isEqualTo(86_400_000L);
    }
}
