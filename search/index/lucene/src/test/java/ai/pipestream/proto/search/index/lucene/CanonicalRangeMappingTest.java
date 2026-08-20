package ai.pipestream.proto.search.index.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.search.index.spi.DateResolution;
import ai.pipestream.proto.search.index.spi.IndexFieldKind;
import ai.pipestream.proto.search.index.spi.IndexMapping;
import ai.pipestream.proto.search.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.types.DateRange;
import ai.pipestream.proto.types.DoubleRange;
import ai.pipestream.proto.types.LongRange;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.lucene.document.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Canonical types.v1 ranges through the Lucene mapper: Lucene range
 * fields are inclusive on both ends, so an excluded bound moves one step
 * inward, an unset bound becomes the open-end sentinel, and the
 * day-grain DateRange covers whole days — an excluded end drops the
 * entire day, not its first millisecond.
 */
class CanonicalRangeMappingTest {

    private final ProtoLuceneMapper mapper =
            new ProtoLuceneMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void aLongRangeEmitsItsBoundsInclusively() throws Exception {
        Document doc = map(LongRange.newBuilder().setBegin(10).setEnd(20).build(),
                IndexFieldKind.LONG_RANGE, DateResolution.MILLIS);
        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("window")[0];
        assertThat(range.getMin(0)).isEqualTo(10L);
        assertThat(range.getMax(0)).isEqualTo(20L);
    }

    @Test
    void excludedLongBoundsMoveOneStepInward() throws Exception {
        Document doc = map(LongRange.newBuilder()
                        .setBegin(10).setEnd(20)
                        .setIncludeHead(false).setIncludeTail(false).build(),
                IndexFieldKind.LONG_RANGE, DateResolution.MILLIS);
        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("window")[0];
        assertThat(range.getMin(0)).isEqualTo(11L);
        assertThat(range.getMax(0)).isEqualTo(19L);
    }

    @Test
    void anUnsetBoundIsAnOpenEnd() throws Exception {
        Document doc = map(LongRange.newBuilder().setBegin(10).build(),
                IndexFieldKind.LONG_RANGE, DateResolution.MILLIS);
        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("window")[0];
        assertThat(range.getMin(0)).isEqualTo(10L);
        assertThat(range.getMax(0)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void excludedDoubleBoundsMoveByOneUlp() throws Exception {
        Document doc = map(DoubleRange.newBuilder()
                        .setBegin(1.5).setEnd(2.5).setIncludeHead(false).build(),
                IndexFieldKind.DOUBLE_RANGE, DateResolution.MILLIS);
        org.apache.lucene.document.DoubleRange range =
                (org.apache.lucene.document.DoubleRange) doc.getFields("window")[0];
        assertThat(range.getMin(0)).isEqualTo(Math.nextUp(1.5));
        assertThat(range.getMax(0)).isEqualTo(2.5);
    }

    @Test
    void aDateRangeCoversWholeDaysInMillis() throws Exception {
        Document doc = map(DateRange.newBuilder()
                        .setBegin("1970-01-02").setEnd("1970-01-03").build(),
                IndexFieldKind.DATE_RANGE, DateResolution.MILLIS);
        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("window")[0];
        assertThat(range.getMin(0)).isEqualTo(86_400_000L);
        assertThat(range.getMax(0)).isEqualTo(259_199_999L);
    }

    @Test
    void anExcludedEndDayDropsTheWholeDay() throws Exception {
        Document doc = map(DateRange.newBuilder()
                        .setBegin("1970-01-02").setEnd("1970-01-03")
                        .setIncludeTail(false).build(),
                IndexFieldKind.DATE_RANGE, DateResolution.MILLIS);
        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("window")[0];
        assertThat(range.getMax(0)).isEqualTo(172_799_999L);
    }

    @Test
    void secondsResolutionNarrowsWithoutCoveringExcludedInstants() throws Exception {
        Document doc = map(DateRange.newBuilder()
                        .setBegin("1970-01-02").setEnd("1970-01-03").build(),
                IndexFieldKind.DATE_RANGE, DateResolution.SECONDS);
        org.apache.lucene.document.LongRange range =
                (org.apache.lucene.document.LongRange) doc.getFields("window")[0];
        assertThat(range.getMin(0)).isEqualTo(86_400L);
        assertThat(range.getMax(0)).isEqualTo(259_199L);
    }

    private Document map(Message range, IndexFieldKind kind, DateResolution resolution)
            throws Exception {
        Descriptor doc = docDescriptor(range.getDescriptorForType());
        DynamicMessage message = DynamicMessage.newBuilder(doc)
                .setField(doc.findFieldByName("window"), range)
                .build();
        IndexMapping mapping = new IndexMapping(doc.getFullName(), List.of(
                new IndexMapping.IndexedField("window", "window",
                        ResolvedFieldHint.builder(kind).dateResolution(resolution).build())));
        return mapper.map(message, mapping);
    }

    private static Descriptor docDescriptor(Descriptor rangeType) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("canonical_" + rangeType.getName().toLowerCase() + "_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addDependency(rangeType.getFile().getName())
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("window")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName("." + rangeType.getFullName())
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[]{rangeType.getFile()})
                .findMessageTypeByName("Doc");
    }
}
