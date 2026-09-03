package ai.protomolt.proto.search.index.solr;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.types.DateRange;
import ai.protomolt.proto.types.DoubleRange;
import ai.protomolt.proto.types.LongRange;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Canonical types.v1 ranges through the Solr mapper: two flat point
 * fields cannot express exclusivity, so an excluded bound is normalized
 * to its inclusive equivalent, an unset bound omits its subfield, and
 * day-grain DateRange bounds become ISO instants covering whole days.
 */
class CanonicalRangeMappingTest {

    private final SolrDocumentMapper mapper =
            new SolrDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void excludedDiscreteBoundsNormalizeOneStepInward() throws Exception {
        Map<String, Object> doc = map(LongRange.newBuilder()
                .setBegin(10).setEnd(20).setIncludeHead(false).setIncludeTail(false).build(),
                IndexFieldKind.LONG_RANGE);
        assertThat(doc).containsEntry("window_min", 11L).containsEntry("window_max", 19L);
    }

    @Test
    void anUnsetBoundOmitsItsSubfield() throws Exception {
        Map<String, Object> doc = map(LongRange.newBuilder().setBegin(10).build(),
                IndexFieldKind.LONG_RANGE);
        assertThat(doc).containsEntry("window_min", 10L).doesNotContainKey("window_max");
    }

    @Test
    void excludedDoubleBoundsNormalizeByOneUlp() throws Exception {
        Map<String, Object> doc = map(DoubleRange.newBuilder()
                .setBegin(1.5).setEnd(2.5).setIncludeTail(false).build(),
                IndexFieldKind.DOUBLE_RANGE);
        assertThat(doc).containsEntry("window_min", 1.5)
                .containsEntry("window_max", Math.nextDown(2.5));
    }

    @Test
    void dayGrainBoundsBecomeIsoInstantsCoveringWholeDays() throws Exception {
        Map<String, Object> doc = map(DateRange.newBuilder()
                .setBegin("1970-01-02").setEnd("1970-01-03").build(),
                IndexFieldKind.DATE_RANGE);
        assertThat(doc).containsEntry("window_min", "1970-01-02T00:00:00Z")
                .containsEntry("window_max", "1970-01-03T23:59:59.999Z");
    }

    @Test
    void anExcludedEndDayStopsBeforeTheDayBegins() throws Exception {
        Map<String, Object> doc = map(DateRange.newBuilder()
                .setBegin("1970-01-02").setEnd("1970-01-03")
                .setIncludeTail(false).build(),
                IndexFieldKind.DATE_RANGE);
        assertThat(doc).containsEntry("window_max", "1970-01-02T23:59:59.999Z");
    }

    private Map<String, Object> map(Message range, IndexFieldKind kind) throws Exception {
        Descriptor doc = docDescriptor(range.getDescriptorForType());
        DynamicMessage message = DynamicMessage.newBuilder(doc)
                .setField(doc.findFieldByName("window"), range)
                .build();
        IndexMapping mapping = new IndexMapping(doc.getFullName(), List.of(
                new IndexMapping.IndexedField("window", "window", ResolvedFieldHint.of(kind))));
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
