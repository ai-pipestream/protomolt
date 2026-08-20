package ai.pipestream.proto.search.index.opensearch;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.search.index.spi.IndexFieldKind;
import ai.pipestream.proto.search.index.spi.IndexMapping;
import ai.pipestream.proto.search.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.types.DateRange;
import ai.pipestream.proto.types.LongRange;
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
 * Canonical types.v1 ranges through the OpenSearch mapper: range objects
 * speak inclusivity natively (gte/gt, lte/lt), an unset bound has no
 * key, and the day-grain DateRange lands as epoch millis covering whole
 * days — an excluded bound day is dropped entirely.
 */
class CanonicalRangeMappingTest {

    private final OpenSearchDocumentMapper mapper =
            new OpenSearchDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));

    @Test
    void includedBoundsUseGteLteAndExcludedUseGtLt() throws Exception {
        Map<String, Object> doc = map(LongRange.newBuilder()
                .setBegin(10).setEnd(20).setIncludeTail(false).build(),
                IndexFieldKind.LONG_RANGE);
        assertThat(doc.get("window")).isEqualTo(Map.of("gte", 10L, "lt", 20L));
    }

    @Test
    void anUnsetBoundHasNoKey() throws Exception {
        Map<String, Object> doc = map(LongRange.newBuilder().setEnd(20).build(),
                IndexFieldKind.LONG_RANGE);
        assertThat(doc.get("window")).isEqualTo(Map.of("lte", 20L));
    }

    @Test
    void aDateRangeCoversWholeDaysInEpochMillis() throws Exception {
        Map<String, Object> doc = map(DateRange.newBuilder()
                .setBegin("1970-01-02").setEnd("1970-01-03").build(),
                IndexFieldKind.DATE_RANGE);
        assertThat(doc.get("window"))
                .isEqualTo(Map.of("gte", 86_400_000L, "lte", 259_199_999L));
    }

    @Test
    void excludedBoundDaysAreDroppedEntirely() throws Exception {
        Map<String, Object> doc = map(DateRange.newBuilder()
                .setBegin("1970-01-02").setEnd("1970-01-04")
                .setIncludeHead(false).setIncludeTail(false).build(),
                IndexFieldKind.DATE_RANGE);
        // Excluded begin skips the whole first day; excluded end stops before its day.
        assertThat(doc.get("window"))
                .isEqualTo(Map.of("gte", 172_800_000L, "lt", 259_200_000L));
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
