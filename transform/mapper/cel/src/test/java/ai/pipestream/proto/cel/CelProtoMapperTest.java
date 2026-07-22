package ai.pipestream.proto.cel;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CelProtoMapperTest {
    @Test
    void appliesSelectorOnlyWhenFilterMatches() throws Exception {
        Descriptor descriptor = testFile().findMessageTypeByName("Document");
        Message.Builder target = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "source");
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(descriptor).addVar("input").build());
        CelProtoMapper mapper = new CelProtoMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()), evaluator);

        mapper.map(target, List.of(
                new CelMappingRule("input.title == 'source'", "input.title + '-mapped'", "output"),
                new CelMappingRule("input.title == 'other'", "'ignored'", "output")));

        assertEquals("source-mapped", target.build().getField(descriptor.findFieldByName("output")));
    }

    @Test
    void mapsProto2BuilderWithUnsetRequiredFields() throws Exception {
        // proto2 (no syntax set) message with a required field that stays unset during mapping;
        // bindings must use buildPartial() or DynamicMessage.build() throws
        // UninitializedMessageException.
        DescriptorProtos.DescriptorProto document = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("LegacyDocument")
                .addField(field("id", 1).toBuilder()
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED))
                .addField(field("title", 2).toBuilder()
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(field("output", 3).toBuilder()
                        .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .build();
        Descriptor descriptor = FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("cel_proto2.proto").setPackage("ai.pipestream.test.legacy")
                        .addMessageType(document).build(),
                new FileDescriptor[]{}).findMessageTypeByName("LegacyDocument");

        Message.Builder target = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("title"), "source");
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageType(descriptor).addVar("input").build());
        CelProtoMapper mapper = new CelProtoMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()), evaluator);

        mapper.map(target, List.of(
                new CelMappingRule("input.title == 'source'", "input.title + '-mapped'", "output")));

        assertEquals("source-mapped",
                target.buildPartial().getField(descriptor.findFieldByName("output")));
    }

    private static FileDescriptor testFile() throws Exception {
        DescriptorProtos.DescriptorProto document = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("Document")
                .addField(field("title", 1))
                .addField(field("output", 2))
                .build();
        return FileDescriptor.buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("cel.proto").setPackage("ai.pipestream.test").addMessageType(document).build(),
                new FileDescriptor[]{});
    }

    private static DescriptorProtos.FieldDescriptorProto field(String name, int number) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING).build();
    }
}
