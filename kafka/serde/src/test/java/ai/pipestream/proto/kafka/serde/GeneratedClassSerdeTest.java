package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.kafka.serde.testdata.GenOrder;
import ai.pipestream.proto.kafka.serde.testdata.LegacyEventOuterClass;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consumers get their own generated classes back, not a descriptor-keyed bag. The class name is
 * derived from the descriptor set's java options — nothing is configured — and a type with no
 * class on the classpath stays a {@link DynamicMessage}. The serializer side of the same story:
 * rules declared on a generated class's descriptor are enforced without any re-parse, because
 * the generated schema and the packaged one are the same bytes.
 */
class GeneratedClassSerdeTest {

    private static Map<String, Object> config(FileDescriptor root, String type,
                                              Map<String, Object> extra) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64, base64SetOf(root));
        config.put(ProtoMoltSerdeConfig.MESSAGE_TYPE, type);
        config.putAll(extra);
        return config;
    }

    /** The generated file and its transitive imports, as the descriptor set a build would emit. */
    private static String base64SetOf(FileDescriptor root) {
        Map<String, FileDescriptorProto> files = new LinkedHashMap<>();
        collect(root, files);
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder();
        files.values().forEach(set::addFile);
        return Base64.getEncoder().encodeToString(set.build().toByteArray());
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptorProto> out) {
        if (out.containsKey(file.getName())) {
            return;
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            collect(dependency, out);
        }
        out.put(file.getName(), file.toProto());
    }

    @Test
    void roundTripsBackToTheGeneratedClass() {
        Map<String, Object> config = config(GenOrder.getDescriptor().getFile(),
                "serde.gen.v1.GenOrder", Map.of());
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config, false);
            deserializer.configure(config, false);

            byte[] bytes = serializer.serialize("orders",
                    GenOrder.newBuilder().setId("A-100").setQuantity(3).build());
            Message back = deserializer.deserialize("orders", bytes);

            assertThat(back).isInstanceOf(GenOrder.class);
            GenOrder order = (GenOrder) back;
            assertThat(order.getId()).isEqualTo("A-100");
            assertThat(order.getQuantity()).isEqualTo(3);
        }
    }

    /** The rule lives on the generated class's own descriptor; nothing else declares it. */
    @Test
    void enforcesRulesDeclaredOnTheGeneratedClass() {
        try (var serializer = new ProtoMoltProtobufSerializer()) {
            serializer.configure(config(GenOrder.getDescriptor().getFile(),
                    "serde.gen.v1.GenOrder", Map.of()), false);
            assertThatThrownBy(() -> serializer.serialize("orders",
                    GenOrder.newBuilder().setId("A").build()))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("violates the schema's declared rules")
                    .hasMessageContaining("id");
        }
    }

    /** legacy_event.proto declares a message named after the file: protoc appends OuterClass. */
    @Test
    void derivesOuterClassNamesIncludingTheClashRule() {
        Map<String, Object> config = config(
                LegacyEventOuterClass.LegacyEvent.getDescriptor().getFile(),
                "serde.gen.v1.LegacyEvent", Map.of());
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config, false);
            deserializer.configure(config, false);

            Message back = deserializer.deserialize("legacy", serializer.serialize("legacy",
                    LegacyEventOuterClass.LegacyEvent.newBuilder().setName("n").build()));

            assertThat(back).isInstanceOf(LegacyEventOuterClass.LegacyEvent.class);
        }
    }

    @Test
    void staysDynamicWhenGeneratedClassesAreTurnedOff() {
        Map<String, Object> config = config(GenOrder.getDescriptor().getFile(),
                "serde.gen.v1.GenOrder",
                Map.of(ProtoMoltSerdeConfig.GENERATED_CLASSES, false));
        try (var serializer = new ProtoMoltProtobufSerializer();
             var deserializer = new ProtoMoltProtobufDeserializer()) {
            serializer.configure(config, false);
            deserializer.configure(config, false);

            Message back = deserializer.deserialize("orders", serializer.serialize("orders",
                    GenOrder.newBuilder().setId("A-100").build()));

            assertThat(back).isInstanceOf(DynamicMessage.class);
        }
    }
}
