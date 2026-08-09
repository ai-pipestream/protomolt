package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Descriptor sets arrive as base64 config values or classpath resources, and both lanes must
 * fail loudly when what arrives is not a descriptor set. The load-bearing detail under test:
 * the parse registers the validation extensions, so a schema's declared rules land as real
 * options rather than unknown fields the validator would never see.
 */
class SerdeDescriptorsTest {

    private static final String PROTO = """
            syntax = "proto3";
            package serde.desc.v1;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Order {
              string id = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 3];
              int32 quantity = 2;
              message Line { string sku = 1; }
            }
            """;

    private static byte[] descriptorSetBytes;

    @BeforeAll
    static void compile() throws Exception {
        String validateProto = new String(SerdeDescriptorsTest.class.getClassLoader()
                .getResourceAsStream("ai/pipestream/proto/validate/v1/validate.proto")
                .readAllBytes());
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/validate/v1/validate.proto", validateProto, "test")
                .add("serde/desc/v1/order.proto", PROTO, "test")
                .build());
        descriptorSetBytes = compiled.descriptorSet().toByteArray();
    }

    /** A classloader that serves the compiled descriptor set as a named resource. */
    private static ClassLoader serving(String resource, byte[] bytes) {
        return new ClassLoader(SerdeDescriptorsTest.class.getClassLoader()) {
            @Override
            public InputStream getResourceAsStream(String name) {
                if (resource.equals(name)) {
                    return new ByteArrayInputStream(bytes);
                }
                return super.getResourceAsStream(name);
            }
        };
    }

    @Test
    void linksABase64DescriptorSet() {
        List<FileDescriptor> files = SerdeDescriptors.fromBase64(
                Base64.getEncoder().encodeToString(descriptorSetBytes));
        assertThat(SerdeDescriptors.findMessageType(files, "serde.desc.v1.Order")).isNotNull();
    }

    /**
     * The whole point of parsing with the extension registry: the rule on {@code id} must be a
     * real option on the descriptor. Parsed without the extensions it would be an unknown field
     * and the serde would validate against a schema that appears to declare nothing.
     */
    @Test
    void keepsDeclaredRulesAsRealOptions() {
        List<FileDescriptor> files = SerdeDescriptors.fromBase64(
                Base64.getEncoder().encodeToString(descriptorSetBytes));
        Descriptor order = SerdeDescriptors.findMessageType(files, "serde.desc.v1.Order");
        assertThat(order.findFieldByName("id").getOptions().getAllFields()).isNotEmpty();
        // A field with no declared rule has no options at all.
        assertThat(order.findFieldByName("quantity").getOptions().getAllFields()).isEmpty();
    }

    @Test
    void readsADescriptorSetFromTheClasspath() {
        List<FileDescriptor> files = SerdeDescriptors.fromClasspath("sets/orders.desc",
                serving("sets/orders.desc", descriptorSetBytes));
        assertThat(SerdeDescriptors.findMessageType(files, "serde.desc.v1.Order")).isNotNull();
    }

    @Test
    void failsWhenTheClasspathResourceIsMissing() {
        assertThatThrownBy(() -> SerdeDescriptors.fromClasspath("no/such.desc",
                getClass().getClassLoader()))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("not found on the classpath")
                .hasMessageContaining("no/such.desc");
    }

    @Test
    void failsWhenTheBase64IsNotBase64() {
        assertThatThrownBy(() -> SerdeDescriptors.fromBase64("!!!not-base64!!!"))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("not valid base64");
    }

    @Test
    void failsWhenTheBytesAreNotADescriptorSet() {
        // A length-delimited field claiming 127 bytes with only one present: protobuf refuses.
        byte[] truncated = {0x0A, 0x7F, 0x01};
        assertThatThrownBy(() -> SerdeDescriptors.fromBase64(
                Base64.getEncoder().encodeToString(truncated)))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("not a serialized FileDescriptorSet");
    }

    @Test
    void failsWhenAnImportedFileIsMissingFromTheSet() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("a.proto")
                        .setPackage("a")
                        .setSyntax("proto3")
                        .addDependency("b.proto"))
                .build();
        assertThatThrownBy(() -> SerdeDescriptors.fromBase64(
                Base64.getEncoder().encodeToString(set.toByteArray())))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("missing an imported file: b.proto");
    }

    @Test
    void resolvesNestedTypesByDottedName() {
        List<FileDescriptor> files = SerdeDescriptors.fromBase64(
                Base64.getEncoder().encodeToString(descriptorSetBytes));
        Descriptor line = SerdeDescriptors.findMessageType(files, "serde.desc.v1.Order.Line");
        assertThat(line).isNotNull();
        assertThat(line.getContainingType().getFullName()).isEqualTo("serde.desc.v1.Order");
    }

    @Test
    void findsNothingInTheWrongPackageOrForAbsentTypes() {
        List<FileDescriptor> files = SerdeDescriptors.fromBase64(
                Base64.getEncoder().encodeToString(descriptorSetBytes));
        assertThat(SerdeDescriptors.findMessageType(files, "other.pkg.Order")).isNull();
        assertThat(SerdeDescriptors.findMessageType(files, "serde.desc.v1.Missing")).isNull();
        // A package prefix alone is not a message.
        assertThat(SerdeDescriptors.findMessageType(files, "serde.desc.v1")).isNull();
    }

    /** The throwing half: callers without a fallback hear about absent types. */
    @Test
    void messageTypeThrowsForAbsentTypes() {
        List<FileDescriptor> files = SerdeDescriptors.fromBase64(
                Base64.getEncoder().encodeToString(descriptorSetBytes));
        assertThatThrownBy(() -> SerdeDescriptors.messageType(files, "serde.desc.v1.Missing"))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("'serde.desc.v1.Missing'")
                .hasMessageContaining("not in the configured descriptor set");
    }
}
