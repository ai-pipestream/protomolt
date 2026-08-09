package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The positive resolution paths through {@link GrpcConnectorSupport} — a valid descriptor set
 * links, top-level and nested message types resolve, and a fully qualified method resolves —
 * plus the lookup failures that must name what was not found. The malformed-input diagnostics
 * live in {@link GrpcConnectorSupportTest}.
 */
class GrpcConnectorSupportResolveTest {

    private static final String PROTO = """
            syntax = "proto3";
            package support.test;
            message Event { int64 seq = 1; }
            message Envelope { message Body { string id = 1; } Body body = 1; }
            message Ack { int64 count = 1; }
            service Collector { rpc Record(Event) returns (Ack); }
            """;

    private static String descriptorSetBase64;
    private static List<FileDescriptor> files;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("support/test/svc.proto", PROTO, "test").build());
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        files = GrpcConnectorSupport.linkedFiles(descriptorSetBase64);
    }

    @Test
    void aValidDescriptorSetLinks() {
        assertThat(files).hasSize(1);
        assertThat(files.get(0).getPackage()).isEqualTo("support.test");
    }

    @Test
    void topLevelAndNestedMessageTypesResolve() {
        assertThat(GrpcConnectorSupport.messageType(files, "support.test.Event").getName())
                .isEqualTo("Event");
        assertThat(GrpcConnectorSupport.messageType(files, "support.test.Envelope.Body")
                .getFullName()).isEqualTo("support.test.Envelope.Body");
    }

    @Test
    void anUnknownMessageTypeIsNamed() {
        assertThatThrownBy(() -> GrpcConnectorSupport.messageType(files, "support.test.NoSuch"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("support.test.NoSuch");
    }

    /**
     * A type name under a package no linked file declares can never resolve; the lookup
     * short-circuits on the package prefix rather than scanning every file.
     */
    @Test
    void aTypeUnderAnUnknownPackageIsNotFound() {
        assertThatThrownBy(() -> GrpcConnectorSupport.messageType(files, "other.place.Event"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("other.place.Event");
    }

    @Test
    void aFullyQualifiedMethodResolves() {
        MethodDescriptor method = GrpcConnectorSupport.resolveMethod(
                descriptorSetBase64, "support.test.Collector/Record");
        assertThat(method.getName()).isEqualTo("Record");
        assertThat(method.getService().getFullName()).isEqualTo("support.test.Collector");
        assertThat(method.getInputType().getFullName()).isEqualTo("support.test.Event");
    }

    /**
     * The service half must be fully qualified: a bare "Collector" matches the simple name but
     * not the full one, so it is rejected rather than silently bound to a same-named service in
     * another package.
     */
    @Test
    void anUnqualifiedServiceNameIsRejected() {
        assertThatThrownBy(() -> GrpcConnectorSupport.resolveMethod(
                descriptorSetBase64, "Collector/Record"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Collector/Record")
                .hasMessageContaining("not found");
    }

    @Test
    void anUnknownMethodIsNamed() {
        assertThatThrownBy(() -> GrpcConnectorSupport.resolveMethod(
                descriptorSetBase64, "support.test.Collector/Nope"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("support.test.Collector/Nope")
                .hasMessageContaining("not found");
    }
}
