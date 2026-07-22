package ai.pipestream.proto.kafka.connect;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The config diagnostics every connector and transform inherits from
 * {@link GrpcConnectorSupport}: a descriptor set that is not base64, one whose imports are
 * absent, one that parses but does not link, and a malformed {@code grpc.method} value.
 * Each has to name the offending config key so an operator can act on the worker log alone.
 */
class GrpcConnectorSupportTest {

    private static String base64(FileDescriptorSet set) {
        return Base64.getEncoder().encodeToString(set.toByteArray());
    }

    @Test
    void descriptorSetThatIsNotBase64FileDescriptorSetNamesTheConfigKey() {
        assertThatThrownBy(() -> GrpcConnectorSupport.linkedFiles("!!! not base64 !!!"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'schema.descriptor.set.base64' is not a base64 "
                        + "serialized FileDescriptorSet");

        String base64Garbage = Base64.getEncoder()
                .encodeToString("this is not a descriptor set".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> GrpcConnectorSupport.linkedFiles(base64Garbage))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'schema.descriptor.set.base64' is not a base64 "
                        + "serialized FileDescriptorSet");
    }

    /**
     * A descriptor set exported without its transitive imports links to nothing useful; the
     * failure has to name the missing file rather than surface as a NullPointerException.
     */
    @Test
    void descriptorSetMissingAnImportNamesTheMissingFile() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("support/test/a.proto")
                        .setSyntax("proto3")
                        .setPackage("support.test")
                        .addDependency("support/test/missing.proto"))
                .build();

        assertThatThrownBy(() -> GrpcConnectorSupport.linkedFiles(base64(set)))
                .isInstanceOf(ConnectException.class)
                .hasMessage("Descriptor set is missing the import 'support/test/missing.proto'");
    }

    @Test
    void descriptorSetThatDoesNotLinkReportsTheValidationFailure() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("support/test/b.proto")
                        .setSyntax("proto3")
                        .setPackage("support.test")
                        .addMessageType(DescriptorProto.newBuilder()
                                .setName("Holder")
                                .addField(FieldDescriptorProto.newBuilder()
                                        .setName("body")
                                        .setNumber(1)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                        .setTypeName(".support.test.NoSuchType"))))
                .build();

        assertThatThrownBy(() -> GrpcConnectorSupport.linkedFiles(base64(set)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Descriptor set does not link")
                .hasMessageContaining(".support.test.NoSuchType");
    }

    @Test
    void grpcMethodWithoutAServiceAndMethodHalfIsRejected() {
        String noSlash = "sink.test.CollectorRecord";
        assertThatThrownBy(() -> GrpcConnectorSupport.resolveMethod("ignored", noSlash))
                .isInstanceOf(ConnectException.class)
                .hasMessage("'grpc.method' must be 'package.Service/Method'; got '"
                        + noSlash + "'");

        assertThatThrownBy(() -> GrpcConnectorSupport.resolveMethod("ignored", "/Record"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'grpc.method' must be 'package.Service/Method'");

        assertThatThrownBy(() ->
                GrpcConnectorSupport.resolveMethod("ignored", "sink.test.Collector/"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'grpc.method' must be 'package.Service/Method'");
    }
}
