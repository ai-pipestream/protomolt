package ai.pipestream.proto.grpc.profile;

import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.HealthProbe;
import ai.pipestream.proto.grpc.profile.v1.MethodPolicy;
import ai.pipestream.proto.grpc.profile.v1.Operation;
import ai.pipestream.proto.grpc.profile.v1.SchemaSource;
import ai.pipestream.proto.grpc.profile.v1.ServiceEndpoint;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.grpc.profile.v1.SourceKind;
import ai.pipestream.proto.grpc.profile.v1.Transport;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Duration;

final class TestProfiles {

    private TestProfiles() {
    }

    static ServiceProfile profile(String name) {
        return ServiceProfile.newBuilder()
                .setName(name)
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("local")
                        .setHost("127.0.0.1")
                        .setPort(9090)
                        .setTransport(Transport.TRANSPORT_PLAINTEXT)
                        .setCredentialRef("credential://dev/service")
                        .build())
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_ARTIFACT)
                        .setSourceRef("registry://descriptors/example")
                        .setDescriptorFingerprint("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
                        .setDescriptorArtifactRef("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
                        .build())
                .setHealthProbe(HealthProbe.newBuilder()
                        .setMethod("grpc.health.v1.Health/Check")
                        .setService("example.v1.ExampleService")
                        .setTimeout(Duration.newBuilder().setSeconds(2).build())
                        .setEnabled(true)
                        .build())
                .addMethodPolicies(MethodPolicy.newBuilder()
                        .setMethod("example.v1.ExampleService/Get")
                        .addOperation(Operation.OPERATION_READ_ONLY)
                        .addOperation(Operation.OPERATION_IDEMPOTENT)
                        .setDeadline(Duration.newBuilder().setSeconds(5).build())
                        .setMaxAttempts(2)
                        .build())
                .addMethodPolicies(MethodPolicy.newBuilder()
                        .setMethod("example.v1.ExampleService/Update")
                        .addOperation(Operation.OPERATION_MUTATING)
                        .addOperation(Operation.OPERATION_APPROVAL_REQUIRED)
                        .setDeadline(Duration.newBuilder().setSeconds(10).build())
                        .setApprovalRequired(true)
                        .build())
                .setDescription("Example service")
                .build();
    }

    static DescriptorArtifact artifact() {
        byte[] descriptorSet = FileDescriptorSet.newBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("example/v1/example.proto")
                        .setPackage("example.v1")
                        .setSyntax("proto3")
                        .addMessageType(DescriptorProto.newBuilder().setName("Request"))
                        .build())
                .build()
                .toByteArray();
        return DescriptorArtifact.newBuilder()
                .setFingerprint(ServiceProfileValidation.sha256(descriptorSet))
                .setDescriptorSet(com.google.protobuf.ByteString.copyFrom(descriptorSet))
                .build();
    }
}
