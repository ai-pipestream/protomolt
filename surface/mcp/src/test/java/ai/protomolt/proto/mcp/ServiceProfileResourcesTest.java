package ai.protomolt.proto.mcp;

import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.profile.v1.DescriptorArtifact;
import ai.protomolt.proto.grpc.profile.v1.SchemaSource;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.SourceKind;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import io.grpc.health.v1.HealthProto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ServiceProfileResourcesTest {

    @TempDir
    Path directory;

    @Test
    void listsAndReadsProfilesAndIndividualMethodContracts() throws Exception {
        FileSystemServiceProfileRepository repository =
                new FileSystemServiceProfileRepository(directory);
        byte[] descriptors = FileDescriptorSet.newBuilder()
                .addFile(HealthProto.getDescriptor().toProto())
                .build().toByteArray();
        String fingerprint = ServiceProfileValidation.sha256(descriptors);
        repository.saveDescriptorArtifact(DescriptorArtifact.newBuilder()
                .setFingerprint(fingerprint)
                .setDescriptorSet(ByteString.copyFrom(descriptors))
                .build());
        repository.save(ServiceProfile.newBuilder()
                .setName("health-local")
                .setDescription("Local health service")
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("local")
                        .setHost("localhost")
                        .setPort(50051)
                        .setTransport(Transport.TRANSPORT_PLAINTEXT))
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(SourceKind.SOURCE_KIND_REFLECTION)
                        .setSourceRef("grpc-reflection:local")
                        .setDescriptorFingerprint(fingerprint)
                        .setDescriptorArtifactRef("sha256:" + fingerprint))
                .build());

        ObjectMapper mapper = new ObjectMapper();
        ServiceProfileResources resources = new ServiceProfileResources(repository);
        String method = "grpc.health.v1.Health/Check";
        String methodUri = "protomolt://services/health-local/methods/"
                + URLEncoder.encode(method, StandardCharsets.UTF_8);

        assertThat(resources.list(mapper).findValuesAsText("uri"))
                .containsExactly("protomolt://services", "protomolt://services/health-local");
        assertThat(resources.templates(mapper).findValuesAsText("uriTemplate"))
                .containsExactly("protomolt://services/{profile}",
                        "protomolt://services/{profile}/methods/{fullMethod}");

        var service = resources.read(mapper, "protomolt://services/health-local").orElseThrow();
        assertThat(service.path("text").asText())
                .contains("health-local", method)
                .doesNotContain("descriptorSetBase64");

        var contract = resources.read(mapper, methodUri).orElseThrow();
        assertThat(contract.path("text").asText())
                .contains(method, "grpc.health.v1.HealthCheckRequest", "service");
    }
}
