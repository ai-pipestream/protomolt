package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.RuntimeVersion;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BuiltinDocumentSchemaTest {
    @Test void retainsCompleteGeneratedDefinitionAndProducerCompilerIdentity() throws Exception {
        var definition=BuiltinDocumentSchema.definition();
        assertThat(BuiltinDocumentSchema.definition()).isSameAs(definition);
        var metadata=definition.metadata();
        assertThat(ProtoValidator.create().validate(metadata).valid()).isTrue();
        var closure=DescriptorProtos.FileDescriptorSet.parseFrom(definition.descriptors());
        var generated=DescriptorFingerprints.closure(Document.getDescriptor());
        assertThat(definition.descriptors()).isEqualTo(generated.toByteString());
        assertThat(closure.getFileList()).extracting(DescriptorProtos.FileDescriptorProto::getName)
                .containsExactlyElementsOf(generated.getFileList().stream().map(DescriptorProtos.FileDescriptorProto::getName).toList());
        assertThat(metadata.getArtifactSha256()).isEqualTo(DocumentPartCodec.sha256Hex(definition.descriptors().toByteArray()));
        assertThat(metadata.getSchema().getDescriptorFingerprint()).isEqualTo(DescriptorFingerprints.fingerprint(generated));
        assertThat(metadata.getSchema().getTypeName()).isEqualTo(Document.getDescriptor().getFullName());
        assertThat(definition.source()).isEmpty();
        var provenance=metadata.getCompilation();
        assertThat(provenance.getOrigin()).isEqualTo(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR);
        assertThat(provenance.getEvidence()).isEqualTo(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_PRODUCER_REPORTED);
        var properties=new Properties();
        try (var stream=Document.class.getResourceAsStream("/META-INF/protomolt/repo-proto/compiler.properties")) {
            assertThat(stream).isNotNull(); properties.load(stream);
        }
        assertThat(provenance.getKnownCompiler().getTool().getName()).isEqualTo(properties.getProperty("compiler.name"));
        assertThat(provenance.getKnownCompiler().getTool().getVersion()).isEqualTo(properties.getProperty("compiler.version"));
        String loaded=RuntimeVersion.class.getField("MAJOR").get(null)+"."
                +RuntimeVersion.class.getField("MINOR").get(null)+"."
                +RuntimeVersion.class.getField("PATCH").get(null)+RuntimeVersion.class.getField("SUFFIX").get(null);
        assertThat(provenance.getAdmissionRuntime().getVersion()).isEqualTo(loaded);
    }
}
