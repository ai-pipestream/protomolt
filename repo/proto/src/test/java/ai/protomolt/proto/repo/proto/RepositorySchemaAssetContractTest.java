package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Shape checks through the real runtime; toolchain claims still require host verification. */
class RepositorySchemaAssetContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String SHA = "a".repeat(64);

    @Test void localAndImportedProvenanceAreExplicit() throws Exception {
        check(asset(), true);
        var reported = local().toBuilder().setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_PRODUCER_REPORTED).build();
        check(asset().toBuilder().setCompilation(reported).build(), true);
        var unknown = reported.toBuilder().setUnknownCompilerReason("Reflection did not report a compiler version")
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN).build();
        check(asset().toBuilder().setCompilation(unknown).build(), true);
        assertThat(unknown.hasKnownCompiler()).isFalse();
    }

    @Test void rejectsMissingAndContradictoryCompilerEvidence() throws Exception {
        check(asset().toBuilder().clearCompilation().build(), false);
        check(local().toBuilder().clearCompiler().build(), false);
        check(local().toBuilder().setUnknownCompilerReason("unknown").build(), false);
        check(local().toBuilder().setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN).build(), false);
        check(local().toBuilder().setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR).build(), false);
        check(local().toBuilder().clearAdmissionRuntime().build(), false);
        check(local().toBuilder().clearSourceArtifactSha256().build(), false);
        check(local().toBuilder().setOriginValue(99).build(), false);
        check(local().toBuilder().setEvidenceValue(99).build(), false);
        var unknown = local().toBuilder().setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN).setUnknownCompilerReason(" ").build();
        check(unknown, false);
    }

    @Test void checksExactIdentitySyntaxWithoutConflatingHashes() throws Exception {
        check(asset().toBuilder().setArtifactSha256("b".repeat(64)).build(), true);
        check(asset().toBuilder().clearSchema().build(), false);
        check(asset().toBuilder().setArtifactSha256("invalid").build(), false);
        for (String url : new String[]{"example.Record", "/example.Record", "type.test/example.Other", "a".repeat(4097)}) {
            check(asset().toBuilder().setTypeUrl(url).build(), false);
        }
        check(local().toBuilder().setSourceArtifactSha256("invalid").build(), false);
        check(local().toBuilder().setSourceArtifactSha256(SHA).build(), true);
    }

    @Test void boundsToolAndCompilerMetadata() throws Exception {
        for (String invalid : new String[]{"", " ", "a".repeat(201)}) {
            check(tool().toBuilder().setVersion(invalid).build(), false);
            check(tool().toBuilder().setName(invalid).build(), false);
        }
        check(tool().toBuilder().setArtifactSha256("not-a-hash").build(), false);
        var compiler = local().getKnownCompiler().toBuilder().putOptions("edition", "2023");
        check(compiler.build(), true);
        check(compiler.clone().putOptions(" ", "value").build(), false);
        check(compiler.clone().putOptions("large", "x".repeat(4097)).build(), false);
        compiler.clearOptions();
        for (int i = 0; i < 65; i++) compiler.putOptions("option" + i, "value");
        check(compiler.build(), false);
    }

    @Test void jsonSchemaReportsSyntaxAndRuntimeOnlyRules() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(RepositorySchemaAsset.getDescriptor()));
        assertThat(schema.at("/properties/artifactSha256/pattern").asText()).isEqualTo("^[0-9a-f]{64}$");
        assertThat(schema.at("/properties/typeUrl/maxLength").asInt()).isEqualTo(4096);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("schema-asset-type-url");
        com.fasterxml.jackson.databind.JsonNode provenance = mapper.valueToTree(generator.generateRooted(SchemaCompilationProvenance.getDescriptor()));
        assertThat(provenance.path("x-protomolt-cel").toString()).contains("schema-compiler-evidence");
        // Cross-field CEL is exposed metadata, not executable standard JSON Schema.
    }

    private static RepositorySchemaAsset asset() {
        return RepositorySchemaAsset.newBuilder().setSchema(PublicationSchemaCondition.newBuilder()
                        .setTypeName("example.Record").setDescriptorFingerprint(SHA))
                .setArtifactSha256(SHA).setTypeUrl("type.test/example.Record").setCompilation(local()).build();
    }
    private static SchemaToolIdentity tool() {
        return SchemaToolIdentity.newBuilder().setName("test-compiler").setVersion("1.0").build();
    }
    private static SchemaCompilationProvenance local() {
        return SchemaCompilationProvenance.newBuilder().setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_LOCAL_SOURCE)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_LOCAL_OBSERVED)
                .setSourceArtifactSha256(SHA).setKnownCompiler(SchemaCompilerDetails.newBuilder().setTool(tool()))
                .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-protobuf-runtime").setVersion("1.0")).build();
    }
    private static void check(Message value, boolean expected) throws Exception {
        for (Message candidate : new Message[]{value, DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteArray())}) {
            var result = VALIDATOR.validate(candidate);
            assertThat(result.valid()).as("%s: %s", value.getDescriptorForType().getFullName(), result).isEqualTo(expected);
        }
    }
}
