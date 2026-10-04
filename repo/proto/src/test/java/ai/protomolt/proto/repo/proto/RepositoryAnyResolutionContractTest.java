package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryAnyResolutionContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String SHA = "a".repeat(64);

    @Test void opaqueResolutionDoesNotRequireASchemaOrUrlSyntaxClaim() throws Exception {
        check(base().setNotAttempted(true).build(), true);
        check(base().setTypeUrl("").setNotAttempted(true).build(), true);
        check(base().setTypeUrl("opaque-label").setNotAttempted(true).build(), true);
        check(base().setNotAttempted(false).build(), false);
        check(base().build(), false);
        for (var failure : RepositorySchemaResolutionFailure.values()) {
            if (failure == RepositorySchemaResolutionFailure.UNRECOGNIZED) continue;
            check(base().setFailure(failure).build(), failure != RepositorySchemaResolutionFailure.REPOSITORY_SCHEMA_RESOLUTION_FAILURE_UNSPECIFIED);
        }
        check(base().setFailureValue(99).build(), false);
    }

    @Test void resolvedSchemaMustMatchButDoesNotConferValidation() throws Exception {
        var resolved = RepositoryResolvedSchema.newBuilder().setArtifactSha256(SHA)
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("archive.Record").setDescriptorFingerprint(SHA));
        check(base().setResolved(resolved).build(), true);
        check(base().setTypeUrl("other-prefix/archive.Record").setResolved(resolved).build(), true);
        check(base().setTypeUrl("types.test/archive.Other").setResolved(resolved).build(), false);
        check(base().setResolved(resolved.clone().clearSchema()).build(), false);
        check(base().setResolved(resolved.clone().setArtifactSha256("bad")).build(), false);
        var replaced = base().setResolved(resolved).setNotAttempted(true).build();
        assertThat(replaced.hasResolved()).isFalse();
        check(replaced, true);
    }

    @Test void rejectsMalformedHashAndOversizedUrl() throws Exception {
        check(base().setNotAttempted(true).setValueSha256("bad").build(), false);
        check(base().setNotAttempted(true).setTypeUrl("x".repeat(4097)).build(), false);
        check(base().setNotAttempted(true).setValueSizeBytes(0).build(), true);
    }

    @Test void jsonSchemaExposesRuntimeOnlyCrossFieldRules() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(RepositoryAnyResolution.getDescriptor()));
        assertThat(schema.at("/properties/typeUrl/maxLength").asInt()).isEqualTo(4096);
        assertThat(schema.at("/properties/valueSha256/pattern").asText()).isEqualTo("^[0-9a-f]{64}$");
        assertThat(schema.path("x-protomolt-cel").toString()).contains("any-resolution-outcome", "any-resolution-type");
        // CEL metadata does not make these rules executable by standard JSON Schema.
    }

    private static RepositoryAnyResolution.Builder base() {
        return RepositoryAnyResolution.newBuilder().setTypeUrl("types.test/archive.Record").setValueSha256(SHA).setValueSizeBytes(3);
    }

    private static void check(RepositoryAnyResolution value, boolean expected) throws Exception {
        assertThat(VALIDATOR.validate(value).valid()).as("generated %s", value).isEqualTo(expected);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteArray())).valid())
                .as("dynamic %s", value).isEqualTo(expected);
    }
}
