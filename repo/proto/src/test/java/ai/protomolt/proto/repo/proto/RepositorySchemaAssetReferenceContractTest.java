package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RepositorySchemaAssetReferenceContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    @Test void acceptsNormalizedIdentitiesWithAndWithoutSource() throws Exception {
        check(reference().build(), true);
        check(reference().setSourceSha256("c".repeat(64)).build(), true);
    }
    @Test void rejectsMissingRequiredIdentitiesAndUnsupportedCodecs() throws Exception {
        var value = reference().build();
        for (var field : value.getDescriptorForType().getFields()) {
            if (!field.getName().equals("source_sha256")) check(value.toBuilder().clearField(field).build(), false);
        }
        check(reference().setMetadataCodec("unknown").build(), false);
        check(reference().setMetadataVersion(2).build(), false);
        check(reference().setTypeUrl("x".repeat(4097)).build(), false);
    }
    @Test void rejectsNoncanonicalHashesIncludingAnExplicitEmptySource() throws Exception {
        for (var hash : new String[]{"", "A".repeat(64), "g".repeat(64), "a".repeat(63), "a".repeat(65)}) {
            check(reference().setDescriptorSha256(hash).build(), false);
            check(reference().setMetadataSha256(hash).build(), false);
            check(reference().setSourceSha256(hash).build(), false);
        }
    }
    @Test void jsonSchemaCarriesTheIdentityBounds() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode schema = mapper.valueToTree(generator.generateRooted(RepositorySchemaAssetReference.getDescriptor()));
        assertThat(schema.at("/properties/typeUrl/maxLength").asInt()).isEqualTo(4096);
        assertThat(schema.at("/properties/descriptorSha256/pattern").asText()).isEqualTo("^[0-9a-f]{64}$");
    }
    private static RepositorySchemaAssetReference.Builder reference() {
        return RepositorySchemaAssetReference.newBuilder().setTypeUrl("type.test/example.Payload")
                .setDescriptorSha256("a".repeat(64)).setMetadataSha256("b".repeat(64))
                .setMetadataCodec("repository-schema-asset").setMetadataVersion(1);
    }
    private static void check(RepositorySchemaAssetReference value, boolean valid) throws Exception {
        assertThat(VALIDATOR.validate(value).valid()).as("generated %s", value).isEqualTo(valid);
        assertThat(VALIDATOR.validate(DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString())).valid())
                .as("dynamic %s", value).isEqualTo(valid);
    }
}
