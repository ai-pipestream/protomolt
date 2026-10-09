package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RepositorySchemaOccurrenceContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String SHA = "a".repeat(64);

    @Test void resolvedBoundariesAndExplicitVersionAreRequired() throws Exception {
        var boundary = boundary();
        var path = RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1).addSteps(boundary);
        check(path.build(), true);
        check(path.clone().addSteps(boundary).build(), true); // Any containing an Any.
        check(path.clone().clearSteps().build(), false);
        check(path.clone().setEncodingVersion(0).build(), false);
        check(path.clone().setEncodingVersion(2).build(), false);
        check(path.clone().addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(1)).build(), false);
        check(RepositorySchemaOccurrenceStep.getDefaultInstance(), false);
        check(boundary.toBuilder().setAnyBoundary(boundary.getAnyBoundary().toBuilder().setNotAttempted(true)).build(), false);
        check(boundary.toBuilder().setAnyBoundary(boundary.getAnyBoundary().toBuilder()
                .setFailure(RepositorySchemaResolutionFailure.REPOSITORY_SCHEMA_RESOLUTION_FAILURE_LOOKUP_FAILED)).build(), false);
        check(boundary.toBuilder().setAnyBoundary(boundary.getAnyBoundary().toBuilder().setValueSha256("bad")).build(), false);
        path.clearSteps();
        for (int i = 0; i < 301; i++) path.addSteps(boundary);
        check(path.build(), true); // Structural validity does not prove candidate depth.
        check(path.addSteps(boundary).build(), false);
    }

    @Test void fieldNumbersAndIndexesAreBounded() throws Exception {
        for (int number : new int[]{1, 18999, 20000, 536870911})
            check(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(number).build(), true);
        for (int number : new int[]{0, 19000, 19500, 19999, 536870912, -1})
            check(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(number).build(), false);
        check(RepositorySchemaOccurrenceStep.newBuilder().setRepeatedIndex(0).build(), true);
        check(RepositorySchemaOccurrenceStep.newBuilder().setRepeatedIndex(Integer.MAX_VALUE).build(), true);
        check(RepositorySchemaOccurrenceStep.newBuilder().setRepeatedIndex(-1).build(), false);
    }

    @Test void keyKindsWidthsAndDefaultValuesAreExplicit() throws Exception {
        for (int type = 1; type <= 12; type++) {
            var key = RepositoryOccurrenceMapKey.newBuilder().setTypeValue(type);
            check(key.build(), false);
            if (type == 1) key.setStringValue("");
            else if (type == 2) key.setBoolValue(false);
            else if (type == 6 || type == 7 || type == 11 || type == 12) key.setUnsignedValue(0);
            else key.setSignedValue(0);
            check(key.build(), true);
            check(key.clone().setTypeValue(0).build(), false);
            check(key.clone().setTypeValue(99).build(), false);
            if (type >= 3 && type <= 5) {
                check(key.clone().setSignedValue(Integer.MIN_VALUE).build(), true);
                check(key.clone().setSignedValue(Integer.MAX_VALUE).build(), true);
                check(key.clone().setSignedValue(2147483648L).build(), false);
                check(key.clone().setSignedValue(-2147483649L).build(), false);
            }
            if (type == 6 || type == 7) {
                check(key.clone().setUnsignedValue(4294967295L).build(), true);
                check(key.clone().setUnsignedValue(4294967296L).build(), false);
            }
            if (type == 11 || type == 12) check(key.clone().setUnsignedValue(-1L).build(), true);
            if (type >= 8 && type <= 10) {
                check(key.clone().setSignedValue(Long.MIN_VALUE).build(), true);
                check(key.clone().setSignedValue(Long.MAX_VALUE).build(), true);
            }
            check(key.clone().setTypeValue(type == 1 ? 2 : 1).build(), false);
        }
    }

    @Test void jsonSchemaPreservesBoundsAndReportsRuntimeRules() {
        var generator = ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator.create();
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        com.fasterxml.jackson.databind.JsonNode path = mapper.valueToTree(generator.generateRooted(RepositorySchemaOccurrencePath.getDescriptor()));
        assertThat(path.at("/properties/steps/maxItems").asInt()).isEqualTo(301);
        assertThat(path.path("x-protomolt-cel").toString()).contains("occurrence-path-boundaries");
        com.fasterxml.jackson.databind.JsonNode key = mapper.valueToTree(generator.generateRooted(RepositoryOccurrenceMapKey.getDescriptor()));
        assertThat(key.path("x-protomolt-cel").toString()).contains("occurrence-key-kind");
    }

    private static RepositorySchemaOccurrenceStep boundary() {
        return RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                .setTypeUrl("type.test/example.Record").setValueSha256(SHA).setValueSizeBytes(12)
                .setResolved(RepositoryResolvedSchema.newBuilder().setArtifactSha256(SHA)
                        .setSchema(PublicationSchemaCondition.newBuilder().setTypeName("example.Record").setDescriptorFingerprint(SHA))))
                .build();
    }

    private static void check(Message value, boolean valid) throws Exception {
        assertThat(VALIDATOR.validate(value).valid()).as("generated %s", value).isEqualTo(valid);
        var dynamic = DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteString());
        assertThat(VALIDATOR.validate(dynamic).valid()).as("dynamic %s", value).isEqualTo(valid);
    }
}
