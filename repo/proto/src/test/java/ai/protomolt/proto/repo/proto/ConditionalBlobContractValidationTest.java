package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.ConditionalBlobVersion;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionalBlobContractValidationTest {
    private static final int MAX_BLOB_BYTES = 9 * 1024 * 1024;
    private static final String SHA = "a".repeat(64);

    private static String maxEtag() {
        return "\"" + "x".repeat(1022) + "\"";
    }

    private static ConditionalBlobKey key() {
        return ConditionalBlobKey.newBuilder().setDriveName("drive-main").setObjectKey("folder/item.bin").build();
    }

    private static ConditionalBlobVersion version(long size) {
        return ConditionalBlobVersion.newBuilder().setKey(key()).setEtag("\"opaque-etag-7\"")
                .setSizeBytes(size).setSha256(SHA).build();
    }

    private static ValidationResult validate(Message message) {
        return ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
    }

    private static ValidationResult validateDynamic(Message message) throws Exception {
        DynamicMessage dynamic = DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
        return ProtoValidator.forMessageType(dynamic.getDescriptorForType()).validate(dynamic);
    }

    private static void validBoth(Message message) throws Exception {
        ValidationResult generated = validate(message);
        ValidationResult dynamic = validateDynamic(message);
        assertThat(generated.valid()).as("generated violations: %s", generated.violations()).isTrue();
        assertThat(dynamic.valid()).as("dynamic violations: %s", dynamic.violations()).isTrue();
    }

    private static void invalidBoth(Message message) throws Exception {
        ValidationResult generated = validate(message);
        ValidationResult dynamic = validateDynamic(message);
        assertThat(generated.valid()).as("generated violations: %s", generated.violations()).isFalse();
        assertThat(dynamic.valid()).as("dynamic violations: %s", dynamic.violations()).isFalse();
        assertThat(dynamic.violations()).isNotEmpty();
    }

    @Test
    void keysAndReadRequestsRequireNonblankCoordinatesAndNestedKey() throws Exception {
        validBoth(key());
        validBoth(GetBlobForUpdateRequest.newBuilder().setKey(key()).build());
        invalidBoth(ConditionalBlobKey.newBuilder().setObjectKey("file").build());
        invalidBoth(ConditionalBlobKey.newBuilder().setDriveName("drive").build());
        invalidBoth(ConditionalBlobKey.newBuilder().setDriveName("  ").setObjectKey("file").build());
        invalidBoth(ConditionalBlobKey.newBuilder().setDriveName("drive").setObjectKey(" \t ").build());
        invalidBoth(GetBlobForUpdateRequest.newBuilder().build());
    }

    @Test
    void writeRequiresExplicitTrueIfAbsentOrNonblankExpectedEtag() throws Exception {
        validBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setIfAbsent(true).build());
        validBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("\"old-etag\"").build());
        validBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("\"*\"").build());
        validBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setIfAbsent(true).setData(ByteString.EMPTY).build());
        CompareAndPutBlobRequest missing = CompareAndPutBlobRequest.newBuilder().setKey(key()).build();
        invalidBoth(missing);
        assertThat(validate(missing).violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo("conditional-blob-precondition"));
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setIfAbsent(true).build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setIfAbsent(false).build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("").build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("   ").build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("*").build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("W/\"old\"").build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("\"one\", \"two\"").build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("etag\r\nInjected: yes").build());
        validBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag(maxEtag()).build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setExpectedEtag("\"" + "x".repeat(1023) + "\"").build());
    }

    @Test
    void unaryByteBoundsIncludeEmptyAndNineMibButRejectOneByteOver() throws Exception {
        validBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setIfAbsent(true).build());
        validBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setIfAbsent(true)
                .setData(ByteString.copyFrom(new byte[MAX_BLOB_BYTES])).build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setIfAbsent(true)
                .setData(ByteString.copyFrom(new byte[MAX_BLOB_BYTES + 1])).build());
        validBoth(GetBlobForUpdateResponse.newBuilder().setVersion(version(0)).build());
        validBoth(GetBlobForUpdateResponse.newBuilder().setVersion(version(MAX_BLOB_BYTES))
                .setData(ByteString.copyFrom(new byte[MAX_BLOB_BYTES])).build());
        invalidBoth(GetBlobForUpdateResponse.newBuilder().setVersion(version(MAX_BLOB_BYTES + 1L))
                .setData(ByteString.copyFrom(new byte[MAX_BLOB_BYTES + 1])).build());
    }

    @Test
    void responseRequiresVersionAndEnforcesSizeDigestAndEtag() throws Exception {
        GetBlobForUpdateResponse empty = GetBlobForUpdateResponse.newBuilder().setVersion(version(0)).build();
        validBoth(empty);
        invalidBoth(GetBlobForUpdateResponse.newBuilder().build());
        invalidBoth(GetBlobForUpdateResponse.newBuilder().setVersion(version(0).toBuilder().setSizeBytes(-1)).build());
        GetBlobForUpdateResponse mismatch = empty.toBuilder().setData(ByteString.copyFromUtf8("x")).build();
        invalidBoth(mismatch);
        assertThat(validate(mismatch).violations()).anySatisfy(v -> assertThat(v.ruleId()).isEqualTo("conditional-blob-read-size"));
        invalidBoth(empty.toBuilder().setVersion(version(0).toBuilder().setSha256("not-a-digest")).build());
        invalidBoth(empty.toBuilder().setVersion(version(0).toBuilder().setEtag("*")).build());
        validBoth(empty.toBuilder().setVersion(version(0).toBuilder().setEtag("\"*\"")).build());
        validBoth(empty.toBuilder().setVersion(version(0).toBuilder().setEtag(maxEtag())).build());
        invalidBoth(empty.toBuilder().setVersion(version(0).toBuilder().setEtag("\"" + "x".repeat(1023) + "\"")).build());
        invalidBoth(empty.toBuilder().setVersion(version(0).toBuilder().setEtag("bad\r\nInjected: yes")).build());
        invalidBoth(CompareAndPutBlobResponse.newBuilder().setVersion(version(0).toBuilder().setSha256("f".repeat(63))).build());
        invalidBoth(CompareAndPutBlobResponse.newBuilder().build());
        validBoth(CompareAndPutBlobResponse.newBuilder().setVersion(version(0)).build());
    }

    @Test
    void optionalMimeDistinguishesAbsentFromExplicitEmpty() throws Exception {
        validBoth(GetBlobForUpdateResponse.newBuilder().setVersion(version(0)).build());
        validBoth(GetBlobForUpdateResponse.newBuilder().setVersion(version(0)).setMimeType("application/octet-stream").build());
        invalidBoth(GetBlobForUpdateResponse.newBuilder().setVersion(version(0)).setMimeType("").build());
        invalidBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setIfAbsent(true).setMimeType("").build());
        validBoth(CompareAndPutBlobRequest.newBuilder().setKey(key()).setIfAbsent(true).setMimeType("application/octet-stream").build());
    }

    @Test
    void jsonSchemaShowsStructureAndCelRulesWhileValidateV1RunsAtRuntime() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ProtoJsonSchemaGenerator generator = ProtoJsonSchemaGenerator.create();
        JsonNode request = mapper.valueToTree(generator.generateRooted(CompareAndPutBlobRequest.getDescriptor()));
        assertThat(request.at("/properties/data/type").asText()).isEqualTo("string");
        assertThat(request.at("/properties/data/contentEncoding").asText()).isEqualTo("base64");
        assertThat(request.path("x-protomolt-cel").toString()).contains("conditional-blob-precondition");
        JsonNode response = mapper.valueToTree(generator.generateRooted(GetBlobForUpdateResponse.getDescriptor()));
        assertThat(response.path("x-protomolt-cel").toString()).contains("conditional-blob-read-size");
        assertThat(response.path("properties").has("mimeType")).isTrue();
        // The generator emits data's base64 representation and CEL IDs, but does not
        // project the bytes.max_len rule; byte limits, optional-string presence, and
        // validate.v1 formats are verified by the runtime fixtures above.
    }
}
