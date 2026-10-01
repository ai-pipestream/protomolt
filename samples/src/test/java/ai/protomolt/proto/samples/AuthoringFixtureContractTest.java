package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.NormalizeTextRequest;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextResponse;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import ai.protomolt.proto.samples.authoring.v1.FixtureStoredRecord;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Runtime contract fixtures only; no claim of RPC execution or durable storage. */
class AuthoringFixtureContractTest {
    private static final String ID = "aa123456-1234-4234-8234-123456789abc";

    @Test
    void normalizationBoundariesApplyToGeneratedAndDynamicMessages() {
        check(NormalizeTextRequest.newBuilder().setText("  Court\r\nrecord  ").build(), true);
        check(NormalizeTextResponse.newBuilder().setText("Court\nrecord").build(), true);
        check(NormalizeTextRequest.getDefaultInstance(), false);
        check(NormalizeTextResponse.getDefaultInstance(), false);
        check(NormalizeTextRequest.newBuilder().setText("a".repeat(8192)).build(), true);
        check(NormalizeTextRequest.newBuilder().setText("a".repeat(8193)).build(), false);
        check(NormalizeTextResponse.newBuilder().setText("a".repeat(8193)).build(), false);
        // Shape-valid whitespace still needs the handler's empty-normalization rejection.
        check(NormalizeTextRequest.newBuilder().setText(" \t\r\n ").build(), true);
    }

    @Test
    void writeRequiresCanonicalIdentityBoundedContentAndDigest() {
        var request = WriteRecordRequest.newBuilder().setOperationId(ID).setContent("Court record").build();
        check(request, true);
        check(request.toBuilder().clearOperationId().build(), false);
        check(request.toBuilder().setOperationId(ID.toUpperCase(java.util.Locale.ROOT)).build(), false);
        check(request.toBuilder().setOperationId("../record").build(), false);
        check(request.toBuilder().clearContent().build(), false);
        check(request.toBuilder().setContent("a".repeat(8193)).build(), false);
        check(request.toBuilder().setContent("\uD83D\uDE00".repeat(8192)).build(), true);
        check(request.toBuilder().setContent("\uD83D\uDE00".repeat(8193)).build(), false);
        var response = WriteRecordResponse.newBuilder().setOperationId(ID)
                .setContentSha256("a".repeat(64)).build();
        check(response, true);
        check(response.toBuilder().clearOperationId().build(), false);
        check(response.toBuilder().setContentSha256("not-a-digest").build(), false);
        // A syntactically valid digest is not proof of content identity or a stored effect.
        check(response.toBuilder().setContentSha256("b".repeat(64)).build(), true);
        var stored = FixtureStoredRecord.newBuilder().setFormatVersion(1)
                .setRequest(request).setResponse(response).build();
        check(stored, true);
        check(stored.toBuilder().setFormatVersion(2).build(), false);
        check(stored.toBuilder().clearRequest().build(), false);
        check(stored.toBuilder().setResponse(response.toBuilder()
                .setOperationId("bb123456-1234-4234-8234-123456789abc")).build(), false);
    }

    private static void check(Message message, boolean valid) {
        var validator = ProtoValidator.forMessageType(message.getDescriptorForType());
        assertThat(validator.validate(message).valid()).as(message.toString()).isEqualTo(valid);
        var dynamic = DynamicMessage.newBuilder(message.getDescriptorForType()).mergeFrom(message).build();
        assertThat(validator.validate(dynamic).valid()).as("dynamic " + message).isEqualTo(valid);
    }
}
