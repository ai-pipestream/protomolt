package ai.pipestream.proto.helpers;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BinaryProtobufIdentifierValidatorTest {

    @Test
    void validateIsNoOpForNullDescriptor() {
        assertThatCode(() -> BinaryProtobufIdentifierValidator.validate("anything", null))
                .doesNotThrowAnyException();
    }

    @Test
    void validateRequiresSourceNameWhenDescriptorPresent() {
        assertThrows(NullPointerException.class,
                () -> BinaryProtobufIdentifierValidator.validate(null, validFile()));
    }

    @Test
    void validateAcceptsWellFormedDescriptor() {
        assertThatCode(() -> BinaryProtobufIdentifierValidator.validate("ok", validFile()))
                .doesNotThrowAnyException();
    }

    @Test
    void validateRejectsInvalidIdentifierWithSourceAttribution() {
        FileDescriptorProto evil = FileDescriptorProto.newBuilder()
                .setName("evil.proto")
                .setPackage("p")
                .addMessageType(DescriptorProto.newBuilder().setName("Data(){console}"))
                .build();

        assertThatThrownBy(() -> BinaryProtobufIdentifierValidator.validate("evil-source", evil))
                .isInstanceOf(ProtoSchemaValidationException.class)
                .hasMessageContaining("identifier")
                .hasMessageContaining("evil-source");
    }

    @Test
    void validateRejectsMissingMessageName() {
        // protobuf-java reports an unnamed message as "Missing name." — the same marker the
        // validator treats as a rejected identifier. (An unnamed *file* is not flagged by
        // protobuf-java and is deliberately left to the caller's normal validation.)
        FileDescriptorProto nameless = FileDescriptorProto.newBuilder()
                .setName("nameless.proto")
                .setPackage("p")
                .addMessageType(DescriptorProto.newBuilder())
                .build();

        assertThatThrownBy(() -> BinaryProtobufIdentifierValidator.validate("nameless", nameless))
                .isInstanceOf(ProtoSchemaValidationException.class)
                .hasMessageContaining("nameless");
    }

    /**
     * Build failures that are not identifier grammar problems (here: two fields sharing a tag)
     * are deliberately left to the caller's normal protobuf validation, not reported as
     * malicious identifiers.
     */
    @Test
    void validateIgnoresNonIdentifierBuildFailures() {
        FileDescriptorProto duplicateTags = FileDescriptorProto.newBuilder()
                .setName("dup.proto")
                .setPackage("p")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Dup")
                        .addField(field("a", 1))
                        .addField(field("b", 1)))
                .build();

        assertThatCode(() -> BinaryProtobufIdentifierValidator.validate("dup", duplicateTags))
                .doesNotThrowAnyException();
    }

    @Test
    void tryParseBase64DescriptorReturnsNullForNullEmptyAndBlank() {
        assertThat(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor(null)).isNull();
        assertThat(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor("")).isNull();
        assertThat(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor("   ")).isNull();
    }

    @Test
    void tryParseBase64DescriptorReturnsNullForTextProto() {
        assertThat(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor(
                "syntax = \"proto3\"; message X {}")).isNull();
    }

    @Test
    void tryParseBase64DescriptorReturnsNullForNonDescriptorBytes() {
        // Decodes as base64 but is not a parseable FileDescriptorProto (invalid wire tag).
        String encoded = Base64.getEncoder().encodeToString(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        assertThat(BinaryProtobufIdentifierValidator.tryParseBase64Descriptor(encoded)).isNull();
    }

    @Test
    void tryParseBase64DescriptorRoundTripsValidDescriptor() {
        FileDescriptorProto file = validFile();
        String encoded = Base64.getEncoder().encodeToString(file.toByteArray());

        FileDescriptorProto parsed = BinaryProtobufIdentifierValidator.tryParseBase64Descriptor(encoded);

        assertThat(parsed).isEqualTo(file);
    }

    @Test
    void validateBytesAcceptsValidDescriptor() {
        assertThatCode(() -> BinaryProtobufIdentifierValidator.validateBytes("ok", validFile().toByteArray()))
                .doesNotThrowAnyException();
    }

    @Test
    void validateBytesRejectsGarbage() {
        assertThatThrownBy(() -> BinaryProtobufIdentifierValidator.validateBytes(
                "blob", new byte[]{(byte) 0xFF, (byte) 0xFE}))
                .isInstanceOf(ProtoSchemaValidationException.class)
                .hasMessageContaining("not a valid FileDescriptorProto")
                .hasMessageContaining("blob");
    }

    @Test
    void validateBytesRejectsNull() {
        assertThrows(NullPointerException.class,
                () -> BinaryProtobufIdentifierValidator.validateBytes("blob", null));
    }

    @Test
    void validateBytesPropagatesIdentifierRejection() {
        FileDescriptorProto evil = FileDescriptorProto.newBuilder()
                .setName("evil.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Bad()Name"))
                .build();

        assertThatThrownBy(() -> BinaryProtobufIdentifierValidator.validateBytes("evil", evil.toByteArray()))
                .isInstanceOf(ProtoSchemaValidationException.class)
                .hasMessageContaining("identifier");
    }

    private static FileDescriptorProto validFile() {
        return FileDescriptorProto.newBuilder()
                .setName("ok.proto")
                .setPackage("p")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Ok")
                        .addField(field("a", 1)))
                .build();
    }

    private static FieldDescriptorProto field(String name, int number) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .build();
    }
}
