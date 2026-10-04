package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.StringRules;
import ai.protomolt.proto.validate.ValidateProto;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentPayloadCheckTest {
    private static final String URL = "type.protomolt.test/payload.Choice";
    private static final DocumentPayloadCheck.Limits LIMITS = new DocumentPayloadCheck.Limits(1024, 100, 10, 100, 1000);

    @Test
    void validatesRealAnnotationsAgainstBoundRetainedSchema() throws Exception {
        var schema = binding(choice("(this.left != '') != (this.right != '')"));
        var original = candidate(schema, "correct", "");
        var checked = check(schema, original, LIMITS);
        assertThat(checked.schema()).isSameAs(schema);
        assertThat(checked.original()).isSameAs(original);
        assertThat(checked.decoded().getField(schema.type().findFieldByName("left"))).isEqualTo("correct");
        for (var invalid : List.of(candidate(schema, "x", ""), candidate(schema, "", ""),
                candidate(schema, "both", "filled"))) {
            assertThatThrownBy(() -> check(schema, invalid, LIMITS))
                    .isInstanceOf(ValidationResult.ValidationException.class);
        }
    }

    @Test
    void preservesNoncanonicalCandidateBytesAndCountsDuplicateTags() throws Exception {
        var schema = binding(choice("true"));
        var single = candidate(schema, "first", "");
        var bytes = single.getValue().concat(candidate(schema, "second", "").getValue());
        var original = single.toBuilder().setValue(bytes).build();
        var checked = check(schema, original, LIMITS);
        assertThat(checked.original().getValue()).isEqualTo(bytes);
        assertThat(checked.decoded().getField(schema.type().findFieldByName("left"))).isEqualTo("second");
        assertThatThrownBy(() -> check(schema, original, new DocumentPayloadCheck.Limits(1024, 1, 10, 100, 1000)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("wire value");
    }

    @Test
    void rejectsWrongTypeAndPrefixBeforeDecoding() throws Exception {
        var schema = binding(choice("true"));
        for (String wrong : List.of("payload.Choice", "other/payload.Choice", "type.protomolt.test/payload.Other")) {
            var candidate = Any.newBuilder().setTypeUrl(wrong).setValue(ByteString.copyFrom(new byte[]{0})).build();
            assertThatThrownBy(() -> check(schema, candidate, LIMITS))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("type URL");
        }
        assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, candidate(schema, "valid", ""),
                "/payload.Choice", validator(), LIMITS, () -> {})).hasMessageContaining("accepted type URL");
    }

    @Test
    void rejectsOversizeMalformedAndUncompilableCandidates() throws Exception {
        var schema = binding(choice("true"));
        assertThatThrownBy(() -> check(schema, candidate(schema, "valid", ""),
                new DocumentPayloadCheck.Limits(1, 100, 10, 100, 1000)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bytes");
        assertThatThrownBy(() -> check(schema, Any.newBuilder().setTypeUrl(URL)
                .setValue(ByteString.copyFrom(new byte[]{10, 100, 1})).build(), LIMITS))
                .isInstanceOf(InvalidProtocolBufferException.class);
        var invalidSchema = binding(choice("this.missing_field == 1"));
        assertThatThrownBy(() -> check(invalidSchema, candidate(invalidSchema, "valid", ""), LIMITS))
                .isInstanceOf(RuleCompilationException.class);
    }

    @Test
    void refusesUnresolvedAnyAndExtensionSchemas() {
        for (Descriptor type : List.of(Any.getDescriptor(), FieldOptions.getDescriptor())) {
            var schema = binding(type);
            String url = "type.protomolt.test/" + type.getFullName();
            var candidate = Any.newBuilder().setTypeUrl(url).build();
            assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, candidate, url, validator(), LIMITS, () -> {}))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void cancellationReturnsNoCheckedCandidate() throws Exception {
        var schema = binding(choice("true"));
        var stop = new CancellationException("cancel payload");
        assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, candidate(schema, "valid", ""), URL,
                validator(), LIMITS, () -> { throw stop; })).isSameAs(stop);
    }

    @Test
    void refusesUnsetNestedAnyAndChecksSchemaLimits() throws Exception {
        var file = FileDescriptorProto.newBuilder().setName("wrapper.proto").setPackage("payload").setSyntax("proto3")
                .addDependency(Any.getDescriptor().getFile().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Wrapper").addField(
                        FieldDescriptorProto.newBuilder().setName("nested").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".google.protobuf.Any"))).build();
        var type = FileDescriptor.buildFrom(file, new FileDescriptor[]{Any.getDescriptor().getFile()})
                .findMessageTypeByName("Wrapper");
        var schema = binding(type);
        String url = "type.protomolt.test/payload.Wrapper";
        var empty = Any.newBuilder().setTypeUrl(url).build();
        assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, empty, url, validator(), LIMITS, () -> {}))
                .isInstanceOf(UnsupportedOperationException.class);
        var limited = new DocumentPayloadCheck.Limits(1024, 100, 10, 1, 1000);
        assertThatThrownBy(() -> DocumentPayloadCheck.check(schema, empty, url, validator(), limited, () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schema message count");
    }

    @Test
    void refusesInterruptedThreadWithoutRelyingOnHostControl() throws Exception {
        var schema = binding(choice("true"));
        var original = candidate(schema, "valid", "");
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> check(schema, original, LIMITS)).isInstanceOf(CancellationException.class);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void preservesUnknownCandidateBytesWithoutClaimingTheirValidation() throws Exception {
        var schema = binding(choice("true"));
        var valid = candidate(schema, "valid", "");
        // Unknown length-delimited field 99 with one opaque byte.
        var bytes = valid.getValue().concat(ByteString.copyFrom(new byte[]{(byte) 0x9a, 0x06, 0x01, 0x7a}));
        var checked = check(schema, valid.toBuilder().setValue(bytes).build(), LIMITS);
        assertThat(checked.original().getValue()).isEqualTo(bytes);
        assertThat(checked.decoded().getUnknownFields().hasField(99)).isTrue();
    }

    private static DocumentPayloadCheck check(DocumentSchemaBinding schema, Any candidate, DocumentPayloadCheck.Limits limits)
            throws InvalidProtocolBufferException {
        return DocumentPayloadCheck.check(schema, candidate, URL, validator(), limits, () -> {});
    }

    private static ProtoValidator validator() {
        return ProtoValidator.create(List.of(new ProtomoltRuleSource()));
    }

    private static Any candidate(DocumentSchemaBinding binding, String left, String right) {
        var type = binding.type();
        var data = DynamicMessage.newBuilder(type).setField(type.findFieldByName("left"), left)
                .setField(type.findFieldByName("right"), right).build();
        return Any.newBuilder().setTypeUrl(URL).setValue(data.toByteString()).build();
    }

    private static DocumentSchemaBinding binding(Descriptor type) {
        var set = DescriptorFingerprints.closure(type);
        var condition = PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(set)).build();
        return DocumentSchemaBinding.bind(condition, set.toByteString(),
                new ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100), () -> {});
    }

    private static Descriptor choice(String expression) throws Exception {
        var message = DescriptorProto.newBuilder().setName("Choice")
                .setOptions(MessageOptions.newBuilder().setExtension(ValidateProto.message,
                        MessageRules.newBuilder().addCel(CelRule.newBuilder().setId("choice.exclusive")
                                .setExpression(expression).setMessage("choose exactly one")).build()));
        for (String field : List.of("left", "right")) {
            message.addField(FieldDescriptorProto.newBuilder().setName(field).setNumber(message.getFieldCount() + 1)
                    .setType(FieldDescriptorProto.Type.TYPE_STRING).setOptions(FieldOptions.newBuilder()
                            .setExtension(ValidateProto.field, FieldRules.newBuilder().setIgnoreIfZero(true)
                                    .setString(StringRules.newBuilder().setMinLen(3)).build())));
        }
        var file = FileDescriptorProto.newBuilder().setName("choice.proto").setPackage("payload").setSyntax("proto3")
                .addDependency(ValidateProto.getDescriptor().getName()).addMessageType(message).build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[]{ValidateProto.getDescriptor()})
                .findMessageTypeByName("Choice");
    }
}
