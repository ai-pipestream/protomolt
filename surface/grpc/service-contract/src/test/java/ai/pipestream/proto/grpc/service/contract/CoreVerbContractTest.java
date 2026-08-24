package ai.pipestream.proto.grpc.service.contract;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bounds on the core toolkit request messages, read off the compiled definition. A
 * schema source is the shared shape underneath most of them, and its rule is the one worth
 * pinning hardest: the three ways of naming a schema are alternatives, and a request that
 * gives two of them has not said which one it means.
 */
class CoreVerbContractTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static Descriptor type(String name) {
        Descriptor descriptor = ProtoMoltServiceSchema.file().findMessageTypeByName(name);
        assertThat(descriptor).as("message %s", name).isNotNull();
        return descriptor;
    }

    private static DynamicMessage.Builder builder(String name) {
        return DynamicMessage.newBuilder(type(name));
    }

    private static DynamicMessage schemaSource(String typeName, String descriptorSet) {
        DynamicMessage.Builder source = builder("SchemaSource");
        Descriptor descriptor = type("SchemaSource");
        if (typeName != null) {
            source.setField(descriptor.findFieldByName("type"), typeName);
        }
        if (descriptorSet != null) {
            source.setField(descriptor.findFieldByName("descriptor_set_base64"), descriptorSet);
        }
        return source.build();
    }

    private static boolean valid(DynamicMessage message) {
        ValidationResult result = VALIDATOR.validate(message);
        return result.valid();
    }

    @Test
    void aSchemaSourceNamingExactlyOneWayIsAccepted() {
        assertThat(valid(schemaSource("google.protobuf.Struct", null))).isTrue();
        assertThat(valid(schemaSource(null, "Zm9v"))).isTrue();
    }

    @Test
    void aSchemaSourceNamingTwoWaysIsRefused() {
        assertThat(valid(schemaSource("google.protobuf.Struct", "Zm9v"))).isFalse();
    }

    @Test
    void aSchemaSourceNamingNoWayIsRefused() {
        assertThat(valid(schemaSource(null, null))).isFalse();
    }

    @Test
    void compilingNothingIsRefused() {
        assertThat(valid(builder("CompileRequest").build())).isFalse();
    }

    @Test
    void anEvalRequestNeedsBothItsSchemaAndItsExpression() {
        Descriptor descriptor = type("EvalCelRequest");
        FieldDescriptor schema = descriptor.findFieldByName("schema");
        FieldDescriptor message = descriptor.findFieldByName("message");
        FieldDescriptor expression = descriptor.findFieldByName("expression");

        DynamicMessage.Builder complete = builder("EvalCelRequest")
                .setField(schema, schemaSource("google.protobuf.Struct", null))
                .setField(message, DynamicMessage.getDefaultInstance(message.getMessageType()))
                .setField(expression, "input.size() > 0");
        assertThat(valid(complete.build())).isTrue();

        assertThat(valid(complete.clone().clearField(expression).build())).isFalse();
        assertThat(valid(complete.clone().clearField(schema).build())).isFalse();
    }
}
