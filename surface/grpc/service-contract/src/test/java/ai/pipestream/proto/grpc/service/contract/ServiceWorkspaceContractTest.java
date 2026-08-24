package ai.pipestream.proto.grpc.service.contract;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bounds on the service-workspace and invoke request messages are declared in the proto
 * and enforced from it. The service is compiled from source at load and bound with dynamic
 * messages, so a rule that survives that round trip is a rule the running server applies;
 * one that does not would leave the published contract advertising a bound nothing checks.
 */
class ServiceWorkspaceContractTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static Descriptor request(String name) {
        Descriptor descriptor = ProtoMoltServiceSchema.file().findMessageTypeByName(name);
        assertThat(descriptor).as("message %s", name).isNotNull();
        return descriptor;
    }

    /**
     * A message satisfying every required field, so that changing one field under test is
     * the only reason a result can turn invalid. Filling only the field under test would
     * leave the others missing and make every assertion pass for the wrong reason.
     */
    private static DynamicMessage.Builder valid(String message) {
        Descriptor descriptor = request(message);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        set(builder, descriptor, "name", "billing-api");
        set(builder, descriptor, "target", "localhost:9090");
        set(builder, descriptor, "method", "example.v1.Service/Call");
        FieldDescriptor request = descriptor.findFieldByName("request");
        if (request != null) {
            builder.setField(request, DynamicMessage.getDefaultInstance(request.getMessageType()));
        }
        FieldDescriptor schema = descriptor.findFieldByName("schema");
        if (schema != null) {
            builder.setField(schema, DynamicMessage.getDefaultInstance(schema.getMessageType()));
        }
        return builder;
    }

    private static void set(DynamicMessage.Builder builder, Descriptor descriptor,
            String field, String value) {
        FieldDescriptor found = descriptor.findFieldByName(field);
        if (found != null && found.getJavaType() == FieldDescriptor.JavaType.STRING) {
            builder.setField(found, value);
        }
    }

    private static ValidationResult validate(String message, String field, Object value) {
        Descriptor descriptor = request(message);
        DynamicMessage.Builder builder = valid(message);
        if (value == null) {
            builder.clearField(descriptor.findFieldByName(field));
        } else {
            builder.setField(descriptor.findFieldByName(field), value);
        }
        return VALIDATOR.validate(builder.build());
    }

    @Test
    void aProfileNameMustBePathSafe() {
        // The name reaches the workspace's storage layout, so the format rule replaces what
        // was a hand-written pattern in the published JSON schema.
        assertThat(validate("ServiceInspectRequest", "name", "billing-api").valid()).isTrue();
        assertThat(validate("ServiceInspectRequest", "name", "billing/api").valid()).isFalse();
    }

    @Test
    void aMissingProfileNameIsRefused() {
        assertThat(validate("ServiceInspectRequest", "name", null).valid()).isFalse();
    }

    @Test
    void aDeadlineBeyondTheChannelPolicyCeilingIsRefused() {
        // 60000 is what the outbound channel policy allows; declaring it here means a caller
        // reading the contract learns the limit instead of discovering it on a refused call.
        assertThat(validate("ServiceRefreshRequest", "deadline_ms", 60_000).valid()).isTrue();
        assertThat(validate("ServiceRefreshRequest", "deadline_ms", 60_001).valid()).isFalse();
        assertThat(validate("ServiceRefreshRequest", "deadline_ms", -1).valid()).isFalse();
    }

    @Test
    void aResponseCapBeyondWhatOneCallMayHoldIsRefused() {
        assertThat(validate("ServiceInvokeRequest", "max_responses", 4_096).valid()).isTrue();
        assertThat(validate("ServiceInvokeRequest", "max_responses", 4_097).valid()).isFalse();
    }

    @Test
    void aReflectTargetIsBoundedButNotNarrowedToAnAuthority() {
        assertThat(validate("ReflectRequest", "target", "localhost:9090").valid()).isTrue();
        assertThat(validate("ReflectRequest", "target", "dns:///svc:443").valid()).isTrue();
        // An in-process name is a target the channel factory resolves, so the message must
        // not refuse it; which forms are permitted is the channel policy's decision.
        assertThat(validate("ReflectRequest", "target", "in-process-7f3a").valid()).isTrue();
        assertThat(validate("ReflectRequest", "target", null).valid()).isFalse();
    }

    @Test
    void theRegisteredProfileCarriesItsDeclaredType() {
        // Not a Struct: the field always held a ServiceProfile written as JSON, which the
        // action parsed by hand. Declaring the type is what moves that into the contract.
        assertThat(request("ServiceRegisterRequest").findFieldByName("profile")
                .getMessageType().getFullName())
                .isEqualTo("ai.pipestream.proto.grpc.profile.v1.ServiceProfile");
    }
}
