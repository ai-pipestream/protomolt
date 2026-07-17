package ai.pipestream.proto.grpc.validate;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.RuleCompilationException;
import ai.pipestream.proto.validate.RuleEvaluationException;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The client-side half: outbound request messages are validated against their schema's declared
 * rules before they leave the process, and an invalid one fails the call locally with
 * {@link Status#INVALID_ARGUMENT} — no network round-trip to be told what the descriptor in hand
 * already knew. Register with {@code ManagedChannelBuilder.intercept(...)} or per stub.
 *
 * <p>Failing locally is the point: the violation reads exactly as the server-side interceptor
 * would report it, so a client sees the same contract whether or not the server enforces it —
 * and a fleet of clients validating before sending is a fleet that never pays a network hop for
 * an answer it was carrying.</p>
 */
public final class ValidatingClientInterceptor implements ClientInterceptor {

    private final ProtoValidator validator;

    private ValidatingClientInterceptor(ProtoValidator validator) {
        this.validator = validator;
    }

    /** Validation with the default rule-source chain. */
    public static ValidatingClientInterceptor create() {
        return new ValidatingClientInterceptor(ProtoValidator.create());
    }

    /** As {@link #create()} with a custom validator. */
    public static ValidatingClientInterceptor create(ProtoValidator validator) {
        return new ValidatingClientInterceptor(Objects.requireNonNull(validator, "validator"));
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {

            @Override
            public void sendMessage(ReqT message) {
                if (message instanceof Message proto) {
                    Status refusal = judge(proto);
                    if (refusal != null) {
                        // Cancel the underlying call, then fail the caller: the message never
                        // reaches the wire, and the caller sees the same status a validating
                        // server would have sent back.
                        cancel(refusal.getDescription(), refusal.asRuntimeException());
                        throw refusal.asRuntimeException();
                    }
                }
                super.sendMessage(message);
            }
        };
    }

    /** The refusal status for {@code message}, or null when it may be sent. */
    private Status judge(Message message) {
        ValidationResult result;
        try {
            result = validator.validate(message);
        } catch (RuleEvaluationException e) {
            return Status.INVALID_ARGUMENT.withDescription(
                    "Request could not be validated: " + e.getMessage());
        } catch (RuleCompilationException e) {
            // The client's own schema is malformed; surface it as the local bug it is.
            throw e;
        }
        if (!result.valid()) {
            return Status.INVALID_ARGUMENT.withDescription(
                    "Request violates the schema's declared rules: " + describe(result));
        }
        return null;
    }

    private static String describe(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + " " + violation.message()
                        + " (" + violation.ruleId() + ")")
                .collect(Collectors.joining("; "));
    }
}
