package ai.pipestream.proto.grpc.validate;

import ai.pipestream.proto.quality.QualityReport;
import ai.pipestream.proto.quality.QualityScorer;
import ai.pipestream.proto.quality.QualitySchemaException;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.RuleCompilationException;
import ai.pipestream.proto.validate.RuleEvaluationException;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Message;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Enforces the schema's own declared rules at the gRPC boundary: every inbound request message —
 * unary or streamed — is validated before the handler sees it, and a message that violates its
 * contract is refused with {@link Status#INVALID_ARGUMENT} naming every violation. The same
 * guarantee the ProtoMolt Kafka serde gives a topic, given to a service: validation stops being
 * something each handler remembers to call, because it stops being the handler's code path.
 *
 * <p>Register it like any interceptor — {@code ServerBuilder.intercept(...)} server-wide, or
 * {@code ServerInterceptors.intercept(service, ...)} per service. Messages that are not protobuf
 * pass through untouched.</p>
 *
 * <p>Status mapping is deliberate. Rule violations are the caller's problem:
 * {@code INVALID_ARGUMENT}, with the violation list in the description. A rule that fails to
 * evaluate against a particular value (undecodable bytes, say) is also the caller's data:
 * {@code INVALID_ARGUMENT}. A rule that cannot even compile is the server's schema problem and
 * no caller can fix it: {@code INTERNAL}, with the detail kept out of the wire description.</p>
 *
 * <p>Messages are also measured against the {@code ai.pipestream.proto.quality.v1} dimensions
 * their schema declares — types declaring none cost one option read ever. Scores go to the
 * metrics listeners and the {@link Builder#onQuality} callback, and a
 * {@link Builder#qualityFloor} turns the measurement into admission criteria
 * ({@code FAILED_PRECONDITION} below the floor).</p>
 *
 * <p>Everything the interceptors decide is observable: {@link GrpcValidationMetricsListener}
 * implementations found via {@link java.util.ServiceLoader} hear every validation, rejection
 * and quality score, and {@code protomolt-grpc-validation-micrometer} ships the Micrometer
 * binding.</p>
 */
public final class ValidatingServerInterceptor implements ServerInterceptor {

    private final ProtoValidator validator;
    private final QualityScorer quality;
    private final Double qualityFloor;
    private final BiConsumer<String, QualityReport> onQuality;
    private final GrpcValidationMetricsListener metrics;

    private ValidatingServerInterceptor(Builder builder) {
        this.validator = builder.validator != null ? builder.validator : ProtoValidator.create();
        this.metrics = GrpcValidationMetricsListeners.load(getClass().getClassLoader());
        // Always measuring matches the serde's default: types declaring no dimensions cost one
        // option read ever, and scores flow to the metrics listeners without opting in.
        this.quality = QualityScorer.create();
        this.qualityFloor = builder.qualityFloor;
        this.onQuality = builder.onQuality;
    }

    /**
     * The default rule-source chain, with quality measured and reported to the metrics listeners
     * but no floor: no message is refused for its score.
     */
    public static ValidatingServerInterceptor create() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private ProtoValidator validator;
        private Double qualityFloor;
        private BiConsumer<String, QualityReport> onQuality;

        private Builder() {
        }

        /** A validator with a custom rule-source chain; defaults to {@link ProtoValidator#create()}. */
        public Builder validator(ProtoValidator validator) {
            this.validator = Objects.requireNonNull(validator, "validator");
            return this;
        }

        /** Refuse requests whose composite quality score falls below this ({@code 0..1}). */
        public Builder qualityFloor(double floor) {
            if (floor < 0 || floor > 1) {
                throw new IllegalArgumentException("qualityFloor must be within [0, 1], got "
                        + floor);
            }
            this.qualityFloor = floor;
            return this;
        }

        /** Receives every quality measurement, keyed by the call's full method name. */
        public Builder onQuality(BiConsumer<String, QualityReport> onQuality) {
            this.onQuality = Objects.requireNonNull(onQuality, "onQuality");
            return this;
        }

        public ValidatingServerInterceptor build() {
            return new ValidatingServerInterceptor(this);
        }
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        return new SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {

            /** Once refused, later listener events must not reach the handler. */
            private boolean refused;

            @Override
            public void onMessage(ReqT message) {
                if (refused) {
                    return;
                }
                if (message instanceof Message proto) {
                    Status refusal = judge(proto, call.getMethodDescriptor().getFullMethodName());
                    if (refusal != null) {
                        refused = true;
                        call.close(refusal, new Metadata());
                        return;
                    }
                }
                super.onMessage(message);
            }

            @Override
            public void onHalfClose() {
                if (!refused) {
                    super.onHalfClose();
                }
            }
        };
    }

    /** The refusal status for {@code message}, or null when it may proceed. */
    private Status judge(Message message, String fullMethodName) {
        String type = message.getDescriptorForType().getFullName();
        ValidationResult result;
        try {
            result = validator.validate(message);
        } catch (RuleEvaluationException e) {
            // Value-dependent: this particular message broke a rule's evaluation.
            return Status.INVALID_ARGUMENT.withDescription(
                    "Request could not be validated: " + e.getMessage());
        } catch (RuleCompilationException e) {
            // The schema's rules are malformed; no caller can fix that, and the detail
            // belongs in server logs, not on the wire.
            return Status.INTERNAL.withDescription(
                    "The service's schema rules are malformed").withCause(e);
        }
        if (!result.valid()) {
            metrics.onRejected(GrpcValidationMetricsListener.SIDE_SERVER, fullMethodName, type,
                    result.violations().stream()
                            .map(ValidationResult.Violation::ruleId).toList());
            return Status.INVALID_ARGUMENT.withDescription(
                    "Request violates the schema's declared rules: " + describe(result));
        }
        QualityReport report;
        try {
            report = quality.score(message);
        } catch (QualitySchemaException e) {
            return Status.INTERNAL.withDescription(
                    "The service's quality dimensions are malformed").withCause(e);
        }
        if (report.scored()) {
            metrics.onQualityScored(fullMethodName, type, report.composite(),
                    report.dimensions());
            if (onQuality != null) {
                onQuality.accept(fullMethodName, report);
            }
            if (qualityFloor != null && report.composite() < qualityFloor) {
                metrics.onQualityRejected(fullMethodName, type, report.composite());
                return Status.FAILED_PRECONDITION.withDescription(String.format(
                        "Request scored %.3f against its schema's quality dimensions, below the "
                                + "floor of %.3f: %s",
                        report.composite(), qualityFloor, report.dimensions()));
            }
        }
        metrics.onValidated(GrpcValidationMetricsListener.SIDE_SERVER, fullMethodName, type);
        return null;
    }

    /** Every violation, not just the first: a caller fixing them one at a time is a slow loop. */
    private static String describe(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + " " + violation.message()
                        + " (" + violation.ruleId() + ")")
                .collect(Collectors.joining("; "));
    }
}
