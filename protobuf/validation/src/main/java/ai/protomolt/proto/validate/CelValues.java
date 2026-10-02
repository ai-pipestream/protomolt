package ai.protomolt.proto.validate;

import ai.protomolt.proto.cel.CelCompilationException;
import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.validate.model.CelConstraint;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static ai.protomolt.proto.validate.WellKnownValues.DURATION_TYPE;
import static ai.protomolt.proto.validate.WellKnownValues.TIMESTAMP_TYPE;
import static ai.protomolt.proto.validate.WellKnownValues.WRAPPER_TYPES;
import static ai.protomolt.proto.validate.WellKnownValues.toInstant;
import static ai.protomolt.proto.validate.WellKnownValues.toJavaDuration;

/**
 * The CEL seam: converts protobuf values to the Java types a CEL program expects, and runs one
 * rule against a converted value.
 *
 * <p>The conversion is what makes {@code this} behave in a rule the way an author reading the
 * {@code .proto} would expect: a {@code uint64} binds as an unsigned value rather than a negative
 * long, an {@code int32} widens to CEL's single integer type, a {@code Timestamp} binds as an
 * {@link Instant}, and a wrapper message binds as the scalar it wraps.</p>
 *
 * <p>Failure classification lives here too, and it matters: a rule whose CEL does not compile is
 * a schema error ({@link RuleCompilationException}), while one that compiles and then fails on a
 * value is a runtime error ({@link RuleEvaluationException}). Neither is a violation.</p>
 */
final class CelValues {

    private CelValues() {
    }

    static void evalCel(
            CelEvaluator evaluator,
            CelConstraint rule,
            Object thisValue,
            String path,
            String rulePath,
            List<ValidationResult.Violation> violations) {
        if (rule.expression().isBlank()) {
            return;
        }
        // With no explicit id protovalidate uses the expression text as the rule id.
        String id = rule.id().isBlank() ? rule.expression() : rule.id();
        try {
            Map<String, Object> bindings = new HashMap<>();
            bindings.put("this", thisValue);
            // protovalidate exposes the current time as `now`; a single value keeps now == now true.
            bindings.put("now", Instant.now());
            if (rule.ruleValue() != null) {
                // Predefined rules see their configured value as `rule`.
                bindings.put("rule", rule.ruleValue());
            }
            Object result = evaluator.evaluateValue(rule.expression(), bindings);
            if (result instanceof Boolean ok) {
                if (!ok) {
                    String msg = rule.message().isBlank()
                            ? "\"" + rule.expression() + "\" returned false" : rule.message();
                    violations.add(new ValidationResult.Violation(path, id, msg, rulePath));
                }
            } else if (result instanceof String text) {
                if (!text.isEmpty()) {
                    violations.add(new ValidationResult.Violation(path, id, text, rulePath));
                }
            } else {
                // Statically bool/string-typed programs never land here; a dyn program returning
                // another type is still a per-value failure.
                violations.add(new ValidationResult.Violation(
                        path, id, "CEL rule must return bool or string", rulePath));
            }
        } catch (CelCompilationException e) {
            // A rule whose CEL does not compile (type error, unknown field) is a compilation error.
            throw new RuleCompilationException(e.getMessage(), e);
        } catch (CelEvaluationException e) {
            // A rule that compiles but fails at evaluation is a runtime error, not a violation.
            throw new RuleEvaluationException(id, "CEL runtime error: " + e.getMessage(), e);
        }
    }

    /** The whole repeated field as a CEL list, each element converted to its CEL Java type. */
    static Object celListValue(Message message, FieldDescriptor field) {
        int count = message.getRepeatedFieldCount(field);
        List<Object> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(celScalar(field, message.getRepeatedField(field, i)));
        }
        return list;
    }

    /** The whole map field as a CEL map, keys and values converted to their CEL Java types. */
    static Object celMapValue(Message message, FieldDescriptor field) {
        Descriptor entryType = field.getMessageType();
        FieldDescriptor keyField = entryType.findFieldByNumber(1);
        FieldDescriptor valueField = entryType.findFieldByNumber(2);
        int count = message.getRepeatedFieldCount(field);
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            Message entry = (Message) message.getRepeatedField(field, i);
            map.put(celScalar(keyField, entry.getField(keyField)),
                    celScalar(valueField, entry.getField(valueField)));
        }
        return map;
    }

    /** Converts a scalar protobuf value to the Java type CEL expects (unsigned for uint types). */
    static Object celScalar(FieldDescriptor field, Object value) {
        return switch (field.getType()) {
            case UINT32, FIXED32 -> UnsignedLong.fromLongBits(Integer.toUnsignedLong((Integer) value));
            case UINT64, FIXED64 -> UnsignedLong.fromLongBits((Long) value);
            case INT32, SINT32, SFIXED32 -> ((Integer) value).longValue();
            case FLOAT -> ((Float) value).doubleValue();
            case ENUM -> (long) ((EnumValueDescriptor) value).getNumber();
            case MESSAGE, GROUP -> celMessage((Message) value);
            default -> value;
        };
    }

    /** Wrapper messages bind as their unwrapped scalar; Timestamp/Duration as temporal values. */
    private static Object celMessage(Message value) {
        Descriptor descriptor = value.getDescriptorForType();
        String type = descriptor.getFullName();
        return switch (type) {
            case TIMESTAMP_TYPE -> toInstant(value);
            case DURATION_TYPE -> toJavaDuration(value);
            default -> {
                if (WRAPPER_TYPES.containsKey(type)) {
                    FieldDescriptor inner = descriptor.findFieldByNumber(1);
                    yield celScalar(inner, value.getField(inner));
                }
                yield value;
            }
        };
    }
}
