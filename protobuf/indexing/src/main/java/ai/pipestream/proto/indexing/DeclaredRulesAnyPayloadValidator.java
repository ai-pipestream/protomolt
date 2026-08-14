package ai.pipestream.proto.indexing;

import ai.pipestream.proto.index.spi.AnyPayloadValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs the declared-rules validation standard on every payload unpacked from a
 * {@code google.protobuf.Any} during write-time plan expansion. Registered via
 * {@code ServiceLoader}, so having this module on the classpath turns the gate on — the
 * same chaining {@link ProtobufIndexer} gives the top-level message. The shared default
 * validator is used, so rule compilation is cached per payload type.
 *
 * <p>Payload types that declare no rules validate clean at no cost, and the standard's own
 * escape hatches ({@code skip_when}, per-field {@code ignore}) are honored by the
 * validator itself. Violations are reported with their paths prefixed by the Any field's
 * path in the root message.
 */
public final class DeclaredRulesAnyPayloadValidator implements AnyPayloadValidator {

    /**
     * @throws ValidationResult.ValidationException when the payload violates a declared rule
     */
    @Override
    public void validate(Message unpacked, String path) {
        ValidationResult result = ValidationResult.validate(unpacked);
        if (result.valid()) {
            return;
        }
        List<ValidationResult.Violation> prefixed = new ArrayList<>(result.violations().size());
        for (ValidationResult.Violation violation : result.violations()) {
            prefixed.add(new ValidationResult.Violation(
                    violation.path().isEmpty() ? path : path + "." + violation.path(),
                    violation.ruleId(),
                    violation.message(),
                    violation.rulePath()));
        }
        throw new ValidationResult.ValidationException(ValidationResult.failed(prefixed));
    }
}
