package ai.pipestream.proto.search.index.protobuf;

import ai.pipestream.proto.search.index.spi.AnyPayloadValidator;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Runs the declared-rules validation standard on every payload unpacked from a
 * {@code google.protobuf.Any} during write-time mapping expansion. Registered via
 * {@code ServiceLoader}, so having this module on the classpath turns the gate on — the
 * same chaining {@link ProtobufIndexer} gives the top-level message. The no-argument
 * (ServiceLoader) form uses the shared default validator, so rule compilation is cached
 * per payload type; a host with its own validator — custom rule sources, a mounted
 * taxonomy catalog — binds the gate to it with {@link #DeclaredRulesAnyPayloadValidator(
 * ProtoValidator)}, so packed payloads validate under exactly the rules and mounts the
 * top-level message does.
 *
 * <p>Payload types that declare no rules validate clean at no cost, and the standard's own
 * escape hatches ({@code skip_when}, per-field {@code ignore}) are honored by the
 * validator itself. Violations are reported with their paths prefixed by the Any field's
 * path in the root message.
 */
public final class DeclaredRulesAnyPayloadValidator implements AnyPayloadValidator {

    /** Null means the shared default validator (the ServiceLoader form). */
    private final ProtoValidator validator;

    /** The ServiceLoader form: the shared default validator, empty taxonomy catalog. */
    public DeclaredRulesAnyPayloadValidator() {
        this.validator = null;
    }

    /**
     * Binds the gate to an explicit validator, carrying its rule-source chain and its
     * taxonomy catalog into every packed payload.
     */
    public DeclaredRulesAnyPayloadValidator(ProtoValidator validator) {
        this.validator = Objects.requireNonNull(validator, "validator");
    }

    /**
     * @throws ValidationResult.ValidationException when the payload violates a declared rule
     */
    @Override
    public void validate(Message unpacked, String path) {
        ValidationResult result = validator == null
                ? ValidationResult.validate(unpacked)
                : validator.validate(unpacked);
        if (result.valid()) {
            return;
        }
        List<ValidationResult.Violation> prefixed = new ArrayList<>(result.violations().size());
        for (ValidationResult.Violation violation : result.violations()) {
            prefixed.add(new ValidationResult.Violation(
                    joinPath(path, violation.path()),
                    violation.ruleId(),
                    violation.message(),
                    violation.rulePath()));
        }
        throw new ValidationResult.ValidationException(ValidationResult.failed(prefixed));
    }

    /** A root-level Any has an empty path; either side empty leaves the other unprefixed. */
    private static String joinPath(String anyPath, String violationPath) {
        if (anyPath.isEmpty()) {
            return violationPath;
        }
        return violationPath.isEmpty() ? anyPath : anyPath + "." + violationPath;
    }
}
