package ai.protomolt.proto.validate.spi;

import ai.protomolt.proto.validate.model.FieldConstraints;
import ai.protomolt.proto.validate.model.MessageConstraints;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.Optional;

/**
 * Reads a particular constraint-annotation dialect off protobuf descriptors and
 * translates it into the neutral {@link FieldConstraints}/{@link MessageConstraints}
 * model. The validator core evaluates only that neutral model, so it stays free of any
 * one dialect's option types.
 *
 * <p>This is the extension seam for constraint compatibility. The built-in
 * {@link ai.protomolt.proto.validate.source.ProtomoltRuleSource} reads the
 * Pipestream {@code validate.v1} options. Additional dialects (for example a
 * {@code buf.validate} reader in a separate, optional module) implement this interface
 * and are picked up automatically via {@link java.util.ServiceLoader} — see
 * {@link ValidationRuleSources#defaults()}. Removing such a module removes its dialect
 * with no change to the core.
 *
 * <p>Implementations must be thread-safe and side-effect free: descriptors are shared.
 */
public interface ValidationRuleSource {

    /**
     * Returns the constraints this dialect declares on {@code field}, or empty when the
     * field carries no annotation from this dialect.
     */
    Optional<FieldConstraints> fieldConstraints(FieldDescriptor field);

    /**
     * Returns the message-level constraints this dialect declares on {@code message}, or
     * empty when the message carries no annotation from this dialect.
     */
    Optional<MessageConstraints> messageConstraints(Descriptor message);

    /** Stable identifier for diagnostics and de-duplication. Defaults to the class name. */
    default String sourceId() {
        return getClass().getSimpleName();
    }
}
