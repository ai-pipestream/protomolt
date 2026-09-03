package ai.protomolt.proto.validate.spi;

import ai.protomolt.proto.validate.source.ProtomoltRuleSource;
import ai.protomolt.proto.validate.source.GoogleTypeRuleSource;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/** Factory helpers for assembling the {@link ValidationRuleSource} chain a validator uses. */
public final class ValidationRuleSources {

    private ValidationRuleSources() {
    }

    /**
     * The default source chain: the built-in Pipestream reader first, then the
     * {@code google.type} well-known constraints, followed by any additional readers
     * registered via {@link ServiceLoader} (for example an optional {@code buf.validate}
     * module dropped on the classpath). Duplicate implementation classes are ignored so
     * a built-in reader is never added twice.
     *
     * <p>Ordering only affects the order violations are reported in; every source is
     * evaluated and all violations are merged.
     */
    public static List<ValidationRuleSource> defaults() {
        List<ValidationRuleSource> sources = new ArrayList<>();
        sources.add(new ProtomoltRuleSource());
        sources.add(new GoogleTypeRuleSource());
        for (ValidationRuleSource discovered : ServiceLoader.load(ValidationRuleSource.class)) {
            boolean known = sources.stream()
                    .anyMatch(s -> s.getClass().equals(discovered.getClass()));
            if (!known) {
                sources.add(discovered);
            }
        }
        return List.copyOf(sources);
    }

    /** Just the built-in Pipestream reader, ignoring any {@link ServiceLoader} extensions. */
    public static List<ValidationRuleSource> protomoltOnly() {
        return List.of(new ProtomoltRuleSource());
    }
}
