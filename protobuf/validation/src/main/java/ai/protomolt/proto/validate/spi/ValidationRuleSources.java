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
     * <p>The thread context class loader is used for {@link ServiceLoader} discovery when it
     * resolves the same {@link ValidationRuleSource} type; otherwise discovery falls back to
     * {@code ValidationRuleSource.class.getClassLoader()} so isolated child-first or
     * platform-parented class loaders do not fail with {@link java.util.ServiceConfigurationError}
     * when the outer thread context class loader was left unchanged.</p>
     *
     * <p>Ordering only affects the order violations are reported in; every source is
     * evaluated and all violations are merged.
     */
    public static List<ValidationRuleSource> defaults() {
        List<ValidationRuleSource> sources = new ArrayList<>();
        sources.add(new ProtomoltRuleSource());
        sources.add(new GoogleTypeRuleSource());
        for (ValidationRuleSource discovered
                : ServiceLoader.load(ValidationRuleSource.class, resolveServiceClassLoader())) {
            boolean known = sources.stream()
                    .anyMatch(s -> s.getClass().equals(discovered.getClass()));
            if (!known) {
                sources.add(discovered);
            }
        }
        return List.copyOf(sources);
    }

    private static ClassLoader resolveServiceClassLoader() {
        ClassLoader fallback = ValidationRuleSource.class.getClassLoader();
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl == null) {
            return fallback;
        }
        try {
            if (tccl.loadClass(ValidationRuleSource.class.getName()) != ValidationRuleSource.class) {
                return fallback;
            }
            return tccl;
        } catch (ClassNotFoundException e) {
            return fallback;
        }
    }

    /** Just the built-in Pipestream reader, ignoring any {@link ServiceLoader} extensions. */
    public static List<ValidationRuleSource> protomoltOnly() {
        return List.of(new ProtomoltRuleSource());
    }
}
