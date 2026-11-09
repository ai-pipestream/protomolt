package ai.protomolt.proto.descriptors;

import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.util.Objects;

/**
 * Resolves the effective {@link StabilityTier} for Java packages, classes, constructors,
 * methods, and fields.
 *
 * <p>Resolution order from most specific to least specific:</p>
 * <ol>
 *   <li>Direct {@link ApiStability} annotation on the member or type.</li>
 *   <li>{@link Deprecated} annotation on the member or type (maps to {@link StabilityTier#DEPRECATED}
 *       unless an explicit {@link StabilityTier#INTERNAL} or {@link StabilityTier#EXPERIMENTAL}
 *       annotation is present).</li>
 *   <li>Declaring class and any enclosing classes from innermost to outermost.</li>
 *   <li>Package-level {@link ApiStability} on {@code package-info.java}.</li>
 *   <li>Package naming convention ({@code .internal.} or {@code .internal} suffix maps to
 *       {@link StabilityTier#INTERNAL}).</li>
 *   <li>Caller-supplied default tier (or {@link StabilityTier#EVOLVING} when unspecified).</li>
 * </ol>
 */
@ApiStability(StabilityTier.STABLE)
public final class ApiStabilityResolver {

    /** Default tier applied to unannotated public Java declarations outside internal packages. */
    public static final StabilityTier DEFAULT_UNANNOTATED_TIER = StabilityTier.EVOLVING;

    private static final String INTERNAL_SEGMENT = ".internal.";
    private static final String INTERNAL_SUFFIX = ".internal";

    private ApiStabilityResolver() {
    }

    /**
     * Resolves the effective stability tier of a Java type using {@link #DEFAULT_UNANNOTATED_TIER}
     * as the fallback when neither the type nor its package is annotated.
     *
     * @param type the class or interface to inspect
     * @return the effective stability tier
     */
    public static StabilityTier resolve(Class<?> type) {
        return resolve(type, DEFAULT_UNANNOTATED_TIER);
    }

    /**
     * Resolves the effective stability tier of a Java type.
     *
     * @param type the class or interface to inspect
     * @param fallback the fallback tier when no annotation or convention matches
     * @return the effective stability tier
     */
    public static StabilityTier resolve(Class<?> type, StabilityTier fallback) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(fallback, "fallback");

        for (Class<?> current = type; current != null; current = current.getEnclosingClass()) {
            ApiStability direct = current.getAnnotation(ApiStability.class);
            if (direct != null) {
                if (direct.value() == StabilityTier.STABLE && current.isAnnotationPresent(Deprecated.class)) {
                    return StabilityTier.DEPRECATED;
                }
                return direct.value();
            }
            if (current.isAnnotationPresent(Deprecated.class)) {
                return StabilityTier.DEPRECATED;
            }
        }

        Package pkg = type.getPackage();
        if (pkg != null) {
            return resolve(pkg, fallback);
        }
        if (isInternalPackageName(type.getPackageName())) {
            return StabilityTier.INTERNAL;
        }
        return fallback;
    }

    /**
     * Resolves the effective stability tier of a method or constructor.
     *
     * @param executable the method or constructor to inspect
     * @return the effective stability tier
     */
    public static StabilityTier resolve(Executable executable) {
        return resolve(executable, DEFAULT_UNANNOTATED_TIER);
    }

    /**
     * Resolves the effective stability tier of a method or constructor.
     *
     * @param executable the method or constructor to inspect
     * @param fallback the fallback tier when no annotation or convention matches
     * @return the effective stability tier
     */
    public static StabilityTier resolve(Executable executable, StabilityTier fallback) {
        Objects.requireNonNull(executable, "executable");
        Objects.requireNonNull(fallback, "fallback");

        ApiStability direct = executable.getAnnotation(ApiStability.class);
        if (direct != null) {
            if (direct.value() == StabilityTier.STABLE && executable.isAnnotationPresent(Deprecated.class)) {
                return StabilityTier.DEPRECATED;
            }
            return direct.value();
        }
        if (executable.isAnnotationPresent(Deprecated.class)) {
            return StabilityTier.DEPRECATED;
        }
        return resolve(executable.getDeclaringClass(), fallback);
    }

    /**
     * Resolves the effective stability tier of a field.
     *
     * @param field the field to inspect
     * @return the effective stability tier
     */
    public static StabilityTier resolve(Field field) {
        return resolve(field, DEFAULT_UNANNOTATED_TIER);
    }

    /**
     * Resolves the effective stability tier of a field.
     *
     * @param field the field to inspect
     * @param fallback the fallback tier when no annotation or convention matches
     * @return the effective stability tier
     */
    public static StabilityTier resolve(Field field, StabilityTier fallback) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(fallback, "fallback");

        ApiStability direct = field.getAnnotation(ApiStability.class);
        if (direct != null) {
            if (direct.value() == StabilityTier.STABLE && field.isAnnotationPresent(Deprecated.class)) {
                return StabilityTier.DEPRECATED;
            }
            return direct.value();
        }
        if (field.isAnnotationPresent(Deprecated.class)) {
            return StabilityTier.DEPRECATED;
        }
        return resolve(field.getDeclaringClass(), fallback);
    }

    /**
     * Resolves the effective stability tier of a Java package.
     *
     * @param pkg the package to inspect
     * @param fallback the fallback tier when no annotation or convention matches
     * @return the effective stability tier
     */
    public static StabilityTier resolve(Package pkg, StabilityTier fallback) {
        Objects.requireNonNull(pkg, "pkg");
        Objects.requireNonNull(fallback, "fallback");

        ApiStability direct = pkg.getAnnotation(ApiStability.class);
        if (direct != null) {
            if (direct.value() == StabilityTier.STABLE && pkg.isAnnotationPresent(Deprecated.class)) {
                return StabilityTier.DEPRECATED;
            }
            return direct.value();
        }
        if (pkg.isAnnotationPresent(Deprecated.class)) {
            return StabilityTier.DEPRECATED;
        }
        if (isInternalPackageName(pkg.getName())) {
            return StabilityTier.INTERNAL;
        }
        return fallback;
    }

    /**
     * Checks whether a package or protobuf namespace name follows the {@code .internal}
     * convention.
     *
     * @param packageName package or proto namespace
     * @return {@code true} if the package is an internal namespace
     */
    public static boolean isInternalPackageName(String packageName) {
        if (packageName == null || packageName.isEmpty()) {
            return false;
        }
        return packageName.equals("internal")
                || packageName.endsWith(INTERNAL_SUFFIX)
                || packageName.contains(INTERNAL_SEGMENT);
    }
}
