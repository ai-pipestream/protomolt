package ai.protomolt.proto.descriptors;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares the stability tier of a Java package, type, method, constructor, or field.
 *
 * <p>Retained at runtime so bytecode and reflection-based compatibility gates can inspect
 * the effective stability of every exported declaration without parsing source files.</p>
 *
 * <p>When a member does not carry {@code @ApiStability} directly, {@link ApiStabilityResolver}
 * inherits the annotation from its declaring type, enclosing types, or package.</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({
        ElementType.PACKAGE,
        ElementType.TYPE,
        ElementType.METHOD,
        ElementType.CONSTRUCTOR,
        ElementType.FIELD
})
@ApiStability(StabilityTier.STABLE)
public @interface ApiStability {

    /**
     * The stability tier for the annotated element.
     *
     * @return the declared stability tier (defaults to {@link StabilityTier#STABLE})
     */
    StabilityTier value() default StabilityTier.STABLE;

    /**
     * Optional version or milestone identifier when this stability tier took effect.
     *
     * @return version string, or empty if unspecified
     */
    String since() default "";

    /**
     * Optional context or migration note for callers and compatibility reports.
     *
     * @return human-readable note, or empty if unspecified
     */
    String notes() default "";
}
