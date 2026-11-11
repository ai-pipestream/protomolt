package ai.protomolt.proto.http.rest;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.StabilityTier;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a gRPC service or method as exposed over the JSON/REST gateway.
 * Framework glue (Quarkus/Spring/Micronaut) discovers these and registers routes.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@ApiStability(StabilityTier.STABLE)
public @interface ProtoRestExposed {

    /**
     * On a type: renames the service segment of the {@code {service}/{method}} route.
     * On a method: rejected at registration — hosts route only the canonical form, and a
     * path the router does not serve must not exist in the contract.
     */
    String path() default "";

    /**
     * HTTP methods allowed. Empty (the default) inherits from a type-level
     * {@code @ProtoRestExposed}; if neither level sets any, POST is used
     * (matching Micronaut grpc-json).
     */
    String[] httpMethods() default {};

    /** Human-readable summary for OpenAPI. */
    String summary() default "";

    /** Longer description for OpenAPI. */
    String description() default "";

    /**
     * Optional stability tier override for the exposed REST route (or default tier for methods
     * on an annotated service class). Empty (the default) means unset and inherits from
     * {@link ApiStability}, enclosing declarations, or {@link StabilityTier#STABLE}. At most
     * one tier may be specified; single-element syntax such as
     * {@code @ProtoRestExposed(stability = StabilityTier.STABLE)} is supported.
     */
    StabilityTier[] stability() default {};
}
