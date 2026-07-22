package ai.pipestream.proto.rest;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares that the REST/OpenAPI surface requires an API token.
 * Consumed by OpenAPI generation and by framework security glue plugins.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PACKAGE})
public @interface ProtoApiToken {

    /** Security scheme / parameter name (e.g. {@code api_token}, {@code Authorization}). */
    String name() default "api_token";

    /** Where the token is sent. */
    In in() default In.HEADER;

    /** OpenAPI security scheme type. */
    Scheme scheme() default Scheme.API_KEY;

    /** When {@link Scheme#HTTP}, the HTTP auth scheme (usually {@code bearer}). */
    String httpScheme() default "bearer";

    boolean required() default true;

    String description() default "API access token";

    enum In {
        HEADER,
        QUERY,
        COOKIE
    }

    enum Scheme {
        API_KEY,
        HTTP
    }
}
