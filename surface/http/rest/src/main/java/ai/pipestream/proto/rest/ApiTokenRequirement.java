package ai.pipestream.proto.rest;

import java.util.Objects;

/**
 * Runtime description of an API-token requirement (from {@link ProtoApiToken} or hand-built).
 */
public record ApiTokenRequirement(
        String name,
        ProtoApiToken.In in,
        ProtoApiToken.Scheme scheme,
        String httpScheme,
        boolean required,
        String description) {

    public ApiTokenRequirement {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(in, "in");
        Objects.requireNonNull(scheme, "scheme");
        httpScheme = httpScheme == null || httpScheme.isBlank() ? "bearer" : httpScheme;
        description = description == null ? "" : description;
    }

    public static ApiTokenRequirement from(ProtoApiToken annotation) {
        Objects.requireNonNull(annotation, "annotation");
        return new ApiTokenRequirement(
                annotation.name(),
                annotation.in(),
                annotation.scheme(),
                annotation.httpScheme(),
                annotation.required(),
                annotation.description());
    }

    public static ApiTokenRequirement apiKeyHeader(String name) {
        return new ApiTokenRequirement(
                name,
                ProtoApiToken.In.HEADER,
                ProtoApiToken.Scheme.API_KEY,
                "bearer",
                true,
                "API access token");
    }

    public static ApiTokenRequirement bearer() {
        return new ApiTokenRequirement(
                "Authorization",
                ProtoApiToken.In.HEADER,
                ProtoApiToken.Scheme.HTTP,
                "bearer",
                true,
                "Bearer API token");
    }
}
