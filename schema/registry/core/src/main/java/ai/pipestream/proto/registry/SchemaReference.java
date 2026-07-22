package ai.pipestream.proto.registry;

import java.util.Objects;

/**
 * One entry of a schema's references array, Confluent-style: the {@code name} is the import
 * path the referencing schema uses, {@code subject}/{@code version} say where the referenced
 * schema text lives in the store.
 *
 * @param name    import path used by the referencing schema, e.g. {@code common/v1/core.proto}
 * @param subject subject the referenced schema is registered under
 * @param version version of that subject being referenced (1-based)
 */
public record SchemaReference(String name, String subject, int version) {

    public SchemaReference {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(subject, "subject");
        if (name.isBlank()) {
            throw new IllegalArgumentException("reference name must not be blank");
        }
        if (subject.isBlank()) {
            throw new IllegalArgumentException("reference subject must not be blank");
        }
        if (version < 1) {
            throw new IllegalArgumentException("reference version must be >= 1: " + version);
        }
    }
}
