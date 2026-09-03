package ai.protomolt.proto.mesh;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import com.google.protobuf.Descriptors.Descriptor;

import java.time.Instant;
import java.util.Objects;

/**
 * The single admission point of the mesh contract gate. A routing, persistence, or
 * processor-execution boundary admits an entity through {@link #admit}, which performs every
 * layer together, so no caller can accidentally run only a subset:
 *
 * <ol>
 *   <li>structural validation: well-known-type timestamp validity, the contract's validate.v1
 *       annotations (field bounds and message CEL), payload digest and length against the exact
 *       bytes, and inline type-URL shape and agreement with the schema
 *       ({@link MeshValidation#validateStructure});</li>
 *   <li>deadline validation: expiry against the caller's clock
 *       ({@link MeshValidation#validate(EntityEnvelope, Instant)});</li>
 *   <li>schema-identity resolution: the registry must hold the named type and the canonical
 *       closure fingerprint must match ({@link SchemaIdentityResolver#resolve}).</li>
 * </ol>
 *
 * <p>Every failure throws {@link IllegalArgumentException} before anything routes, persists, or
 * executes. On success the caller receives the exact descriptor the body's bytes deserialize
 * with, which is what a transform or processor boundary needs next.
 */
public final class MeshGate {

    private final SchemaIdentityResolver resolver;

    /**
     * Creates the gate over the descriptor registry schema identity resolves against.
     *
     * @param registry the registry payload type names resolve against
     */
    public MeshGate(DescriptorRegistry registry) {
        this.resolver = new SchemaIdentityResolver(Objects.requireNonNull(registry, "registry"));
    }

    /**
     * Admits an entity against the wall clock. Prefer {@link #admit(EntityEnvelope, Instant)}
     * where a clock is injectable.
     *
     * @param entity the entity to admit
     * @return the descriptor the body's bytes deserialize with
     */
    public Descriptor admit(EntityEnvelope entity) {
        return admit(entity, Instant.now());
    }

    /**
     * Admits an entity: structural validation, deadline validation against {@code now}, and
     * schema-identity resolution, in one call.
     *
     * @param entity the entity to admit
     * @param now the instant deadline expiry is judged against
     * @return the descriptor the body's bytes deserialize with
     * @throws IllegalArgumentException when any layer rejects the entity
     */
    public Descriptor admit(EntityEnvelope entity, Instant now) {
        MeshValidation.validate(entity, now);
        // Schema-only resolution: the envelope is already fully validated, and the structural
        // layer proved the body's type identity agrees with this schema reference.
        return resolver.resolve(entity.getSchema());
    }
}
