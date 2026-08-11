package ai.pipestream.proto.mesh;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.mesh.v1.EntityEnvelope;
import ai.pipestream.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.Objects;

/**
 * Proves mesh schema identity: that an {@code Any} type and a canonical descriptor fingerprint
 * identify the same message definition.
 *
 * <p>Resolution binds a {@link SchemaReference} to a live descriptor in two steps. The registry
 * must hold a descriptor under exactly {@code schema.type_name}; then the canonical fingerprint
 * of that descriptor's defining closure (see {@link MeshDigest#fingerprintOf}) must equal
 * {@code schema.descriptor_fingerprint}. A type URL or name alone is never accepted: a same-named
 * type with drifted schema bytes resolves to a different fingerprint and is rejected with both
 * fingerprints in the message.
 */
public final class SchemaIdentityResolver {

    private final DescriptorRegistry registry;

    /**
     * Creates a resolver over the given descriptor registry.
     *
     * @param registry the registry type names resolve against
     */
    public SchemaIdentityResolver(DescriptorRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    /**
     * Resolves a schema reference to the exact message definition it names.
     *
     * @param schema the schema identity to resolve
     * @return the descriptor whose closure fingerprint matches the reference
     * @throws IllegalArgumentException when the type is unknown or the fingerprint does not match
     */
    public Descriptor resolve(SchemaReference schema) {
        MeshValidation.validate(schema);
        Descriptor descriptor = registry.findDescriptorByFullName(schema.getTypeName());
        if (descriptor == null) {
            throw new IllegalArgumentException(
                    "schema.type_name is not a registered type: " + schema.getTypeName());
        }
        String actual = MeshDigest.fingerprintOf(descriptor);
        if (!actual.equals(schema.getDescriptorFingerprint())) {
            throw new IllegalArgumentException(
                    "schema.descriptor_fingerprint mismatch for " + schema.getTypeName()
                            + ": the envelope declares " + schema.getDescriptorFingerprint()
                            + " but the registered schema fingerprints to " + actual);
        }
        return descriptor;
    }

    /**
     * Resolves an entity's body to its exact message definition. The entity must already pass
     * {@link MeshValidation#validateStructure(EntityEnvelope)}; this method re-checks the body's
     * type identity against the schema reference before resolving it. Deadline expiry is not
     * re-judged: schema identity is a structural property, not a temporal one.
     *
     * @param entity the entity whose payload type resolves
     * @return the descriptor the body's bytes deserialize with
     * @throws IllegalArgumentException when the body and schema disagree or resolution fails
     */
    public Descriptor resolve(EntityEnvelope entity) {
        MeshValidation.validateStructure(entity);
        String bodyType;
        if (entity.hasPayload()) {
            bodyType = MeshValidation.typeNameOf(entity.getPayload().getTypeUrl());
        } else if (entity.hasClaimCheck()) {
            bodyType = entity.getClaimCheck().getPayloadTypeName();
        } else {
            throw new IllegalArgumentException(
                    "entity must carry an inline payload or a claim check");
        }
        if (!bodyType.equals(entity.getSchema().getTypeName())) {
            throw new IllegalArgumentException(
                    "entity body names '" + bodyType + "' but schema.type_name is '"
                            + entity.getSchema().getTypeName() + "'");
        }
        return resolve(entity.getSchema());
    }
}
