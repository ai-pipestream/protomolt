package ai.pipestream.proto.mesh;

import ai.pipestream.proto.index.hints.IndexingHintsProto;
import ai.pipestream.proto.meta.MetadataProto;
import ai.pipestream.proto.mesh.v1.ClaimCheck;
import ai.pipestream.proto.mesh.v1.EntityEnvelope;
import ai.pipestream.proto.mesh.v1.EntityHeader;
import ai.pipestream.proto.mesh.v1.EntityStatus;
import ai.pipestream.proto.mesh.v1.ProfileReference;
import ai.pipestream.proto.mesh.v1.SchemaReference;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Descriptor-driven coverage proof for the mesh contract's annotation discipline. This test, not
 * reviewer attention, is what keeps the rules true as the contract evolves.
 */
class MeshAnnotationCoverageTest {

    /** The persisted contract messages the annotation discipline covers. */
    private static final List<Descriptor> PERSISTED_MESSAGES = List.of(
            EntityEnvelope.getDescriptor(),
            EntityHeader.getDescriptor(),
            SchemaReference.getDescriptor(),
            ClaimCheck.getDescriptor(),
            ProfileReference.getDescriptor(),
            EntityStatus.getDescriptor());

    /**
     * The exact indexed-field allowlist: the named discovery-query fields and nothing else. A
     * field gaining or losing an index hint fails this test. Payload bytes, claim-check
     * coordinates beyond the type name, posture digests, and correlation ids must never appear.
     */
    private static final Set<String> INDEXED_FIELDS = Set.of(
            "EntityHeader.entity_id",
            "EntityHeader.scope_id",
            "EntityHeader.parent_entity_id",
            "EntityHeader.created_at",
            "SchemaReference.type_name",
            "EntityStatus.state",
            "EntityStatus.processor_id",
            "EntityStatus.terminal_at");

    /**
     * Fields exempt from the meta.v1 sensitivity requirement: none. Every persisted field must
     * declare a sensitivity class. (The options.proto carriers are schema-time option definitions
     * consumed at compile time, not persisted contract fields, so they are out of scope here.)
     */
    private static final Set<String> SENSITIVITY_EXEMPTIONS = Set.of();

    private static String id(Descriptor message, FieldDescriptor field) {
        return message.getName() + "." + field.getName();
    }

    @Test
    void exactlyTheAllowlistedFieldsCarryIndexHints() {
        Set<String> indexed = new TreeSet<>();
        for (Descriptor message : PERSISTED_MESSAGES) {
            for (FieldDescriptor field : message.getFields()) {
                if (field.getOptions().hasExtension(IndexingHintsProto.index)) {
                    indexed.add(id(message, field));
                }
            }
        }
        assertThat(indexed).containsExactlyInAnyOrderElementsOf(INDEXED_FIELDS);
    }

    @Test
    void everyPersistedFieldDeclaresMetaSensitivity() {
        Set<String> missing = new TreeSet<>();
        for (Descriptor message : PERSISTED_MESSAGES) {
            for (FieldDescriptor field : message.getFields()) {
                if (SENSITIVITY_EXEMPTIONS.contains(id(message, field))) {
                    continue;
                }
                String sensitivity = field.getOptions()
                        .getExtension(MetadataProto.field)
                        .getSensitivity();
                if (sensitivity.isEmpty()) {
                    missing.add(id(message, field));
                }
            }
        }
        assertThat(missing).as("persisted fields without meta.v1 sensitivity").isEmpty();
    }
}
