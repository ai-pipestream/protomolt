package ai.protomolt.proto.mesh;

import ai.protomolt.proto.search.index.hints.IndexingHintsProto;
import ai.protomolt.proto.meta.MetadataProto;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
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

    /**
     * Messages in entity.proto that are not persisted contract state and are therefore exempt
     * from the annotation discipline: none today. A new message added to entity.proto is
     * covered automatically (the set derives from the file descriptor), so it must either carry
     * the full annotation discipline or be consciously exempted here. (The options.proto
     * carriers are schema-time option definitions in a separate file, so they never enter the
     * derived set.)
     */
    private static final Set<String> NON_PERSISTED_MESSAGES = Set.of();

    /** Every top-level message declared in entity.proto, minus the explicit exemptions. */
    private static List<Descriptor> persistedMessages() {
        return EntityEnvelope.getDescriptor().getFile().getMessageTypes().stream()
                .filter(message -> !NON_PERSISTED_MESSAGES.contains(message.getName()))
                .toList();
    }

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
     * declare a sensitivity class.
     */
    private static final Set<String> SENSITIVITY_EXEMPTIONS = Set.of();

    private static String id(Descriptor message, FieldDescriptor field) {
        return message.getName() + "." + field.getName();
    }

    @Test
    void exactlyTheAllowlistedFieldsCarryIndexHints() {
        Set<String> indexed = new TreeSet<>();
        for (Descriptor message : persistedMessages()) {
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
        for (Descriptor message : persistedMessages()) {
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
