package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.index.hints.IndexingHintsProto;
import ai.pipestream.proto.meta.MetadataProto;
import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Descriptor-driven coverage proof for the cluster contract's annotation discipline. This
 * test, not reviewer attention, is what keeps the rules true as the contract evolves.
 */
class ClusterAnnotationCoverageTest {

    /**
     * Messages in cluster.proto that are not persisted contract state and are therefore exempt
     * from the annotation discipline: none today. A new message added to cluster.proto is
     * covered automatically (the set derives from the file descriptor), so it must either
     * carry the full annotation discipline or be consciously exempted here.
     */
    private static final Set<String> NON_PERSISTED_MESSAGES = Set.of();

    /** Every top-level message declared in cluster.proto, minus the explicit exemptions. */
    private static List<Descriptor> persistedMessages() {
        return ClusterDescriptor.getDescriptor().getFile().getMessageTypes().stream()
                .filter(message -> !NON_PERSISTED_MESSAGES.contains(message.getName()))
                .toList();
    }

    /**
     * The exact indexed-field allowlist: the named discovery-query fields and nothing else.
     * A field gaining or losing an index hint fails this test. Endpoint addresses, TLS
     * posture, fingerprints, epochs, sequences, timestamps, and every ClusterEvent field must
     * never appear: the event log is replayed, not searched. Schema names resolve through the
     * reused mesh.v1 SchemaReference, whose type_name is indexed in the core contract.
     */
    private static final Set<String> INDEXED_FIELDS = Set.of(
            "ClusterDescriptor.cluster_id",
            "NodeAdvertisement.node_id",
            "NodeAdvertisement.cluster_id",
            "NodePresence.node_id",
            "NodePresence.cluster_id",
            "NodePresence.state",
            "ProcessorAdvertisement.processor_id",
            "ProcessorAdvertisement.node_id",
            "ProcessorAdvertisement.kind",
            "ProcessorAdvertisement.capabilities",
            "CapacityAdvertisement.node_id");

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
