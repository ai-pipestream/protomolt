package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.storage.v1.ClusterEventLog;
import ai.pipestream.proto.meta.MetadataProto;
import ai.pipestream.proto.validate.ValidationResult;
import org.junit.jupiter.api.Test;

import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterEventLogContractTest {

    @Test
    void validLogPassesAnnotationsAndReplayValidation() {
        ClusterDirectory directory = new ClusterDirectory(ClusterFixtures.cluster(),
                new ClusterFixtures.MutableClock(ClusterFixtures.T0));
        directory.register(ClusterFixtures.node("node-1"));
        ClusterEventLog eventLog = ClusterEventLog.newBuilder()
                .setClusterId(ClusterFixtures.CLUSTER_ID)
                .setClusterFingerprint(ClusterFixtures.cluster().getFingerprint())
                .addAllEvents(directory.events())
                .build();

        assertThat(ValidationResult.validate(eventLog).valid()).isTrue();
        ClusterValidation.validateEventLog(eventLog.getEventsList());
    }

    @Test
    void identityFieldsAreValidated() {
        ClusterEventLog invalid = ClusterEventLog.newBuilder()
                .setClusterId("bad cluster id")
                .setClusterFingerprint("not-a-fingerprint")
                .build();

        ValidationResult result = ValidationResult.validate(invalid);

        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .extracting(ValidationResult.Violation::path)
                .contains("cluster_id", "cluster_fingerprint");
    }

    @Test
    void everyStoredFieldDeclaresSensitivityAndContractImportsNoIndexOptions() {
        var descriptor = ClusterEventLog.getDescriptor();
        TreeSet<String> missing = new TreeSet<>();
        descriptor.getFields().forEach(field -> {
            if (field.getOptions().getExtension(MetadataProto.field)
                    .getSensitivity().isEmpty()) {
                missing.add(field.getName());
            }
        });

        assertThat(missing).isEmpty();
        assertThat(descriptor.getFile().getDependencies())
                .extracting(dependency -> dependency.getName())
                .noneMatch(name -> name.contains("index"));
    }
}
