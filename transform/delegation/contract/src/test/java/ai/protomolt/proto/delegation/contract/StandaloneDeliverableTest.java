package ai.protomolt.proto.delegation.contract;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises descriptor-carried rules without any coordinator implementation. */
class StandaloneDeliverableTest {
    private DeliverableContract contract() {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(WorkerHello.getDescriptor().getFile(), files);
        var set = FileDescriptorSet.newBuilder();
        files.values().forEach(file -> set.addFile(file.toProto()));
        return DeliverableContract.newBuilder().setDescriptorSet(set.build().toByteString())
                .setTypeName(WorkerHello.getDescriptor().getFullName()).build();
    }

    private void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.putIfAbsent(file.getName(), file) == null) {
            file.getDependencies().forEach(dependency -> collect(dependency, files));
        }
    }

    @Test
    void linksTransmittedDescriptorsAndRejectsInvalidPayload() {
        var valid = WorkerHello.newBuilder().setWorkerId("worker-1")
                .setProvider("test").setProtocolVersion(1).build();
        assertThat(DeliverableContracts.check(contract(), Any.pack(valid))).isEmpty();
        assertThat(DeliverableContracts.check(contract(), Any.pack(
                valid.toBuilder().setProtocolVersion(0).build()))).isNotEmpty();
        assertThat(DeliverableContracts.check(contract(), Any.pack(
                com.google.protobuf.Timestamp.getDefaultInstance()))).isNotEmpty();
    }

    @Test
    void refusesUnknownContractType() {
        assertThatThrownBy(() -> DeliverableContracts.compile(contract().toBuilder()
                .setTypeName("missing.v1.Unknown").build()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
