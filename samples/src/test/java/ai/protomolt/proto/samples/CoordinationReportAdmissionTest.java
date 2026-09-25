package ai.protomolt.proto.samples;

import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.samples.starter.v1.CoordinationReport;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Ensures caller-defined report rules gate the real candidate admission path. */
class CoordinationReportAdmissionTest {
    private static final String WORKER = "report-fixture-worker";
    private static final String TASK = "8ab31c41-7d31-49d1-96eb-6ab8e944be92";
    private static final String CHECK = "report-check";

    @Test
    void aReportWithAnIncorrectFindingCountCannotEnterReview() {
        var coordinator = new InProcessDelegationCoordinator();
        var bridge = new DelegationBridge(coordinator);
        try {
            bridge.registerWorker(WorkerHello.newBuilder().setWorkerId(WORKER)
                    .setProtocolVersion(1).setProvider("fixture")
                    .addCapabilities(WorkerCapability.newBuilder().setName("report"))
                    .build());
            bridge.offer(WORKER, TASK, specWithReportContract(), Duration.ofMinutes(1), null);
            bridge.accept(WORKER, TASK, 1);

            CompletionCandidate invalidCandidate = CompletionCandidate.newBuilder()
                    .setAttempt(1).setRevision(1)
                    .setSummary("fixture report produced")
                    .addCommits(ai.protomolt.proto.delegation.v1.CommitReference.newBuilder()
                            .setRepository("example.org/repo")
                            .setCommit("a".repeat(40)).setSubject("fixture output"))
                    .addEvidence(CheckEvidence.newBuilder().setCheckName(CHECK)
                            .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                            .setRanAt(Timestamp.newBuilder().setSeconds(1_790_000_000L))
                            .setDetail("fixture check ran"))
                    .setResult(Any.pack(CoordinationReport.newBuilder()
                            .setHeadline("A report with one finding")
                            .addFindings("one finding")
                            .setFindingCount(2)
                            .build()))
                    .build();

            assertThatThrownBy(() -> bridge.submitCandidate(WORKER, TASK, invalidCandidate))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("report-count-matches-findings")
                    .hasMessageContaining("finding_count must equal the number of findings");

            assertThat(coordinator.state().tasks().get(TASK).phase())
                    .isEqualTo(ai.protomolt.proto.delegation.DelegationReducer.Phase.LEASED);
            assertThat(coordinator.transcript().getEntriesList())
                    .noneSatisfy(entry -> assertThat(entry.getWorkerFrame().hasCompletion()).isTrue());
        } finally {
            bridge.close();
            coordinator.close();
        }
    }

    private static TaskSpec specWithReportContract() {
        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        collect(CoordinationReport.getDescriptor().getFile(), closure);
        var descriptorSet = FileDescriptorSet.newBuilder();
        closure.values().forEach(file -> descriptorSet.addFile(file.toProto()));
        var contract = DeliverableContract.newBuilder()
                .setTypeName(CoordinationReport.getDescriptor().getFullName())
                .setDescriptorSet(descriptorSet.build().toByteString());
        return TaskSpec.newBuilder()
                .setObjective("Produce a report whose count matches the findings")
                .addAllowedScope("samples/**")
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName(CHECK)
                        .setDescription("fixture report check"))
                .setContract(contract)
                .build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }
}
