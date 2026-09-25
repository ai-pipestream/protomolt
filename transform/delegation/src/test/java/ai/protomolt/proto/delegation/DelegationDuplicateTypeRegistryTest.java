package ai.protomolt.proto.delegation;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Caller-owned descriptor sets may give the same name distinct task-local meanings. */
class DelegationDuplicateTypeRegistryTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String WORKER = "duplicate-type-worker";

    @Test void candidateJsonUsesTheAddressedTasksContractRatherThanTheFirstNamedType()
            throws Exception {
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator();
             DelegationBridge bridge = new DelegationBridge(coordinator)) {
            ActionCatalog catalog = DelegationActions.register(
                    ActionCatalog.defaults(ActionContext.create()), bridge);
            bridge.registerWorker(WorkerHello.newBuilder().setWorkerId(WORKER)
                    .setProtocolVersion(1).setProvider("fixture")
                    .addCapabilities(WorkerCapability.newBuilder().setName("report"))
                    .build());
            String older = UUID.randomUUID().toString();
            String newer = UUID.randomUUID().toString();
            bridge.offer(WORKER, older, spec(DeliverableFixtures.contract()),
                    Duration.ofSeconds(30), null);
            bridge.offer(WORKER, newer, spec(extendedContract()),
                    Duration.ofSeconds(30), null);
            bridge.accept(WORKER, newer, 1);

            CompletionCandidate candidate = CompletionCandidate.newBuilder()
                    .setAttempt(1).setRevision(1).setSummary("extended report")
                    .addEvidence(DelegationFixtures.evidence("unit-tests"))
                    .addCommits(DelegationFixtures.commit("extended"))
                    .build();
            ObjectNode json = (ObjectNode) JSON.readTree(JsonFormat.printer().print(candidate));
            json.set("result", JSON.createObjectNode()
                    .put("@type", "type.googleapis.com/" + DeliverableFixtures.TYPE_NAME)
                    .put("headline", "a headline long enough")
                    .put("findings", 2)
                    .put("evidenceNote", "second task's field"));
            ObjectNode request = JSON.createObjectNode().put("workerId", WORKER)
                    .put("taskId", newer);
            request.set("candidate", json);

            ObjectNode accepted = catalog.execute("delegation-candidate", request);

            assertThat(accepted.path("ok").asBoolean()).isTrue();
            assertThat(coordinator.state().tasks().get(newer).phase())
                    .isEqualTo(DelegationReducer.Phase.CANDIDATE);
        }
    }

    @Test void paginatedHistoryAndWatchUseEachRecordedAttemptsOwnDescriptor()
            throws Exception {
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator();
             DelegationBridge bridge = new DelegationBridge(coordinator)) {
            ActionCatalog catalog = DelegationActions.register(
                    ActionCatalog.defaults(ActionContext.create()), bridge);
            bridge.registerWorker(WorkerHello.newBuilder().setWorkerId(WORKER)
                    .setProtocolVersion(1).setProvider("fixture")
                    .addCapabilities(WorkerCapability.newBuilder().setName("report"))
                    .build());
            String task = UUID.randomUUID().toString();
            bridge.offer(WORKER, task, spec(DeliverableFixtures.contract()),
                    Duration.ofSeconds(30), null);
            bridge.accept(WORKER, task, 1);
            bridge.submitCandidate(WORKER, task,
                    candidate(1, DeliverableFixtures.result("a headline long enough", 2)));
            bridge.cancel(task, "new report contract");
            DeliverableContract extended = extendedContract();
            bridge.offer(WORKER, task, spec(extended), Duration.ofSeconds(30), null);
            bridge.accept(WORKER, task, 2);
            var descriptor = DeliverableContracts.compile(extended).descriptor();
            DynamicMessage report = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("headline"), "a headline long enough")
                    .setField(descriptor.findFieldByName("findings"), 2)
                    .setField(descriptor.findFieldByName("evidence_note"), "new attempt field")
                    .build();
            Any result = Any.newBuilder()
                    .setTypeUrl("type.googleapis.com/" + DeliverableFixtures.TYPE_NAME)
                    .setValue(report.toByteString()).build();
            bridge.submitCandidate(WORKER, task, candidate(2, result));

            long beforeSecondCandidate = coordinator.eventsAfter(task, 0).stream()
                    .filter(event -> event.entry().hasWorkerFrame()
                            && event.entry().getWorkerFrame().hasCompletion()
                            && event.entry().getWorkerFrame().getCompletion().getAttempt() == 1)
                    .findFirst().orElseThrow().cursor();
            ObjectNode page = catalog.execute("delegation-transcript",
                    JSON.createObjectNode().put("taskId", task)
                            .put("afterCursor", beforeSecondCandidate)
                            .put("maxEntries", 20));
            assertThat(page.toString()).contains("new attempt field");
            ObjectNode watch = catalog.execute("delegation-watch",
                    JSON.createObjectNode().put("taskId", task)
                            .put("afterCursor", beforeSecondCandidate)
                            .put("timeoutMs", 0).put("maxEvents", 20));
            assertThat(watch.toString()).contains("new attempt field");

            ObjectNode all = catalog.execute("delegation-transcript",
                    JSON.createObjectNode().put("taskId", task).put("maxEntries", 20));
            assertThat(all.toString()).contains("new attempt field")
                    .contains("a headline long enough");
        }
    }

    private static CompletionCandidate candidate(int attempt, Any result) {
        return CompletionCandidate.newBuilder().setAttempt(attempt).setRevision(1)
                .setSummary("review report")
                .addEvidence(DelegationFixtures.evidence("unit-tests"))
                .addCommits(DelegationFixtures.commit("report-" + attempt))
                .setResult(result).build();
    }

    private static TaskSpec spec(DeliverableContract contract) {
        return DelegationFixtures.spec("unit-tests").toBuilder().setContract(contract).build();
    }

    private static DeliverableContract extendedContract() throws Exception {
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder()
                .mergeFrom(DeliverableFixtures.descriptorSet());
        set.getFileBuilder(0).getMessageTypeBuilder(0).addField(
                FieldDescriptorProto.newBuilder().setName("evidence_note")
                        .setJsonName("evidenceNote").setNumber(3)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING));
        return DeliverableContract.newBuilder().setDescriptorSet(set.build().toByteString())
                .setTypeName(DeliverableFixtures.TYPE_NAME).build();
    }
}
