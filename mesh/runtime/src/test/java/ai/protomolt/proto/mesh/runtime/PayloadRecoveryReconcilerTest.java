package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.v1.DeadLetterReplayStatus;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import ai.protomolt.proto.mesh.runtime.v1.ReconcileRecoveryRequest;
import ai.protomolt.proto.mesh.runtime.v1.ReconciliationClassification;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.google.protobuf.util.Timestamps;

import static org.assertj.core.api.Assertions.assertThat;

class PayloadRecoveryReconcilerTest {

    private static final String NAMESPACE = "a2afcaf1-e96f-4519-a20d-49530ac905a5";
    private static final String PROFILE = "recovery-payloads";

    @TempDir
    Path temporary;

    @Test
    void reportsUnknownBeforeRepairAndNeverDeletesOnMissingEvidence() {
        Clock clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);
        InMemoryPayloadStore store = new InMemoryPayloadStore(clock, 10, 1_000_000);
        var descriptors = ProcessorChannelFixtures.descriptors();
        var contract = ProcessorChannelFixtures.contract();
        var policy = ChannelPolicies.localDurable().getPolicy().toBuilder()
                .setInlineByteLimit(1)
                .setPayloadStoreProfile(PROFILE)
                .build();
        var inline = EntityEnvelopes.root(UUID.randomUUID().toString(), NAMESPACE,
                RawInput.newBuilder().setText("dead-letter payload").build(),
                ProcessorChannelFixtures.NOW,
                ProcessorChannelFixtures.NOW.plusSeconds(600),
                CompletionPolicy.COMPLETION_POLICY_STRICT);
        var externalized = new PayloadExternalizer(store).externalize(
                inline, policy, NAMESPACE, "flow-owner",
                ProcessorChannelFixtures.NOW.plusSeconds(300));

        try (var channel = new FileDurableProcessorChannel(
                temporary.resolve("reconcile.wal"), descriptors, clock)) {
            var work = ProcessorChannelFixtures.work(
                    UUID.randomUUID().toString(), policy, contract).toBuilder()
                    .setInput(externalized.envelope())
                    .setNamespace(NAMESPACE)
                    .setPayloadStoreProfile(PROFILE)
                    .build();
            channel.enqueue(work);
            var claim = channel.claim("worker-a", List.of(contract), 1,
                    java.time.Duration.ofSeconds(30), ProcessorChannelFixtures.NOW)
                    .getFirst();
            channel.fail("worker-a", ProcessorFailure.newBuilder()
                    .setDeliveryId(work.getDeliveryId())
                    .setLeaseToken(claim.getLeaseToken())
                    .setCompletionId(UUID.randomUUID().toString())
                    .setCode("poison")
                    .setMessage("poison")
                    .setOutcome(ProcessorOutcomes.permanent(
                            "poison", "poison", contract.getProcessorId(), 1))
                    .build(), ProcessorChannelFixtures.NOW);
            var record = channel.deadLetters(NAMESPACE, 0, 10).entries()
                    .getFirst().record();
            var request = ReconcileRecoveryRequest.newBuilder()
                    .setNamespace(NAMESPACE).build();

            var unavailable = new PayloadRecoveryReconciler(channel, Map.of())
                    .reconcile(request, 10).getFindings(0);
            assertThat(unavailable.getClassification()).isEqualTo(
                    ReconciliationClassification.RECONCILIATION_CLASSIFICATION_UNKNOWN);

            var missingStore = new InMemoryPayloadStore(clock, 10, 1_000_000);
            var missing = new PayloadRecoveryReconciler(
                    channel, Map.of(PROFILE, missingStore))
                    .reconcile(request, 10).getFindings(0);
            assertThat(missing.getClassification()).isEqualTo(
                    ReconciliationClassification.RECONCILIATION_CLASSIFICATION_MISSING);

            PayloadRecoveryReconciler reconciler = new PayloadRecoveryReconciler(
                    channel, Map.of(PROFILE, store));
            assertThat(reconciler.reconcile(request, 10).getFindings(0)
                    .getClassification()).isEqualTo(
                    ReconciliationClassification.RECONCILIATION_CLASSIFICATION_CONSISTENT);

            record = channel.changeDeadLetterStatus(record.getDeadLetterId(),
                    DeadLetterReplayStatus.DEAD_LETTER_REPLAY_STATUS_ACKNOWLEDGED,
                    "", "retain", true);
            var beforeRepair = reconciler.reconcile(request, 10).getFindings(0);
            assertThat(beforeRepair.getClassification()).isEqualTo(
                    ReconciliationClassification.RECONCILIATION_CLASSIFICATION_UNKNOWN);
            assertThat(beforeRepair.getRepaired()).isFalse();

            var tooRecent = reconciler.reconcile(request.toBuilder()
                            .setRepair(true)
                            .setNotBefore(Timestamps.fromMillis(
                                    ProcessorChannelFixtures.NOW.minusSeconds(1).toEpochMilli()))
                            .build(), 10)
                    .getFindings(0);
            assertThat(tooRecent.getClassification()).isEqualTo(
                    ReconciliationClassification.RECONCILIATION_CLASSIFICATION_UNKNOWN);
            assertThat(tooRecent.getRepaired()).isFalse();
            assertThat(tooRecent.getEvidence()).contains("age guard");

            var repaired = reconciler.reconcile(request.toBuilder()
                            .setRepair(true)
                            .setNotBefore(Timestamps.fromMillis(
                                    ProcessorChannelFixtures.NOW.plusSeconds(1).toEpochMilli()))
                            .build(), 10)
                    .getFindings(0);
            assertThat(repaired.getClassification()).isEqualTo(
                    ReconciliationClassification.RECONCILIATION_CLASSIFICATION_CONSISTENT);
            assertThat(repaired.getRepaired()).isTrue();
            assertThat(store.head(externalized.lease().getIdentity()).getRetentionHold())
                    .isTrue();

            store.hold(externalized.lease().getIdentity(), false);
            store.release(externalized.lease().getIdentity(),
                    externalized.lease().getOwnerId(), externalized.lease().getLeaseId());
            store.markEligible(externalized.lease().getIdentity(),
                    ProcessorChannelFixtures.NOW, "", "");
            store.purge(externalized.lease().getIdentity(), "test purge",
                    ProcessorChannelFixtures.NOW.plusSeconds(1));
            assertThat(reconciler.reconcile(request, 10).getFindings(0)
                    .getClassification()).isEqualTo(
                    ReconciliationClassification.RECONCILIATION_CLASSIFICATION_MISSING);
            assertThat(record.getRetentionHold()).isTrue();
        }
    }
}
