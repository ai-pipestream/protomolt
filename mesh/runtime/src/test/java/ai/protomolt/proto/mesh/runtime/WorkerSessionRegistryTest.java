package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.DeliveryClaim;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorExecutionGuarantees;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorLeaseBinding;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorWork;
import ai.protomolt.proto.mesh.runtime.v1.WorkerHello;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerSessionRegistryTest {

    private static final Instant NOW = Instant.parse("2026-09-04T00:00:00Z");

    @Test
    void exactSessionResumesOnlyIdempotentClaimsInsideGrace() {
        WorkerSessionRegistry registry = new WorkerSessionRegistry();
        WorkerHello hello = hello(1, "", true);
        var opened = registry.open(hello, Duration.ofSeconds(10), NOW);
        DeliveryClaim claim = claim(hello.getContracts(0));
        registry.claimed(opened.sessionId(), claim);

        assertThat(registry.disconnect(opened.sessionId(), NOW.plusSeconds(1))).isEmpty();
        var resumed = registry.open(hello(1, opened.sessionId(), true),
                Duration.ofSeconds(10), NOW.plusSeconds(5));

        assertThat(resumed.admitted()).isTrue();
        assertThat(resumed.resumed()).isTrue();
        assertThat(resumed.sessionId()).isEqualTo(opened.sessionId());
        assertThat(resumed.claims()).containsKey(claim.getWork().getDeliveryId());
    }

    @Test
    void nonIdempotentClaimReturnsToChannelAtDisconnect() {
        WorkerSessionRegistry registry = new WorkerSessionRegistry();
        WorkerHello hello = hello(1, "", false);
        var opened = registry.open(hello, Duration.ofSeconds(10), NOW);
        DeliveryClaim claim = claim(hello.getContracts(0));
        registry.claimed(opened.sessionId(), claim);

        assertThat(registry.disconnect(opened.sessionId(), NOW.plusSeconds(1)))
                .singleElement()
                .extracting(WorkerSessionRegistry.ClaimRef::leaseToken)
                .isEqualTo(claim.getLeaseToken());
    }

    @Test
    void expiredGraceAndNewerIncarnationFenceOldClaims() {
        WorkerSessionRegistry registry = new WorkerSessionRegistry();
        WorkerHello first = hello(1, "", true);
        var opened = registry.open(first, Duration.ofSeconds(2), NOW);
        DeliveryClaim claim = claim(first.getContracts(0));
        registry.claimed(opened.sessionId(), claim);
        registry.disconnect(opened.sessionId(), NOW);

        assertThat(registry.expire(NOW.plusSeconds(2)))
                .extracting(WorkerSessionRegistry.ClaimRef::deliveryId)
                .containsExactly(claim.getWork().getDeliveryId());
        assertThat(registry.open(hello(1, opened.sessionId(), true),
                Duration.ofSeconds(2), NOW.plusSeconds(2)).admitted()).isFalse();

        var replacement = registry.open(hello(2, "", true),
                Duration.ofSeconds(2), NOW.plusSeconds(3));
        assertThat(replacement.admitted()).isTrue();
        assertThat(replacement.sessionId()).isNotEqualTo(opened.sessionId());
    }

    private static WorkerHello hello(long epoch, String resume, boolean idempotent) {
        ProcessorContract contract = ProcessorContract.newBuilder()
                .setProcessorId("processor")
                .setContractFingerprint("a".repeat(64))
                .setGuarantees(ProcessorExecutionGuarantees.newBuilder()
                        .setIdempotentInvocation(idempotent))
                .build();
        return WorkerHello.newBuilder()
                .setWorkerId("worker")
                .setNodeId("node")
                .setNodeIncarnationEpoch(epoch)
                .setEndpointId("grpc")
                .setResumeSessionId(resume)
                .addContracts(contract)
                .addProcessorLeases(ProcessorLeaseBinding.newBuilder()
                        .setProcessorId("processor")
                        .setLeaseEpoch(1)
                        .setContractFingerprint(contract.getContractFingerprint()))
                .build();
    }

    private static DeliveryClaim claim(ProcessorContract contract) {
        return DeliveryClaim.newBuilder()
                .setWorkerId("worker")
                .setLeaseToken(UUID.randomUUID().toString())
                .setWork(ProcessorWork.newBuilder()
                        .setDeliveryId(UUID.randomUUID().toString())
                        .setContract(contract))
                .build();
    }
}
