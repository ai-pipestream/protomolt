package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelOverflowAction;
import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessorChannelConformanceTest {

    @TempDir
    Path temporary;

    @Test
    void memoryAndWalApplyTheSameStateMachine() {
        var descriptors = ProcessorChannelFixtures.descriptors();
        Clock clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);
        List<DurableProcessorChannel> channels = new ArrayList<>();
        channels.add(new BoundedMemoryProcessorChannel(descriptors, 10, 1_000_000,
                ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_REFUSE, null, clock));
        channels.add(new FileDurableProcessorChannel(
                temporary.resolve("conformance.wal"), descriptors, clock));
        try {
            for (DurableProcessorChannel channel : channels) {
                assertThat(transitions(channel)).containsExactly(
                        ChannelRecord.EventCase.ENQUEUED,
                        ChannelRecord.EventCase.CLAIMED,
                        ChannelRecord.EventCase.RELEASED,
                        ChannelRecord.EventCase.CLAIMED,
                        ChannelRecord.EventCase.COMPLETED,
                        ChannelRecord.EventCase.SETTLED);
            }
        } finally {
            channels.forEach(DurableProcessorChannel::close);
        }
    }

    static List<ChannelRecord.EventCase> transitions(DurableProcessorChannel channel) {
        var contract = ProcessorChannelFixtures.contract();
        var work = ProcessorChannelFixtures.work(UUID.randomUUID().toString(),
                ChannelPolicies.localDurable().getPolicy(), contract);
        channel.enqueue(work);
        var first = channel.claim("worker-a", List.of(contract), 1,
                Duration.ofSeconds(30), ProcessorChannelFixtures.NOW).getFirst();
        channel.fail("worker-a", ProcessorFailure.newBuilder()
                .setDeliveryId(work.getDeliveryId())
                .setLeaseToken(first.getLeaseToken())
                .setCompletionId(UUID.randomUUID().toString())
                .setCode("transient")
                .setMessage("retry")
                .setOutcome(ProcessorOutcomes.retryable(
                        "transient", "retry", contract.getProcessorId(), 1, 3))
                .build(), ProcessorChannelFixtures.NOW);
        var second = channel.claim("worker-b", List.of(contract), 1,
                Duration.ofSeconds(30), ProcessorChannelFixtures.NOW).getFirst();
        channel.complete("worker-b", ProcessorChannelFixtures.completion(
                work, second.getLeaseToken(), UUID.randomUUID().toString()),
                ProcessorChannelFixtures.NOW);
        channel.settle(work.getDeliveryId(), second.getLeaseToken(),
                ProcessorChannelFixtures.NOW);
        assertThat(channel.delivery(work.getDeliveryId()).orElseThrow().state())
                .isEqualTo(DurableProcessorChannel.DeliveryState.SETTLED);
        return channel.records().stream().map(ChannelRecord::getEventCase).toList();
    }
}
