package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelOverflowAction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedMemoryProcessorChannelTest {

    @TempDir
    Path temporary;

    @Test
    void refusesRatherThanExceedingItsDeclaredItemBudget() {
        var descriptors = ProcessorChannelFixtures.descriptors();
        var policy = ChannelPolicies.localDurable().getPolicy();
        var first = ProcessorChannelFixtures.work(
                UUID.randomUUID().toString(), policy,
                ProcessorChannelFixtures.contract());
        var second = ProcessorChannelFixtures.work(
                UUID.randomUUID().toString(), policy,
                ProcessorChannelFixtures.contract());
        try (var channel = new BoundedMemoryProcessorChannel(
                descriptors, 1, 1_000_000,
                ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_REFUSE,
                null, Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC))) {
            channel.enqueue(first);
            assertThat(channel.reservedItems()).isEqualTo(1);
            assertThatThrownBy(() -> channel.enqueue(second))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("memory-channel-full");
        }
    }

    @Test
    void usesOnlyTheNamedDurableSpillAndThatWorkSurvivesRestart() {
        var descriptors = ProcessorChannelFixtures.descriptors();
        var contract = ProcessorChannelFixtures.contract();
        var policy = ChannelPolicies.localDurable().getPolicy();
        var first = ProcessorChannelFixtures.work(
                UUID.randomUUID().toString(), policy, contract);
        var spilled = ProcessorChannelFixtures.work(
                UUID.randomUUID().toString(), policy, contract);
        Path wal = temporary.resolve("spill.wal");
        try (var durable = new FileDurableProcessorChannel(wal, descriptors,
                     Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC));
             var channel = new BoundedMemoryProcessorChannel(
                     descriptors, 1, 1_000_000,
                     ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_DURABLE_SPILL,
                     durable, Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC))) {
            channel.enqueue(first);
            channel.enqueue(spilled);
            assertThat(durable.delivery(spilled.getDeliveryId())).isPresent();
        }
        try (var recovered = new FileDurableProcessorChannel(wal, descriptors,
                Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC))) {
            assertThat(recovered.delivery(spilled.getDeliveryId())).isPresent();
            assertThat(recovered.delivery(first.getDeliveryId())).isEmpty();
        }
    }
}
