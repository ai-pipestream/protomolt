package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelDeliveryMode;
import ai.protomolt.proto.mesh.runtime.v1.ChannelOverflowAction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class PolicyRoutedProcessorChannelTest {

    @TempDir
    Path temporary;

    @Test
    void compiledMemoryAndWalModesHaveTheirDeclaredRestartSemantics() {
        var descriptors = ProcessorChannelFixtures.descriptors();
        var contract = ProcessorChannelFixtures.contract();
        var clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);
        var durablePolicy = ChannelPolicies.localDurable().getPolicy();
        var memoryPolicy = durablePolicy.toBuilder()
                .setDeliveryMode(ChannelDeliveryMode.CHANNEL_DELIVERY_MODE_BOUNDED_MEMORY)
                .setOverflowAction(ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_REFUSE)
                .setMaxItems(2)
                .setMaxBytes(1_000_000)
                .setPersistenceProhibited(true)
                .build();
        var memoryWork = ProcessorChannelFixtures.work(
                UUID.randomUUID().toString(), memoryPolicy, contract);
        var durableWork = ProcessorChannelFixtures.work(
                UUID.randomUUID().toString(), durablePolicy, contract);
        Path wal = temporary.resolve("routed.wal");

        try (var router = new PolicyRoutedProcessorChannel(descriptors,
                new FileDurableProcessorChannel(wal, descriptors, clock), Map.of(), clock)) {
            router.enqueue(memoryWork);
            router.enqueue(durableWork);
            assertThat(router.delivery(memoryWork.getDeliveryId())).isPresent();
            assertThat(router.delivery(durableWork.getDeliveryId())).isPresent();
        }

        try (var router = new PolicyRoutedProcessorChannel(descriptors,
                new FileDurableProcessorChannel(wal, descriptors, clock), Map.of(), clock)) {
            assertThat(router.delivery(memoryWork.getDeliveryId())).isEmpty();
            assertThat(router.delivery(durableWork.getDeliveryId())).isPresent();
        }
    }
}
