package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ProcessorFailure;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class JdbcTransactionalProcessorChannelTest {

    @Container
    static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:18-alpine");

    @Test
    void stateAndOutboxTransitionsSurviveAChannelRestart() {
        var dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        var descriptors = ProcessorChannelFixtures.descriptors();
        var contract = ProcessorChannelFixtures.contract();
        var policy = ChannelPolicies.localDurable().getPolicy();
        var work = ProcessorChannelFixtures.work(
                UUID.randomUUID().toString(), policy, contract);
        String channelId = "channel-" + UUID.randomUUID();
        String firstCompletionId = UUID.randomUUID().toString();
        String finalCompletionId = UUID.randomUUID().toString();
        String finalLease;
        int durableRecordCount;
        Clock clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);

        try (var channel = new JdbcTransactionalProcessorChannel(
                dataSource, channelId, descriptors, clock)) {
            channel.enqueue(work);
            var first = channel.claim("worker-a", List.of(contract), 1,
                    Duration.ofSeconds(30), ProcessorChannelFixtures.NOW).getFirst();
            var failure = ProcessorFailure.newBuilder()
                    .setDeliveryId(work.getDeliveryId())
                    .setLeaseToken(first.getLeaseToken())
                    .setCompletionId(firstCompletionId)
                    .setCode("transient")
                    .setMessage("try again")
                    .setOutcome(ProcessorOutcomes.retryable(
                            "transient", "try again", contract.getProcessorId(), 1, 3))
                    .build();
            channel.fail("worker-a", failure, ProcessorChannelFixtures.NOW);
            channel.fail("worker-a", failure, ProcessorChannelFixtures.NOW);

            assertThat(channel.delivery(work.getDeliveryId()).orElseThrow().state())
                    .isEqualTo(DurableProcessorChannel.DeliveryState.PENDING);
            var second = channel.claim("worker-b", List.of(contract), 1,
                    Duration.ofSeconds(30), ProcessorChannelFixtures.NOW).getFirst();
            finalLease = second.getLeaseToken();
            var completion = ProcessorChannelFixtures.completion(
                    work, finalLease, finalCompletionId);
            channel.complete("worker-b", completion, ProcessorChannelFixtures.NOW);
            channel.complete("worker-b", completion, ProcessorChannelFixtures.NOW);
            channel.settle(work.getDeliveryId(), finalLease, ProcessorChannelFixtures.NOW);
            channel.settle(work.getDeliveryId(), finalLease, ProcessorChannelFixtures.NOW);
            durableRecordCount = channel.records().size();

            var outbox = channel.claimOutbox("relay-a", 100,
                    Duration.ofSeconds(30), ProcessorChannelFixtures.NOW);
            assertThat(outbox).hasSize(durableRecordCount);
            assertThat(outbox).allSatisfy(entry ->
                    assertThat(entry.protobufRecord()).isNotEmpty());
            var retry = outbox.getFirst();
            channel.releaseOutbox(retry.eventId(), "relay-a", "broker unavailable");
            assertThat(channel.claimOutbox("relay-b", 1,
                    Duration.ofSeconds(30), ProcessorChannelFixtures.NOW))
                    .singleElement()
                    .satisfies(entry -> {
                        assertThat(entry.eventId()).isEqualTo(retry.eventId());
                        assertThat(entry.attempts()).isEqualTo(2);
                        channel.markOutboxPublished(entry.eventId(), "relay-b");
                    });
            outbox.stream().skip(1).forEach(entry ->
                    channel.markOutboxPublished(entry.eventId(), "relay-a"));
        }

        try (var recovered = new JdbcTransactionalProcessorChannel(
                dataSource, channelId, descriptors, clock)) {
            var view = recovered.delivery(work.getDeliveryId()).orElseThrow();
            assertThat(view.state()).isEqualTo(DurableProcessorChannel.DeliveryState.SETTLED);
            assertThat(view.attempt()).isEqualTo(2);
            assertThat(recovered.records()).hasSize(durableRecordCount);
            assertThat(recovered.claimOutbox("relay-c", 10,
                    Duration.ofSeconds(30), ProcessorChannelFixtures.NOW)).isEmpty();
            assertThatThrownBy(() -> recovered.enqueue(work.toBuilder()
                    .setNodeId("different").build()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("conflicting work");
        }
    }

    @Test
    void fencesASecondWriterUntilTheFirstCloses() {
        var dataSource = dataSource();
        String channelId = "writer-" + UUID.randomUUID();
        Clock clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);

        try (var first = new JdbcTransactionalProcessorChannel(
                dataSource, channelId, ProcessorChannelFixtures.descriptors(), clock)) {
            assertThatThrownBy(() -> new JdbcTransactionalProcessorChannel(
                    dataSource, channelId, ProcessorChannelFixtures.descriptors(), clock))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("transactional-channel-writer-fenced");
        }

        try (var replacement = new JdbcTransactionalProcessorChannel(
                dataSource, channelId, ProcessorChannelFixtures.descriptors(), clock)) {
            assertThat(replacement.records()).isEmpty();
        }
    }

    @Test
    void boundedRelayRetriesWithTheSameBrokerIdempotencyKey() {
        var dataSource = dataSource();
        String channelId = "relay-" + UUID.randomUUID();
        Clock clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);
        var attempts = new ArrayList<UUID>();
        var publishedBodies = new ArrayList<byte[]>();

        try (var channel = new JdbcTransactionalProcessorChannel(
                dataSource, channelId, ProcessorChannelFixtures.descriptors(), clock)) {
            var work = ProcessorChannelFixtures.work(UUID.randomUUID().toString(),
                    ChannelPolicies.localDurable().getPolicy(),
                    ProcessorChannelFixtures.contract());
            channel.enqueue(work);
            var relay = new TransactionalOutboxRelay(channel,
                    (eventId, sequence, body) -> {
                        attempts.add(eventId);
                        if (attempts.size() == 1) {
                            throw new IllegalStateException("broker unavailable");
                        }
                        publishedBodies.add(body);
                    }, "relay", 1, Duration.ofSeconds(5), clock);

            assertThat(relay.runOnce()).isEqualTo(
                    new TransactionalOutboxRelay.BatchResult(1, 0, 1));
            assertThat(relay.runOnce()).isEqualTo(
                    new TransactionalOutboxRelay.BatchResult(1, 1, 0));
            assertThat(attempts).hasSize(2);
            assertThat(attempts.get(0)).isEqualTo(attempts.get(1));
            assertThat(publishedBodies).hasSize(1);
            assertThat(publishedBodies.getFirst()).isNotEmpty();
            assertThat(relay.runOnce()).isEqualTo(
                    new TransactionalOutboxRelay.BatchResult(0, 0, 0));
        }
    }

    @Test
    void transactionalAdapterUsesTheSharedChannelStateMachine() {
        var dataSource = dataSource();
        Clock clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);
        try (var channel = new JdbcTransactionalProcessorChannel(dataSource,
                "conformance-" + UUID.randomUUID(),
                ProcessorChannelFixtures.descriptors(), clock)) {
            assertThat(ProcessorChannelConformanceTest.transitions(channel)).containsExactly(
                    ai.protomolt.proto.mesh.runtime.v1.ChannelRecord.EventCase.ENQUEUED,
                    ai.protomolt.proto.mesh.runtime.v1.ChannelRecord.EventCase.CLAIMED,
                    ai.protomolt.proto.mesh.runtime.v1.ChannelRecord.EventCase.RELEASED,
                    ai.protomolt.proto.mesh.runtime.v1.ChannelRecord.EventCase.CLAIMED,
                    ai.protomolt.proto.mesh.runtime.v1.ChannelRecord.EventCase.COMPLETED,
                    ai.protomolt.proto.mesh.runtime.v1.ChannelRecord.EventCase.SETTLED);
        }
    }

    private static PGSimpleDataSource dataSource() {
        var dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }
}
