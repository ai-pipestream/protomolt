package ai.protomolt.proto.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.metric.QueryMetricsRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The consumer contract: a valid document applies atomically with its
 * version as evidence, a document failing its type's own declared rules
 * refuses and the old config keeps serving, unchanged versions skip,
 * absence never un-applies, and neither a broken payload, a broken
 * source, nor a broken listener disturbs what a node already runs.
 */
class DistributedConfigTest {

    /** A source over a map, with a throw switch. */
    static final class FakeSource implements ConfigSource {
        final Map<String, Fetched> documents = new HashMap<>();
        boolean broken;

        @Override
        public Optional<Fetched> fetch(String subject) throws Exception {
            if (broken) {
                throw new IllegalStateException("source is down");
            }
            return Optional.ofNullable(documents.get(subject));
        }
    }

    static QueryMetricsRequest document(int limit) {
        return QueryMetricsRequest.newBuilder()
                .setMappingSubject("orders")
                .addMeasures("revenue")
                .setLimit(limit)
                .build();
    }

    @Test
    void aValidDocumentAppliesWithItsVersionAsEvidence() {
        FakeSource source = new FakeSource();
        source.documents.put("metrics-defaults",
                new ConfigSource.Fetched("v1", document(10).toByteArray()));
        List<String> seen = new ArrayList<>();
        try (DistributedConfig config = DistributedConfig.over(source)) {
            DistributedConfig.Subscription<QueryMetricsRequest> subscription =
                    config.subscribe("metrics-defaults",
                            QueryMetricsRequest.getDefaultInstance());
            subscription.onChange((applied, version) ->
                    seen.add(version + ":" + applied.getLimit()));
            assertThat(subscription.current()).isEmpty();

            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(outcome.applied()).containsExactly("metrics-defaults");
            assertThat(outcome.refused()).isEmpty();
            assertThat(subscription.current()).isPresent();
            assertThat(subscription.current().orElseThrow().version()).isEqualTo("v1");
            assertThat(subscription.current().orElseThrow().config().getLimit())
                    .isEqualTo(10);
            assertThat(seen).containsExactly("v1:10");
        }
    }

    @Test
    void aDocumentFailingItsOwnRulesRefusesAndTheOldConfigKeepsServing() {
        FakeSource source = new FakeSource();
        source.documents.put("metrics-defaults",
                new ConfigSource.Fetched("v1", document(10).toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            DistributedConfig.Subscription<QueryMetricsRequest> subscription =
                    config.subscribe("metrics-defaults",
                            QueryMetricsRequest.getDefaultInstance());
            config.refresh();

            // limit 0 violates the type's own declared gt-zero rule.
            source.documents.put("metrics-defaults",
                    new ConfigSource.Fetched("v2", document(0).toByteArray()));
            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(outcome.applied()).isEmpty();
            assertThat(outcome.refused()).hasSize(1);
            DistributedConfig.Refusal refusal = outcome.refused().get(0);
            assertThat(refusal.subject()).isEqualTo("metrics-defaults");
            assertThat(refusal.version()).isEqualTo("v2");
            assertThat(refusal.reason()).contains("limit");
            assertThat(subscription.current().orElseThrow().version())
                    .as("the old config keeps serving")
                    .isEqualTo("v1");
        }
    }

    @Test
    void unchangedVersionsSkipAndAbsenceNeverUnapplies() {
        FakeSource source = new FakeSource();
        source.documents.put("metrics-defaults",
                new ConfigSource.Fetched("v1", document(10).toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            DistributedConfig.Subscription<QueryMetricsRequest> subscription =
                    config.subscribe("metrics-defaults",
                            QueryMetricsRequest.getDefaultInstance());
            config.refresh();

            DistributedConfig.RefreshOutcome again = config.refresh();
            assertThat(again.unchanged()).isEqualTo(1);
            assertThat(again.applied()).isEmpty();

            source.documents.clear();
            DistributedConfig.RefreshOutcome gone = config.refresh();
            assertThat(gone.absent()).containsExactly("metrics-defaults");
            assertThat(subscription.current().orElseThrow().version())
                    .as("a gap in the source is not a removal")
                    .isEqualTo("v1");
        }
    }

    @Test
    void brokenPayloadsSourcesAndListenersNeverDisturbTheCurrentConfig() {
        FakeSource source = new FakeSource();
        source.documents.put("metrics-defaults",
                new ConfigSource.Fetched("v1", document(10).toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            DistributedConfig.Subscription<QueryMetricsRequest> subscription =
                    config.subscribe("metrics-defaults",
                            QueryMetricsRequest.getDefaultInstance());
            List<String> secondListener = new ArrayList<>();
            subscription.onChange((applied, version) -> {
                throw new IllegalStateException("bad listener");
            });
            subscription.onChange((applied, version) -> secondListener.add(version));

            DistributedConfig.RefreshOutcome first = config.refresh();
            assertThat(first.applied()).containsExactly("metrics-defaults");
            assertThat(secondListener)
                    .as("a throwing listener never poisons the others")
                    .containsExactly("v1");

            source.documents.put("metrics-defaults", new ConfigSource.Fetched(
                    "v2", new byte[] {(byte) 0xFF, (byte) 0xFF, 1, 2, 3}));
            DistributedConfig.RefreshOutcome garbage = config.refresh();
            assertThat(garbage.refused().get(0).reason())
                    .contains("QueryMetricsRequest");
            assertThat(subscription.current().orElseThrow().version()).isEqualTo("v1");

            source.broken = true;
            DistributedConfig.RefreshOutcome down = config.refresh();
            assertThat(down.refused().get(0).reason()).contains("fetch failed");
            assertThat(subscription.current().orElseThrow().version()).isEqualTo("v1");
        }
    }

    @Test
    void duplicateSubjectsAndBlankInputsRefuse() {
        try (DistributedConfig config = DistributedConfig.over(new FakeSource())) {
            config.subscribe("one", QueryMetricsRequest.getDefaultInstance());
            assertThatThrownBy(() ->
                    config.subscribe("one", QueryMetricsRequest.getDefaultInstance()))
                    .hasMessageContaining("already subscribed");
            assertThatThrownBy(() ->
                    config.subscribe(" ", QueryMetricsRequest.getDefaultInstance()))
                    .hasMessageContaining("subject");
        }
    }
}
