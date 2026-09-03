package ai.protomolt.proto.kafka.serde;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Subject naming, pinned against the strings Confluent's own strategies produce: a deployment's
 * registry already holds schemas under these names, so the serde must ask for exactly them —
 * including the detail that only the topic strategy distinguishes keys from values.
 */
class SubjectsTest {

    @Test
    void topicStrategyAppendsValueOrKey() {
        assertThat(Subjects.of(Subjects.TOPIC, "orders", "acme.orders.v1.Order", false))
                .isEqualTo("orders-value");
        assertThat(Subjects.of(Subjects.TOPIC, "orders", "acme.orders.v1.Order", true))
                .isEqualTo("orders-key");
    }

    /** A record's name is the same subject whether it rides in the key or the value. */
    @Test
    void recordStrategyIgnoresKeyness() {
        assertThat(Subjects.of(Subjects.RECORD, "orders", "acme.orders.v1.Order", false))
                .isEqualTo("acme.orders.v1.Order");
        assertThat(Subjects.of(Subjects.RECORD, "orders", "acme.orders.v1.Order", true))
                .isEqualTo("acme.orders.v1.Order");
    }

    @Test
    void topicRecordStrategyJoinsTopicAndRecord() {
        assertThat(Subjects.of(Subjects.TOPIC_RECORD, "orders", "acme.orders.v1.Order", false))
                .isEqualTo("orders-acme.orders.v1.Order");
        assertThat(Subjects.of(Subjects.TOPIC_RECORD, "orders", "acme.orders.v1.Order", true))
                .isEqualTo("orders-acme.orders.v1.Order");
    }

    @Test
    void unknownStrategiesAreRejected() {
        assertThatThrownBy(() -> Subjects.of("bogus", "orders", "acme.orders.v1.Order", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown subject strategy: bogus");
    }
}
