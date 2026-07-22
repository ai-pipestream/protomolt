package ai.pipestream.proto.kafka.serde;

import java.util.List;

/**
 * What the serde can tell a metrics system, without depending on one.
 *
 * <p>The serde is the choke point every record passes through, and it is already deciding what a
 * data-quality dashboard wants to know: how many records flowed, which ones violated their
 * schema's declared rules and which rules they violated, and how often the registry could not
 * answer. Implementations are discovered with {@link java.util.ServiceLoader} — put one on the
 * classpath and every serde reports to it, no configuration — and the
 * {@code protomolt-serde-micrometer} module ships one that turns these calls into Micrometer
 * counters.</p>
 *
 * <p>Every method has an empty default, so a listener implements only what it wants. Listener
 * failures are contained: an exception thrown here is logged and does not fail the record.</p>
 */
public interface SerdeMetricsListener {

    /** Refused because the message or frame carries a type the serde is not configured for. */
    String REASON_WRONG_TYPE = "wrong-type";
    /** Refused because the type is not declared in the packaged descriptor set. */
    String REASON_NOT_IN_CONTRACT = "not-in-contract";
    /** Refused because an unpinned consumer met a schema id the registry could not resolve. */
    String REASON_UNRESOLVED_ID = "unresolved-id";
    /** Refused because the registry's latest schema cannot read what this producer writes. */
    String REASON_INCOMPATIBLE_WITH_LATEST = "incompatible-with-latest";

    /** A record framed and handed to Kafka. */
    default void onSerialized(String topic, String type) {
    }

    /** A record parsed and handed to the application. */
    default void onDeserialized(String topic, String type) {
    }

    /**
     * A record rejected for violating its schema's declared rules — the data-quality signal.
     *
     * @param write   true when the serializer refused to write it, false when the deserializer
     *                refused to hand it over
     * @param ruleIds the id of every violated rule, e.g. {@code string.min_len}; bounded by the
     *                schema's declared rules
     */
    default void onValidationRejected(String topic, String type, boolean write,
                                      List<String> ruleIds) {
    }

    /**
     * A record refused on type identity rather than data.
     *
     * @param reason one of {@link #REASON_WRONG_TYPE}, {@link #REASON_NOT_IN_CONTRACT},
     *               {@link #REASON_UNRESOLVED_ID}, {@link #REASON_INCOMPATIBLE_WITH_LATEST}
     */
    default void onTypeRefused(String topic, String reason) {
    }

    /**
     * A registry lookup the registry could not answer, with the packaged descriptor set carrying
     * on. Counted per failed lookup, not per record: the retry backoff means an outage produces
     * one of these per window, so a nonzero rate here reads as "the registry is struggling", not
     * as traffic volume.
     */
    default void onRegistryFallback() {
    }

    /**
     * A record measured against the quality dimensions its schema declares
     * ({@code ai.pipestream.proto.quality.v1}). Reported whether or not the record proceeds.
     *
     * @param composite  weighted average of the scored dimensions, in {@code [0, 1]}
     * @param dimensions score per dimension id; ids are the schema's own, so cardinality is
     *                   bounded by what the schema declares
     */
    default void onQualityScored(String topic, String type, double composite,
                                 java.util.Map<String, Double> dimensions) {
    }

    /** A write refused because its composite quality score fell below the configured floor. */
    default void onQualityRejected(String topic, String type, double composite) {
    }
}
