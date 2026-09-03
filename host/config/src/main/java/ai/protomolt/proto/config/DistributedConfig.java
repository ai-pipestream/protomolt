package ai.protomolt.proto.config;

import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The consumer side of config distribution: typed subscriptions over one
 * {@link ConfigSource}, applied verify-then-swap. Each refresh fetches
 * every subscribed subject, parses the payload strictly as the declared
 * type, enforces the type's own declared validate.v1 rules — the same
 * enforcement the wire boundaries mount, applied before anything applies —
 * and only then swaps the current config atomically and notifies
 * listeners. A document that fails any step is refused with the reason
 * and the node keeps serving the config it already has: exact or
 * refused, never a half-applied change.
 *
 * <p>There is no timer here and no coordination: the host owns the
 * refresh cadence (the reader-refresh idiom), the writer is whoever
 * publishes to the source, and every node is just a reader of its
 * subjects. The applied version rides the outcome and the subscription,
 * so a node can always say which config it runs.</p>
 */
public final class DistributedConfig implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedConfig.class);

    private final ConfigSource source;
    private final ProtoValidator validator = ProtoValidator.create();
    private final Map<String, Subscription<?>> subscriptions = new LinkedHashMap<>();

    private DistributedConfig(ConfigSource source) {
        this.source = source;
    }

    /**
     * Creates the consumer over one source.
     *
     * @param source where documents come from
     * @return the consumer, with no subscriptions yet
     */
    public static DistributedConfig over(ConfigSource source) {
        if (source == null) {
            throw new IllegalArgumentException("source must not be null");
        }
        return new DistributedConfig(source);
    }

    /**
     * Subscribes one subject as one message type. The subscription is
     * empty until a refresh applies a valid document.
     *
     * @param subject the config subject to read
     * @param prototype any instance of the expected type (its default
     *        instance works); only the type is used
     * @param <T> the config document's type
     * @return the subscription
     */
    public synchronized <T extends Message> Subscription<T> subscribe(
            String subject, T prototype) {
        if (subject == null || subject.isBlank()) {
            throw new IllegalArgumentException("subject must not be blank");
        }
        if (prototype == null) {
            throw new IllegalArgumentException("prototype must not be null");
        }
        if (subscriptions.containsKey(subject)) {
            throw new IllegalArgumentException(
                    "subject '" + subject + "' is already subscribed");
        }
        Subscription<T> subscription = new Subscription<>(subject, prototype);
        subscriptions.put(subject, subscription);
        return subscription;
    }

    /**
     * One pull over every subscription: fetch, parse, validate, swap.
     * Failures never partially apply and never disturb another subject's
     * config; the outcome says exactly what happened to each subject.
     *
     * @return what this refresh applied, refused, skipped, and missed
     */
    public synchronized RefreshOutcome refresh() {
        List<String> applied = new ArrayList<>();
        List<Refusal> refused = new ArrayList<>();
        List<String> absent = new ArrayList<>();
        int unchanged = 0;
        for (Subscription<?> subscription : subscriptions.values()) {
            switch (refreshOne(subscription, refused)) {
                case APPLIED -> applied.add(subscription.subject());
                case UNCHANGED -> unchanged++;
                case ABSENT -> absent.add(subscription.subject());
                case REFUSED -> {
                }
            }
        }
        return new RefreshOutcome(List.copyOf(applied), List.copyOf(refused),
                unchanged, List.copyOf(absent));
    }

    private enum One { APPLIED, UNCHANGED, ABSENT, REFUSED }

    private <T extends Message> One refreshOne(
            Subscription<T> subscription, List<Refusal> refused) {
        Optional<ConfigSource.Fetched> fetched;
        try {
            fetched = source.fetch(subscription.subject());
        } catch (Exception e) {
            refused.add(new Refusal(subscription.subject(), "",
                    "fetch failed: " + e.getMessage()));
            LOG.warn("config fetch of '{}' failed; the current config keeps serving",
                    subscription.subject(), e);
            return One.REFUSED;
        }
        if (fetched.isEmpty()) {
            // Absence is not removal: a config is replaced by a newer
            // document, never silently by a gap in the source.
            return One.ABSENT;
        }
        Optional<Applied<T>> current = subscription.current();
        if (current.isPresent() && current.get().version().equals(fetched.get().version())) {
            return One.UNCHANGED;
        }
        T parsed;
        try {
            @SuppressWarnings("unchecked")
            T message = (T) subscription.prototype().getParserForType()
                    .parseFrom(fetched.get().payload());
            parsed = message;
        } catch (InvalidProtocolBufferException e) {
            refused.add(new Refusal(subscription.subject(), fetched.get().version(),
                    "payload is not a " + subscription.prototype()
                            .getDescriptorForType().getFullName() + ": " + e.getMessage()));
            LOG.warn("config '{}' version {} refused: unparsable; the current config"
                    + " keeps serving", subscription.subject(), fetched.get().version());
            return One.REFUSED;
        }
        ValidationResult validated = validator.validate(parsed);
        if (!validated.valid()) {
            String reasons = validated.violations().stream()
                    .map(violation -> violation.path() + ": " + violation.message())
                    .collect(Collectors.joining("; "));
            refused.add(new Refusal(
                    subscription.subject(), fetched.get().version(), reasons));
            LOG.warn("config '{}' version {} refused by its own declared rules ({});"
                    + " the current config keeps serving",
                    subscription.subject(), fetched.get().version(), reasons);
            return One.REFUSED;
        }
        subscription.apply(new Applied<>(parsed, fetched.get().version()));
        LOG.info("config '{}' applied at version {}",
                subscription.subject(), fetched.get().version());
        return One.APPLIED;
    }

    @Override
    public void close() {
        source.close();
    }

    /** One subject's typed subscription. */
    public static final class Subscription<T extends Message> {

        private final String subject;
        private final T prototype;
        private final AtomicReference<Applied<T>> current = new AtomicReference<>();
        private final List<BiConsumer<T, String>> listeners = new ArrayList<>();

        private Subscription(String subject, T prototype) {
            this.subject = subject;
            this.prototype = prototype;
        }

        /** The subscribed subject. */
        public String subject() {
            return subject;
        }

        T prototype() {
            return prototype;
        }

        /** The currently applied config, or empty before the first apply. */
        public Optional<Applied<T>> current() {
            return Optional.ofNullable(current.get());
        }

        /**
         * Registers a change listener, called after each successful swap
         * with the new config and its version. A listener that throws is
         * logged and never poisons the refresh or the other listeners.
         *
         * @param listener the change consumer
         */
        public synchronized void onChange(BiConsumer<T, String> listener) {
            if (listener == null) {
                throw new IllegalArgumentException("listener must not be null");
            }
            listeners.add(listener);
        }

        private synchronized void apply(Applied<T> applied) {
            current.set(applied);
            for (BiConsumer<T, String> listener : listeners) {
                try {
                    listener.accept(applied.config(), applied.version());
                } catch (RuntimeException e) {
                    LOG.warn("config listener for '{}' failed at version {}",
                            subject, applied.version(), e);
                }
            }
        }
    }

    /**
     * One applied config document.
     *
     * @param config the validated config
     * @param version the source's version of it, the evidence a node can
     *        print for which config it runs
     * @param <T> the config document's type
     */
    public record Applied<T extends Message>(T config, String version) {
    }

    /**
     * What one refresh did.
     *
     * @param applied subjects whose config changed this refresh
     * @param refused documents that failed and never applied
     * @param unchanged subjects already at the source's version
     * @param absent subjects the source has no document for
     */
    public record RefreshOutcome(
            List<String> applied, List<Refusal> refused, int unchanged,
            List<String> absent) {
    }

    /**
     * One refused document: the node keeps serving its current config.
     *
     * @param subject the config subject
     * @param version the refused document's version; empty when the fetch
     *        itself failed
     * @param reason what was wrong, naming the violations
     */
    public record Refusal(String subject, String version, String reason) {
    }
}
