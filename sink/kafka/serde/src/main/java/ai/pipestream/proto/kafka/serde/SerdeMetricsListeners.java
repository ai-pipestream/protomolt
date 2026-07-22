package ai.pipestream.proto.kafka.serde;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link SerdeMetricsListener}s found on the classpath, fanned out behind the same interface.
 *
 * <p>Listeners observe; they do not participate. A listener that throws is logged once and the
 * record proceeds — a metrics system that can take a serde down is worse than no metrics — and a
 * provider that cannot even load (its dependencies absent, say) is skipped the same way.</p>
 */
final class SerdeMetricsListeners implements SerdeMetricsListener {

    private static final Logger LOG = LoggerFactory.getLogger(SerdeMetricsListeners.class);
    private static final SerdeMetricsListener NO_OP = new SerdeMetricsListener() {
    };

    private final List<SerdeMetricsListener> listeners;
    private final AtomicBoolean warnedFailure = new AtomicBoolean();

    private SerdeMetricsListeners(List<SerdeMetricsListener> listeners) {
        this.listeners = listeners;
    }

    static SerdeMetricsListener load(ClassLoader loader) {
        List<SerdeMetricsListener> found = new ArrayList<>();
        var providers = ServiceLoader.load(SerdeMetricsListener.class, loader).iterator();
        while (true) {
            try {
                if (!providers.hasNext()) {
                    break;
                }
                found.add(providers.next());
            } catch (java.util.ServiceConfigurationError e) {
                LOG.warn("Skipping a metrics listener that could not load: {}", e.getMessage());
            }
        }
        return found.isEmpty() ? NO_OP : new SerdeMetricsListeners(found);
    }

    @Override
    public void onSerialized(String topic, String type) {
        each(listener -> listener.onSerialized(topic, type));
    }

    @Override
    public void onDeserialized(String topic, String type) {
        each(listener -> listener.onDeserialized(topic, type));
    }

    @Override
    public void onValidationRejected(String topic, String type, boolean write,
                                     List<String> ruleIds) {
        each(listener -> listener.onValidationRejected(topic, type, write, ruleIds));
    }

    @Override
    public void onTypeRefused(String topic, String reason) {
        each(listener -> listener.onTypeRefused(topic, reason));
    }

    @Override
    public void onRegistryFallback() {
        each(SerdeMetricsListener::onRegistryFallback);
    }

    @Override
    public void onQualityScored(String topic, String type, double composite,
                                java.util.Map<String, Double> dimensions) {
        each(listener -> listener.onQualityScored(topic, type, composite, dimensions));
    }

    @Override
    public void onQualityRejected(String topic, String type, double composite) {
        each(listener -> listener.onQualityRejected(topic, type, composite));
    }

    private void each(java.util.function.Consumer<SerdeMetricsListener> event) {
        for (SerdeMetricsListener listener : listeners) {
            try {
                event.accept(listener);
            } catch (RuntimeException | LinkageError e) {
                if (warnedFailure.compareAndSet(false, true)) {
                    LOG.warn("A metrics listener threw and will keep being called; records are "
                            + "unaffected. This is logged once.", e);
                }
            }
        }
    }
}
