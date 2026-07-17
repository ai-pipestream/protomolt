package ai.pipestream.proto.grpc.validate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The {@link GrpcValidationMetricsListener}s found on the classpath, fanned out behind the same
 * interface. Listeners observe; they do not participate: one that throws is logged once and the
 * call proceeds, and a provider that cannot even load is skipped the same way.
 */
final class GrpcValidationMetricsListeners implements GrpcValidationMetricsListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(GrpcValidationMetricsListeners.class);
    private static final GrpcValidationMetricsListener NO_OP =
            new GrpcValidationMetricsListener() {
            };

    private final List<GrpcValidationMetricsListener> listeners;
    private final AtomicBoolean warnedFailure = new AtomicBoolean();

    private GrpcValidationMetricsListeners(List<GrpcValidationMetricsListener> listeners) {
        this.listeners = listeners;
    }

    static GrpcValidationMetricsListener load(ClassLoader loader) {
        List<GrpcValidationMetricsListener> found = new ArrayList<>();
        var providers = ServiceLoader.load(GrpcValidationMetricsListener.class, loader).iterator();
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
        return found.isEmpty() ? NO_OP : new GrpcValidationMetricsListeners(found);
    }

    @Override
    public void onValidated(String side, String method, String type) {
        each(listener -> listener.onValidated(side, method, type));
    }

    @Override
    public void onRejected(String side, String method, String type, List<String> ruleIds) {
        each(listener -> listener.onRejected(side, method, type, ruleIds));
    }

    @Override
    public void onQualityScored(String method, String type, double composite,
                                Map<String, Double> dimensions) {
        each(listener -> listener.onQualityScored(method, type, composite, dimensions));
    }

    @Override
    public void onQualityRejected(String method, String type, double composite) {
        each(listener -> listener.onQualityRejected(method, type, composite));
    }

    private void each(Consumer<GrpcValidationMetricsListener> event) {
        for (GrpcValidationMetricsListener listener : listeners) {
            try {
                event.accept(listener);
            } catch (RuntimeException | LinkageError e) {
                if (warnedFailure.compareAndSet(false, true)) {
                    LOG.warn("A metrics listener threw and will keep being called; calls are "
                            + "unaffected. This is logged once.", e);
                }
            }
        }
    }
}
