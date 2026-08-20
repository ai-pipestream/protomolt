package ai.pipestream.proto.metric.service;

import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import ai.pipestream.proto.metric.spi.MetricMapping;
import java.util.Map;

/**
 * One subject the metric service serves: its built mapping and the engines
 * mounted for it. Built at mount time, so a subject that appears here has
 * already survived every schema check.
 *
 * @param mapping the subject's built metric mapping
 * @param executors the mounted engines, keyed by backend; at least one
 */
public record ServedMetricSubject(
        MetricMapping mapping, Map<MetricBackend, MetricExecutor> executors) {

    public ServedMetricSubject {
        if (mapping == null) {
            throw new IllegalArgumentException("mapping must not be null");
        }
        if (executors == null || executors.isEmpty()) {
            throw new IllegalArgumentException(
                    "at least one executor is required for subject '"
                            + (mapping == null ? "?" : mapping.subject()) + "'");
        }
        executors = Map.copyOf(executors);
    }
}
