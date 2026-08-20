package ai.pipestream.proto.metric.spi;

import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MessageMetric;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Programmatic metric declarations for schemas that cannot be annotated:
 * third-party descriptors, inferred schemas. Field declarations key on
 * {@code <messageFullName>.<fieldName>} first and the bare field name as a
 * fallback, the same lookup the indexing catalog makes.
 */
public final class CatalogMetricHintSource implements MetricHintSource {

    private final Map<String, FieldMetric> fields = new ConcurrentHashMap<>();
    private final Map<String, MessageMetric> messages = new ConcurrentHashMap<>();

    /** Declares a field metric under a bare field name or qualified key. */
    public CatalogMetricHintSource put(String key, FieldMetric metric) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        if (metric == null) {
            throw new IllegalArgumentException("metric must not be null");
        }
        fields.put(key, metric);
        return this;
    }

    /** Declares a field metric for one message's field. */
    public CatalogMetricHintSource put(String messageFullName, String fieldName, FieldMetric metric) {
        return put(messageFullName + "." + fieldName, metric);
    }

    /** Declares a message metric for one message type. */
    public CatalogMetricHintSource putMessage(String messageFullName, MessageMetric metric) {
        if (messageFullName == null || messageFullName.isBlank()) {
            throw new IllegalArgumentException("messageFullName must not be blank");
        }
        if (metric == null) {
            throw new IllegalArgumentException("metric must not be null");
        }
        messages.put(messageFullName, metric);
        return this;
    }

    @Override
    public Optional<FieldMetric> field(FieldDescriptor field) {
        FieldMetric qualified = fields.get(
                field.getContainingType().getFullName() + "." + field.getName());
        if (qualified != null) {
            return Optional.of(qualified);
        }
        return Optional.ofNullable(fields.get(field.getName()));
    }

    @Override
    public Optional<MessageMetric> message(Descriptor descriptor) {
        return Optional.ofNullable(messages.get(descriptor.getFullName()));
    }
}
