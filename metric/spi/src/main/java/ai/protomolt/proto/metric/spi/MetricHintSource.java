package ai.protomolt.proto.metric.spi;

import ai.protomolt.proto.metric.FieldMetric;
import ai.protomolt.proto.metric.MessageMetric;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.util.Optional;

/**
 * Where metric declarations come from. The schema's own metric.v1 options
 * are the primary source; a catalog source covers schemas that cannot be
 * annotated (third-party descriptors), the same split indexing hints make.
 */
public interface MetricHintSource {

    /** The field's metric declaration, when this source has one. */
    Optional<FieldMetric> field(FieldDescriptor field);

    /** The message's metric declaration, when this source has one. */
    Optional<MessageMetric> message(Descriptor descriptor);

    /** A source with no declarations. */
    static MetricHintSource empty() {
        return new MetricHintSource() {
            @Override
            public Optional<FieldMetric> field(FieldDescriptor field) {
                return Optional.empty();
            }

            @Override
            public Optional<MessageMetric> message(Descriptor descriptor) {
                return Optional.empty();
            }
        };
    }

    /** This source, falling back to {@code fallback} where it is silent. */
    default MetricHintSource orElse(MetricHintSource fallback) {
        MetricHintSource primary = this;
        return new MetricHintSource() {
            @Override
            public Optional<FieldMetric> field(FieldDescriptor field) {
                return primary.field(field).or(() -> fallback.field(field));
            }

            @Override
            public Optional<MessageMetric> message(Descriptor descriptor) {
                return primary.message(descriptor).or(() -> fallback.message(descriptor));
            }
        };
    }
}
