package ai.pipestream.proto.metric.spi;

import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MessageMetric;
import ai.pipestream.proto.metric.MetricProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Optional;

/**
 * Reads the schema's own metric.v1 declarations off descriptor options. A
 * descriptor set linked without the extension registry keeps the option as
 * an unknown field; those bytes are reparsed with the registry rather than
 * silently dropped, because dropping them would revert deliberate schema
 * decisions.
 */
public final class ProtoOptionsMetricHintSource implements MetricHintSource {

    private static final ExtensionRegistry EXTENSIONS = ExtensionRegistry.newInstance();

    static {
        MetricProto.registerAllExtensions(EXTENSIONS);
    }

    @Override
    public Optional<FieldMetric> field(FieldDescriptor field) {
        FieldOptions options = field.getOptions();
        if (options.hasExtension(MetricProto.metric)) {
            return Optional.of(options.getExtension(MetricProto.metric));
        }
        if (!options.getUnknownFields().hasField(MetricProto.metric.getNumber())) {
            return Optional.empty();
        }
        try {
            return Optional.of(FieldOptions.parseFrom(options.toByteString(), EXTENSIONS)
                    .getExtension(MetricProto.metric));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(
                    "cannot reparse the metric option on field '" + field.getFullName() + "'", e);
        }
    }

    @Override
    public Optional<MessageMetric> message(Descriptor descriptor) {
        MessageOptions options = descriptor.getOptions();
        if (options.hasExtension(MetricProto.metricMessage)) {
            return Optional.of(options.getExtension(MetricProto.metricMessage));
        }
        if (!options.getUnknownFields().hasField(MetricProto.metricMessage.getNumber())) {
            return Optional.empty();
        }
        try {
            return Optional.of(MessageOptions.parseFrom(options.toByteString(), EXTENSIONS)
                    .getExtension(MetricProto.metricMessage));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(
                    "cannot reparse the metric option on message '"
                            + descriptor.getFullName() + "'", e);
        }
    }
}
