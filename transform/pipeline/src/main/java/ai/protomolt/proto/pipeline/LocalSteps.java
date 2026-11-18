package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.PipelineExecutionException;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;

/**
 * The steps that call nothing: {@code unnest} spreads a repeated field into a stream and
 * {@code collect} gathers a stream into a repeated field. Both are pure functions of the
 * bindings in scope, which is why replay computes them again instead of trusting a checkpoint.
 */
final class LocalSteps {

    private LocalSteps() {
    }

    static PipelineBinding unnest(PipelineStep step, Map<String, PipelineBinding> scope,
                                  int limit) throws PipelineExecutionException {
        PipelineBinding source = scope.get(step.getUnnest().getSource());
        FieldDescriptor field = StreamPaths.repeatedField(source.type(),
                step.getUnnest().getPath());
        List<?> raw = StreamPaths.repeatedValues(source.one(), step.getUnnest().getPath());
        if (raw.size() > limit) {
            throw PipelineExecutor.failure(step.getName(), FailureKind.PIPELINE,
                    "unnest produced " + raw.size() + " messages, over max_stream_messages "
                            + limit, null);
        }
        List<DynamicMessage> values = raw.stream()
                .map(value -> StepValues.dynamic((Message) value)).toList();
        return PipelineBinding.many(field.getMessageType(), values);
    }

    /** The collected holder. The caller removes the consumed source stream from scope. */
    static PipelineBinding collect(PipelineStep step, Map<String, PipelineBinding> scope,
                                   DescriptorRegistry registry) {
        PipelineBinding source = scope.get(step.getCollect().getSource());
        Descriptor type = registry.findDescriptorByFullName(step.getCollect().getCollectType());
        FieldDescriptor field = type.findFieldByName(step.getCollect().getCollectInto());
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
        source.values().forEach(value -> builder.addRepeatedField(field, value));
        return PipelineBinding.one(builder.build());
    }
}
