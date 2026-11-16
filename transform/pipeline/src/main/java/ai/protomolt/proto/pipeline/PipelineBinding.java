package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.GrpcCallStep;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import java.util.Map;

/**
 * A named value in a running pipeline's scope: one message, or an ordered bounded stream of
 * messages of one type.
 */
record PipelineBinding(Descriptor type, EdgeCardinality cardinality,
                       List<DynamicMessage> values) {
    PipelineBinding {
        values = List.copyOf(values);
        if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE
                && values.size() != 1) {
            throw new IllegalArgumentException("a ONE binding contains exactly one value");
        }
    }

    DynamicMessage one() {
        return values.getFirst();
    }

    static PipelineBinding one(DynamicMessage value) {
        return new PipelineBinding(value.getDescriptorForType(),
                EdgeCardinality.EDGE_CARDINALITY_ONE, List.of(value));
    }

    static PipelineBinding many(Descriptor type, List<DynamicMessage> values) {
        return new PipelineBinding(type, EdgeCardinality.EDGE_CARDINALITY_MANY, values);
    }

    /** A client-streaming call uses up the stream binding that fed it. */
    static void consumeRequestStream(GrpcCallStep call, MethodDescriptor method,
                                     Map<String, PipelineBinding> scope) {
        if (!method.isClientStreaming() || !call.hasEdge()) {
            return;
        }
        call.getEdge().getSourcesList().stream()
                .filter(source -> {
                    PipelineBinding binding = scope.get(source);
                    return binding != null && binding.cardinality()
                            == EdgeCardinality.EDGE_CARDINALITY_MANY;
                })
                .findFirst().ifPresent(scope::remove);
    }
}
