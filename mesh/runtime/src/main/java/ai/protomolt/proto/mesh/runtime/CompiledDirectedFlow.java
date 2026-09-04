package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.mesh.runtime.v1.CompiledFlowPlan;
import ai.protomolt.proto.mesh.runtime.v1.FlowEdge;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorNode;
import ai.protomolt.proto.projection.MessageProjection;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** An immutable, fully checked directed flow ready for execution. */
public final class CompiledDirectedFlow {

    static final String INPUT = "$input";

    private final CompiledFlowPlan plan;
    private final Map<String, NodeBinding> nodes;
    private final Map<String, List<EdgeBinding>> edgesBySource;
    private final Set<OutputBinding> outputs;

    CompiledDirectedFlow(
            CompiledFlowPlan plan,
            Map<String, NodeBinding> nodes,
            Map<String, List<EdgeBinding>> edgesBySource,
            Set<OutputBinding> outputs) {
        this.plan = Objects.requireNonNull(plan, "plan");
        this.nodes = Map.copyOf(nodes);
        this.edgesBySource = Map.copyOf(edgesBySource);
        this.outputs = Set.copyOf(outputs);
    }

    public CompiledFlowPlan plan() {
        return plan;
    }

    Map<String, NodeBinding> nodes() {
        return nodes;
    }

    List<EdgeBinding> edgesFrom(String source) {
        return edgesBySource.getOrDefault(source, List.of());
    }

    boolean retains(String nodeId, ai.protomolt.proto.mesh.v1.SchemaReference schema) {
        return outputs.contains(new OutputBinding(nodeId, RuntimeSchemas.identity(schema)));
    }

    record NodeBinding(ProcessorNode definition, ProcessorInvoker invoker) {
        NodeBinding {
            Objects.requireNonNull(definition, "definition");
            Objects.requireNonNull(invoker, "invoker");
        }
    }

    record EdgeBinding(
            FlowEdge definition,
            Descriptor sourceType,
            String source,
            String target,
            CelEvaluator predicate,
            MessageProjection projection) {
        EdgeBinding {
            Objects.requireNonNull(definition, "definition");
            Objects.requireNonNull(sourceType, "sourceType");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(target, "target");
        }

        Optional<CelEvaluator> optionalPredicate() {
            return Optional.ofNullable(predicate);
        }

        Optional<MessageProjection> optionalProjection() {
            return Optional.ofNullable(projection);
        }
    }

    record OutputBinding(
            String nodeId,
            ai.protomolt.proto.descriptors.DescriptorIdentity schema) {
        OutputBinding {
            Objects.requireNonNull(nodeId, "nodeId");
            Objects.requireNonNull(schema, "schema");
        }
    }
}
