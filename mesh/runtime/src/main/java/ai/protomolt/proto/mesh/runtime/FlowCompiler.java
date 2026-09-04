package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.cel.CelCompilationException;
import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluator;
import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.CompiledDirectedFlow.EdgeBinding;
import ai.protomolt.proto.mesh.runtime.CompiledDirectedFlow.NodeBinding;
import ai.protomolt.proto.mesh.runtime.CompiledDirectedFlow.OutputBinding;
import ai.protomolt.proto.mesh.runtime.v1.CompiledFlowPlan;
import ai.protomolt.proto.mesh.runtime.v1.ChannelAcknowledgementPoint;
import ai.protomolt.proto.mesh.runtime.v1.ChannelDeliveryMode;
import ai.protomolt.proto.mesh.runtime.v1.ChannelOrderingGuarantee;
import ai.protomolt.proto.mesh.runtime.v1.ChannelOverflowAction;
import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;
import ai.protomolt.proto.mesh.runtime.v1.FlowDefinition;
import ai.protomolt.proto.mesh.runtime.v1.FlowEdge;
import ai.protomolt.proto.mesh.runtime.v1.FlowOutput;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorContract;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorNode;
import ai.protomolt.proto.mesh.v1.SchemaReference;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.ProjectionException;
import ai.protomolt.proto.shapes.RuleChecker;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.MessageLite;
import com.google.protobuf.util.Durations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Compiles authored directed flows into exact, executable plans. */
public final class FlowCompiler {

    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");

    private final DescriptorRegistry descriptors;
    private final ProcessorRegistry processors;
    private final ChannelResourceCatalog channelResources;

    public FlowCompiler(DescriptorRegistry descriptors, ProcessorRegistry processors) {
        this(descriptors, processors, ChannelResourceCatalog.builtIns());
    }

    public FlowCompiler(
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            ChannelResourceCatalog channelResources) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.processors = Objects.requireNonNull(processors, "processors");
        this.channelResources = Objects.requireNonNull(channelResources, "channelResources");
    }

    /** Compiles and fingerprints an authored definition. */
    public CompiledDirectedFlow compile(FlowDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        validateAnnotations(definition);
        if (!definition.hasInputSchema()) {
            throw new IllegalArgumentException("flow requires input_schema");
        }
        if (!definition.hasDeadline() || !Durations.isValid(definition.getDeadline())
                || Durations.compare(definition.getDeadline(),
                com.google.protobuf.Duration.getDefaultInstance()) <= 0) {
            throw new IllegalArgumentException("flow deadline must be a positive duration");
        }
        if (definition.getMaxMessages() < 1) {
            throw new IllegalArgumentException("flow max_messages must be positive");
        }
        Descriptor inputType = RuntimeSchemas.resolve(descriptors, definition.getInputSchema());

        Map<String, NodeBinding> nodes = compileNodes(definition);
        Map<String, ChannelPolicy> policies = compileChannelPolicies(definition, nodes);
        Map<String, List<EdgeBinding>> edges = compileEdges(
                definition, inputType, nodes, policies);
        Set<OutputBinding> outputs = compileOutputs(definition, nodes);
        proveAcyclicAndReachable(nodes.keySet(), edges);
        proveSchemaCoverage(definition, nodes, edges, outputs);

        CompiledFlowPlan.Builder unsigned = CompiledFlowPlan.newBuilder()
                .setDefinition(definition);
        for (ProcessorNode node : definition.getNodesList()) {
            unsigned.addProcessorContracts(nodes.get(node.getNodeId()).invoker().contract());
        }
        String fingerprint = DescriptorIdentity.sha256(deterministicBytes(unsigned.build()));
        CompiledFlowPlan plan = unsigned.setPlanFingerprint(fingerprint).build();
        return new CompiledDirectedFlow(plan, nodes, immutableLists(edges), outputs);
    }

    /** Restores a persisted compiled plan and refuses contract or fingerprint drift. */
    public CompiledDirectedFlow restore(CompiledFlowPlan persisted) {
        Objects.requireNonNull(persisted, "persisted");
        CompiledDirectedFlow current = compile(persisted.getDefinition());
        if (!current.plan().getProcessorContractsList()
                .equals(persisted.getProcessorContractsList())) {
            throw new IllegalArgumentException(
                    "persisted flow processor contracts do not match the live registry");
        }
        if (!current.plan().getPlanFingerprint().equals(persisted.getPlanFingerprint())) {
            throw new IllegalArgumentException("persisted flow plan fingerprint mismatch: stored "
                    + persisted.getPlanFingerprint() + " but compiled "
                    + current.plan().getPlanFingerprint());
        }
        return current;
    }

    private Map<String, NodeBinding> compileNodes(FlowDefinition definition) {
        if (definition.getNodesCount() == 0) {
            throw new IllegalArgumentException("flow requires at least one processor node");
        }
        Map<String, NodeBinding> nodes = new LinkedHashMap<>();
        for (ProcessorNode node : definition.getNodesList()) {
            if (!IDENTIFIER.matcher(node.getNodeId()).matches()) {
                throw new IllegalArgumentException(
                        "node_id must be a protobuf-style identifier: " + node.getNodeId());
            }
            if (nodes.containsKey(node.getNodeId())) {
                throw new IllegalArgumentException("duplicate node_id: " + node.getNodeId());
            }
            ProcessorInvoker invoker = processors.find(node.getProcessorId())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "node " + node.getNodeId() + " names unregistered processor "
                                    + node.getProcessorId()));
            if (!node.hasInputSchema() || node.getOutputSchemasCount() == 0) {
                throw new IllegalArgumentException("node " + node.getNodeId()
                        + " requires one input schema and at least one output schema");
            }
            RuntimeSchemas.resolve(descriptors, node.getInputSchema());
            node.getOutputSchemasList().forEach(schema ->
                    RuntimeSchemas.resolve(descriptors, schema));
            requireContract(node, invoker.contract());
            nodes.put(node.getNodeId(), new NodeBinding(node, invoker));
        }
        return nodes;
    }

    private Map<String, List<EdgeBinding>> compileEdges(
            FlowDefinition definition,
            Descriptor inputType,
            Map<String, NodeBinding> nodes,
            Map<String, ChannelPolicy> policies) {
        if (definition.getEdgesCount() == 0) {
            throw new IllegalArgumentException("flow requires at least one edge");
        }
        Set<String> ids = new HashSet<>();
        Map<String, List<EdgeBinding>> edges = new LinkedHashMap<>();
        for (FlowEdge edge : definition.getEdgesList()) {
            if (!IDENTIFIER.matcher(edge.getEdgeId()).matches() || !ids.add(edge.getEdgeId())) {
                throw new IllegalArgumentException("edge_id must be a unique identifier: "
                        + edge.getEdgeId());
            }
            if (!edge.hasSourceSchema()) {
                throw new IllegalArgumentException("edge " + edge.getEdgeId()
                        + " requires source_schema");
            }
            ChannelPolicy channelPolicy = policies.get(edge.getChannelPolicyId());
            if (channelPolicy == null) {
                throw new IllegalArgumentException(
                        "channel-policy-unknown: edge " + edge.getEdgeId()
                                + " names unavailable policy " + edge.getChannelPolicyId());
            }
            Descriptor sourceType = RuntimeSchemas.resolve(descriptors, edge.getSourceSchema());
            String source = switch (edge.getSourceCase()) {
                case FLOW_INPUT -> {
                    if (!edge.getFlowInput()) {
                        throw new IllegalArgumentException("edge " + edge.getEdgeId()
                                + " sets flow_input=false");
                    }
                    requireSchema("edge " + edge.getEdgeId() + " input source",
                            RuntimeSchemas.reference(inputType), edge.getSourceSchema());
                    yield CompiledDirectedFlow.INPUT;
                }
                case SOURCE_NODE -> {
                    NodeBinding sourceNode = nodes.get(edge.getSourceNode());
                    if (sourceNode == null) {
                        throw new IllegalArgumentException("edge " + edge.getEdgeId()
                                + " names unknown source node " + edge.getSourceNode());
                    }
                    if (!containsSchema(sourceNode.definition().getOutputSchemasList(),
                            edge.getSourceSchema())) {
                        throw new IllegalArgumentException("edge " + edge.getEdgeId()
                                + " source schema is not an output of node "
                                + edge.getSourceNode());
                    }
                    yield edge.getSourceNode();
                }
                case SOURCE_NOT_SET -> throw new IllegalArgumentException("edge "
                        + edge.getEdgeId() + " requires flow_input or source_node");
            };
            NodeBinding target = nodes.get(edge.getTargetNode());
            if (target == null) {
                throw new IllegalArgumentException("edge " + edge.getEdgeId()
                        + " names unknown target node " + edge.getTargetNode());
            }

            MessageProjection projection = compileProjection(edge, sourceType);
            SchemaReference delivered = projection == null
                    ? edge.getSourceSchema() : edge.getProjectTo();
            requireSchema("edge " + edge.getEdgeId() + " delivered schema",
                    target.definition().getInputSchema(), delivered);

            CelEvaluator predicate = compilePredicate(edge, sourceType);
            edges.computeIfAbsent(source, ignored -> new ArrayList<>())
                    .add(new EdgeBinding(edge, sourceType, source, edge.getTargetNode(),
                            predicate, projection, channelPolicy));
        }
        return edges;
    }

    private Map<String, ChannelPolicy> compileChannelPolicies(
            FlowDefinition definition, Map<String, NodeBinding> nodes) {
        Map<String, ChannelPolicy> policies = new LinkedHashMap<>();
        definition.getChannelPoliciesList().forEach(named -> {
            if (policies.putIfAbsent(named.getPolicyId(), named.getPolicy()) != null) {
                throw new IllegalArgumentException(
                        "channel-policy-duplicate: " + named.getPolicyId());
            }
            validatePolicy(named.getPolicyId(), named.getPolicy());
        });
        if (policies.isEmpty()) {
            throw new IllegalArgumentException(
                    "channel-policy-missing: a flow must declare at least one channel policy");
        }
        for (var edge : definition.getEdgesList()) {
            ChannelPolicy policy = policies.get(edge.getChannelPolicyId());
            if (policy == null) {
                continue;
            }
            NodeBinding target = nodes.get(edge.getTargetNode());
            if (target != null
                    && target.invoker().contract().getGuarantees().getRequiresDurableInput()
                    && policy.getDeliveryMode()
                    == ChannelDeliveryMode.CHANNEL_DELIVERY_MODE_BOUNDED_MEMORY) {
                throw new IllegalArgumentException(
                        "channel-policy-durability-required: edge " + edge.getEdgeId()
                                + " targets processor " + target.invoker().contract()
                                .getProcessorId());
            }
        }
        for (Map.Entry<String, ChannelPolicy> entry : policies.entrySet()) {
            ChannelPolicy policy = entry.getValue();
            if (policy.getOverflowAction()
                    == ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_DURABLE_SPILL) {
                ChannelPolicy spill = policies.get(policy.getDurableSpillPolicyId());
                if (spill == null || spill.getDeliveryMode()
                        == ChannelDeliveryMode.CHANNEL_DELIVERY_MODE_BOUNDED_MEMORY) {
                    throw new IllegalArgumentException(
                            "channel-policy-spill-unavailable: " + entry.getKey());
                }
            }
        }
        return Map.copyOf(policies);
    }

    private void validatePolicy(String id, ChannelPolicy policy) {
        if (policy.getDeliveryMode()
                == ChannelDeliveryMode.CHANNEL_DELIVERY_MODE_UNSPECIFIED) {
            refuse(id, "delivery-mode-required");
        }
        if (policy.getOverflowAction()
                == ChannelOverflowAction.CHANNEL_OVERFLOW_ACTION_UNSPECIFIED) {
            refuse(id, "overflow-action-required");
        }
        if (policy.getMaxItems() < 1 || policy.getMaxItems() > 1_000_000
                || policy.getMaxBytes() < 1) {
            refuse(id, "unbounded-capacity");
        }
        if (policy.getInlineByteLimit() < 0) {
            refuse(id, "inline-byte-limit-overflow");
        }
        if (policy.getMaximumAttempts() < 1 || policy.getMaximumAttempts() > 100) {
            refuse(id, "attempt-bound-required");
        }
        if (!channelResources.retryPolicies().contains(
                policy.getRetryPolicyReference())) {
            refuse(id, "retry-policy-unavailable");
        }
        if (!channelResources.deadLetterChannels().contains(
                policy.getDeadLetterChannelReference())) {
            refuse(id, "dead-letter-channel-unavailable");
        }
        if (!policy.getPayloadStoreProfile().isBlank()
                && !channelResources.payloadStores().contains(
                policy.getPayloadStoreProfile())) {
            refuse(id, "payload-store-unavailable");
        }
        if (policy.getDeliveryMode()
                == ChannelDeliveryMode.CHANNEL_DELIVERY_MODE_TRANSACTIONAL_EXTERNAL
                && !channelResources.transactionalChannels().contains(
                policy.getTransactionalChannelProfile())) {
            refuse(id, "transactional-channel-unavailable");
        }
        if (policy.getPersistenceProhibited()
                && policy.getDeliveryMode()
                != ChannelDeliveryMode.CHANNEL_DELIVERY_MODE_BOUNDED_MEMORY) {
            refuse(id, "persistence-prohibition-conflict");
        }
        if (policy.getOrderingGuarantee()
                == ChannelOrderingGuarantee.CHANNEL_ORDERING_GUARANTEE_UNSPECIFIED
                || policy.getConcurrencyBound() <= 0
                || policy.getConcurrencyBound() > 1_000_000) {
            refuse(id, "ordering-or-concurrency-unspecified");
        }
        if (policy.getOrderingGuarantee()
                == ChannelOrderingGuarantee.CHANNEL_ORDERING_GUARANTEE_TOTAL
                && policy.getConcurrencyBound() != 1) {
            refuse(id, "total-order-requires-single-concurrency");
        }
        if (policy.getOrderingGuarantee()
                == ChannelOrderingGuarantee.CHANNEL_ORDERING_GUARANTEE_PER_KEY
                && policy.getOrderingKey().isBlank()) {
            refuse(id, "per-key-order-requires-key");
        }
        if (policy.getAcknowledgementPoint()
                == ChannelAcknowledgementPoint.CHANNEL_ACKNOWLEDGEMENT_POINT_UNSPECIFIED
                || policy.getRequiredCompletionPolicyValue() == 0) {
            refuse(id, "acknowledgement-or-completion-unspecified");
        }
        if (!policy.getRetentionPolicyReference().isBlank()
                && !channelResources.retentionPolicies().contains(
                policy.getRetentionPolicyReference())) {
            refuse(id, "retention-policy-unavailable");
        }
        if (!policy.getLegalHoldPolicyReference().isBlank()
                && !channelResources.legalHoldPolicies().contains(
                policy.getLegalHoldPolicyReference())) {
            refuse(id, "legal-hold-policy-unavailable");
        }
    }

    private static void refuse(String id, String reason) {
        throw new IllegalArgumentException(
                "channel-policy-" + reason + ": " + id);
    }

    private MessageProjection compileProjection(FlowEdge edge, Descriptor sourceType) {
        if (!edge.hasProjectTo()) {
            return null;
        }
        Descriptor target = RuntimeSchemas.resolve(descriptors, edge.getProjectTo());
        MessageProjection projection;
        try {
            projection = MessageProjection.forTarget(target, descriptors)
                    .orElseThrow(() -> new IllegalArgumentException("edge " + edge.getEdgeId()
                            + " projection target declares no projection sources: "
                            + target.getFullName()));
        } catch (ProjectionException e) {
            throw new IllegalArgumentException("edge " + edge.getEdgeId()
                    + " projection is invalid: " + e.getMessage(), e);
        }
        if (!projection.supports(sourceType)) {
            throw new IllegalArgumentException("edge " + edge.getEdgeId() + " projection "
                    + target.getFullName() + " does not accept exact source "
                    + DescriptorIdentity.of(sourceType));
        }
        return projection;
    }

    private static CelEvaluator compilePredicate(FlowEdge edge, Descriptor sourceType) {
        if (edge.getWhen().isBlank()) {
            return null;
        }
        CelEvaluator evaluator = new CelEvaluator(CelEnvironmentFactory.builder()
                .addMessageVar("message", sourceType)
                .build());
        List<RuleChecker.Finding> findings = new RuleChecker().checkInPlace(
                "message", sourceType, List.of(), List.of(), List.of(edge.getWhen()));
        if (!findings.isEmpty()) {
            throw new IllegalArgumentException("edge " + edge.getEdgeId()
                    + " predicate is not a valid boolean expression for "
                    + sourceType.getFullName() + ": " + findings.getFirst().error());
        }
        try {
            evaluator.precompile(edge.getWhen());
        } catch (CelCompilationException e) {
            throw new IllegalArgumentException("edge " + edge.getEdgeId()
                    + " predicate does not compile against " + sourceType.getFullName()
                    + ": " + edge.getWhen(), e);
        }
        return evaluator;
    }

    private Set<OutputBinding> compileOutputs(
            FlowDefinition definition, Map<String, NodeBinding> nodes) {
        if (definition.getOutputsCount() == 0) {
            throw new IllegalArgumentException("flow requires at least one output");
        }
        Set<OutputBinding> outputs = new LinkedHashSet<>();
        for (FlowOutput output : definition.getOutputsList()) {
            NodeBinding node = nodes.get(output.getNodeId());
            if (node == null) {
                throw new IllegalArgumentException("flow output names unknown node "
                        + output.getNodeId());
            }
            RuntimeSchemas.resolve(descriptors, output.getSchema());
            if (!containsSchema(node.definition().getOutputSchemasList(), output.getSchema())) {
                throw new IllegalArgumentException("flow output schema "
                        + output.getSchema().getTypeName() + " is not produced by node "
                        + output.getNodeId());
            }
            OutputBinding binding = new OutputBinding(
                    output.getNodeId(), RuntimeSchemas.identity(output.getSchema()));
            if (!outputs.add(binding)) {
                throw new IllegalArgumentException("duplicate flow output for node "
                        + output.getNodeId() + " and schema "
                        + output.getSchema().getTypeName());
            }
        }
        return outputs;
    }

    private static void proveAcyclicAndReachable(
            Set<String> nodeIds, Map<String, List<EdgeBinding>> edges) {
        Map<String, Integer> indegree = new LinkedHashMap<>();
        nodeIds.forEach(node -> indegree.put(node, 0));
        for (Map.Entry<String, List<EdgeBinding>> entry : edges.entrySet()) {
            if (entry.getKey().equals(CompiledDirectedFlow.INPUT)) {
                continue;
            }
            Set<String> targets = new HashSet<>();
            for (EdgeBinding edge : entry.getValue()) {
                if (targets.add(edge.target())) {
                    indegree.compute(edge.target(), (ignored, value) -> value + 1);
                }
            }
        }
        Deque<String> ready = new ArrayDeque<>();
        indegree.forEach((node, degree) -> {
            if (degree == 0) {
                ready.add(node);
            }
        });
        int visited = 0;
        while (!ready.isEmpty()) {
            String source = ready.removeFirst();
            visited++;
            Set<String> targets = new HashSet<>();
            for (EdgeBinding edge : edges.getOrDefault(source, List.of())) {
                if (targets.add(edge.target())
                        && indegree.compute(edge.target(), (ignored, value) -> value - 1) == 0) {
                    ready.addLast(edge.target());
                }
            }
        }
        if (visited != nodeIds.size()) {
            throw new IllegalArgumentException("flow graph contains a processor cycle");
        }

        Set<String> reachable = new LinkedHashSet<>();
        Deque<String> pending = new ArrayDeque<>();
        edges.getOrDefault(CompiledDirectedFlow.INPUT, List.of()).forEach(edge ->
                pending.add(edge.target()));
        while (!pending.isEmpty()) {
            String node = pending.removeFirst();
            if (!reachable.add(node)) {
                continue;
            }
            edges.getOrDefault(node, List.of()).forEach(edge -> pending.add(edge.target()));
        }
        if (!reachable.equals(nodeIds)) {
            Set<String> missing = new LinkedHashSet<>(nodeIds);
            missing.removeAll(reachable);
            throw new IllegalArgumentException("flow contains nodes unreachable from input: "
                    + missing);
        }
    }

    private static void proveSchemaCoverage(
            FlowDefinition definition,
            Map<String, NodeBinding> nodes,
            Map<String, List<EdgeBinding>> edges,
            Set<OutputBinding> outputs) {
        if (edges.getOrDefault(CompiledDirectedFlow.INPUT, List.of()).isEmpty()) {
            throw new IllegalArgumentException("flow input is not connected to a processor node");
        }
        for (NodeBinding node : nodes.values()) {
            for (SchemaReference schema : node.definition().getOutputSchemasList()) {
                boolean routed = edges.getOrDefault(node.definition().getNodeId(), List.of())
                        .stream()
                        .anyMatch(edge -> RuntimeSchemas.same(
                                edge.definition().getSourceSchema(), schema));
                boolean retained = outputs.contains(new OutputBinding(
                        node.definition().getNodeId(), RuntimeSchemas.identity(schema)));
                if (!routed && !retained) {
                    throw new IllegalArgumentException("node " + node.definition().getNodeId()
                            + " output schema " + schema.getTypeName()
                            + " is neither routed nor retained as a flow output");
                }
            }
        }
    }

    private static void requireContract(ProcessorNode node, ProcessorContract contract) {
        if (!node.getProcessorId().equals(contract.getProcessorId())) {
            throw new IllegalArgumentException("node " + node.getNodeId()
                    + " resolved a processor with another id: "
                    + contract.getProcessorId());
        }
        requireSchema("node " + node.getNodeId() + " input",
                contract.getInputSchema(), node.getInputSchema());
        Set<DescriptorIdentity> declared = identities(node.getOutputSchemasList());
        Set<DescriptorIdentity> actual = identities(contract.getOutputSchemasList());
        if (!declared.equals(actual)) {
            throw new IllegalArgumentException("node " + node.getNodeId()
                    + " output schemas do not match processor " + node.getProcessorId()
                    + ": node=" + declared + ", processor=" + actual);
        }
    }

    private static void requireSchema(
            String subject, SchemaReference expected, SchemaReference actual) {
        DescriptorIdentity expectedIdentity = RuntimeSchemas.identity(expected);
        DescriptorIdentity actualIdentity = RuntimeSchemas.identity(actual);
        if (!expectedIdentity.equals(actualIdentity)) {
            throw new IllegalArgumentException(subject + " mismatch: expected "
                    + expectedIdentity + " but got " + actualIdentity);
        }
    }

    private static boolean containsSchema(
            List<SchemaReference> schemas, SchemaReference candidate) {
        DescriptorIdentity identity = RuntimeSchemas.identity(candidate);
        return schemas.stream().map(RuntimeSchemas::identity).anyMatch(identity::equals);
    }

    private static Set<DescriptorIdentity> identities(List<SchemaReference> schemas) {
        Set<DescriptorIdentity> identities = schemas.stream()
                .map(RuntimeSchemas::identity)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (identities.size() != schemas.size()) {
            throw new IllegalArgumentException("schema list contains duplicate exact identities");
        }
        return identities;
    }

    private static Map<String, List<EdgeBinding>> immutableLists(
            Map<String, List<EdgeBinding>> source) {
        Map<String, List<EdgeBinding>> result = new LinkedHashMap<>();
        source.forEach((name, bindings) -> result.put(name, List.copyOf(bindings)));
        return result;
    }

    private static void validateAnnotations(FlowDefinition definition) {
        var result = ProtoValidator.forMessageType(definition.getDescriptorForType())
                .validate(definition);
        if (!result.valid()) {
            throw new IllegalArgumentException("flow definition fails validation: "
                    + result.violations().stream()
                    .map(violation -> "[" + violation.path() + "] "
                            + violation.ruleId() + ": " + violation.message())
                    .collect(Collectors.joining("; ")));
        }
    }

    static byte[] deterministicBytes(MessageLite message) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(message.getSerializedSize());
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try {
            message.writeTo(output);
            output.flush();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory protobuf serialization failed", e);
        }
        return bytes.toByteArray();
    }
}
