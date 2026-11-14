package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.http.json.ProtobufJsonTranscoder;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.Definition;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.pipeline.PipelineChecker;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.pipeline.PipelineExecutor.BranchOutcome;
import ai.protomolt.proto.pipeline.PipelineExecutor.Checkpoint;
import ai.protomolt.proto.pipeline.PipelineExecutor.FailureKind;
import ai.protomolt.proto.pipeline.PipelineExecutor.Result;
import ai.protomolt.proto.pipeline.PipelineExecutor.StepOutcome;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Package-private binary-faithful JSONB codec for durable pipeline runs stored in
 * {@code workflow_run}. Keeps {@code get-job} and {@code list-jobs} compatible with the
 * existing JSON object columns while preserving exact protobuf wire bytes, descriptor
 * closure, cardinalities, and fan-out branch evidence across restarts.
 */
final class PipelineRunCodec {

    static final String FORMAT_KEY = "format";
    static final String PIPELINE_FORMAT_V1 = "protomolt.pipeline.v1";
    static final String PIPELINE_BASE64_KEY = "pipelineBase64";
    static final String DESCRIPTOR_SET_BASE64_KEY = "descriptorSetBase64";
    static final String DESCRIPTOR_FILES_KEY = "descriptorFiles";
    static final String INPUT_BASE64_KEY = "inputBase64";
    static final String PIPELINE_FINGERPRINT_KEY = "pipelineFingerprint";
    static final String INPUT_FINGERPRINT_KEY = "inputFingerprint";
    static final String PIPELINE_CHECKPOINT_KEY = "pipelineCheckpoint";

    private static final String NAME_KEY = "name";
    private static final String SKIPPED_KEY = "skipped";
    private static final String RESPONSE_KEY = "response";
    private static final String CARDINALITY_KEY = "cardinality";
    private static final String REQUEST_COUNT_KEY = "requestCount";
    private static final String RESPONSE_COUNT_KEY = "responseCount";
    private static final String MESSAGES_BASE64_KEY = "messagesBase64";
    private static final String BRANCHES_KEY = "branches";
    private static final String BRANCH_ID_KEY = "id";
    private static final String BRANCH_INDEX_KEY = "index";
    private static final String BRANCH_SUCCEEDED_KEY = "succeeded";
    private static final String BRANCH_KIND_KEY = "kind";
    private static final String BRANCH_ERROR_KEY = "error";
    private static final String ITEMS_KEY = "items";
    private static final String BINARY_BASE64_FALLBACK_KEY = "binaryBase64";

    private static final ObjectMapper JSON = new ObjectMapper();

    private PipelineRunCodec() {
    }

    static boolean isPipelineNode(JsonNode node) {
        return node != null
                && node.isObject()
                && PIPELINE_FORMAT_V1.equals(node.path(FORMAT_KEY).asText());
    }

    static boolean isPipelineDefinition(String workflowDefinition) {
        if (workflowDefinition == null || workflowDefinition.isBlank()) {
            return false;
        }
        try {
            return isPipelineNode(JSON.readTree(workflowDefinition));
        } catch (Exception e) {
            return false;
        }
    }

    static ObjectNode encodeDefinition(Pipeline pipeline, List<FileDescriptor> files,
                                       DynamicMessage canonicalInput) {
        List<FileDescriptor> sortedRootFiles = files.stream()
                .sorted(Comparator.comparing(FileDescriptor::getName))
                .toList();
        Map<String, FileDescriptorProto> closure = new LinkedHashMap<>();
        for (FileDescriptor file : sortedRootFiles) {
            collectTopological(file, closure);
        }
        FileDescriptorSet descriptorSet = FileDescriptorSet.newBuilder()
                .addAllFile(closure.values())
                .build();
        String pipelineFingerprint = PipelineExecutor.pipelineFingerprint(pipeline, sortedRootFiles);
        String inputFingerprint = PipelineExecutor.inputFingerprint(canonicalInput);

        ObjectNode root = JSON.createObjectNode();
        root.put(FORMAT_KEY, PIPELINE_FORMAT_V1);
        root.put(PIPELINE_BASE64_KEY, Base64.getEncoder().encodeToString(canonicalBytes(pipeline)));
        root.put(DESCRIPTOR_SET_BASE64_KEY,
                Base64.getEncoder().encodeToString(canonicalBytes(descriptorSet)));
        ArrayNode fileNames = root.putArray(DESCRIPTOR_FILES_KEY);
        for (FileDescriptor file : sortedRootFiles) {
            fileNames.add(file.getName());
        }
        root.put(INPUT_BASE64_KEY,
                Base64.getEncoder().encodeToString(canonicalBytes(canonicalInput)));
        root.put(PIPELINE_FINGERPRINT_KEY, pipelineFingerprint);
        root.put(INPUT_FINGERPRINT_KEY, inputFingerprint);
        return root;
    }

    record PartialSubmission(Pipeline pipeline, List<FileDescriptor> files,
                             Descriptor inputType, ProtobufJsonTranscoder transcoder) {
    }

    static PartialSubmission decodeSubmissionEnvelope(ObjectNode envelope,
                                                      DescriptorRegistry extraRegistry) {
        Pipeline pipeline = decodePipeline(envelope.path(PIPELINE_BASE64_KEY).asText(null));
        Map<String, FileDescriptor> allBuilt = decodeDescriptorMap(
                envelope.path(DESCRIPTOR_SET_BASE64_KEY).asText(null));
        List<FileDescriptor> files = selectRootFiles(envelope.get(DESCRIPTOR_FILES_KEY), allBuilt);
        verifyPipeline(pipeline, files);
        DescriptorRegistry registry = buildRegistry(files, extraRegistry);
        Descriptor inputType = registry.findDescriptorByFullName(pipeline.getInputType());
        if (inputType == null) {
            throw new PipelineDefinitionException("",
                    "input_type '" + pipeline.getInputType() + "' does not resolve");
        }
        return new PartialSubmission(pipeline, files, inputType,
                new ProtobufJsonTranscoder(registry));
    }

    static Definition readDefinition(WorkflowRunRecord job) {
        if (job == null || job.workflowDefinition == null) {
            throw new PipelineDefinitionException("", "job has no stored pipeline definition");
        }
        JsonNode tree;
        try {
            tree = JSON.readTree(job.workflowDefinition);
        } catch (Exception e) {
            throw new PipelineDefinitionException("",
                    "stored pipeline definition is not readable JSON: " + e.getMessage(), e);
        }
        if (!isPipelineNode(tree)) {
            throw new PipelineDefinitionException("",
                    "stored workflow definition is not a " + PIPELINE_FORMAT_V1 + " envelope");
        }
        ObjectNode envelope = (ObjectNode) tree;
        Pipeline pipeline = decodePipeline(envelope.path(PIPELINE_BASE64_KEY).asText(null));
        Map<String, FileDescriptor> allBuilt = decodeDescriptorMap(
                envelope.path(DESCRIPTOR_SET_BASE64_KEY).asText(null));
        List<FileDescriptor> files = selectRootFiles(envelope.get(DESCRIPTOR_FILES_KEY), allBuilt);
        verifyPipeline(pipeline, files);

        String storedPipelineFp = envelope.path(PIPELINE_FINGERPRINT_KEY).asText("");
        String computedPipelineFp = PipelineExecutor.pipelineFingerprint(pipeline, files);
        if (!computedPipelineFp.equals(storedPipelineFp)) {
            throw new PipelineDefinitionException("",
                    "stored pipeline fingerprint does not match reconstructed pipeline and descriptors");
        }

        DescriptorRegistry registry = buildRegistry(files, null);
        Descriptor inputType = registry.findDescriptorByFullName(pipeline.getInputType());
        String inputBase64 = envelope.path(INPUT_BASE64_KEY).asText(null);
        if (inputBase64 == null || inputBase64.isEmpty()
                && !envelope.hasNonNull(INPUT_BASE64_KEY)) {
            throw new PipelineDefinitionException("", "stored pipeline definition is missing inputBase64");
        }
        DynamicMessage input;
        try {
            byte[] rawInput = Base64.getDecoder().decode(inputBase64);
            input = DynamicMessage.parseFrom(inputType, rawInput);
        } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "stored pipeline inputBase64 cannot be decoded as "
                            + inputType.getFullName() + ": " + e.getMessage(), e);
        }
        String storedInputFp = envelope.path(INPUT_FINGERPRINT_KEY).asText("");
        String computedInputFp = PipelineExecutor.inputFingerprint(input);
        if (!computedInputFp.equals(storedInputFp)) {
            throw new PipelineDefinitionException("",
                    "stored input fingerprint does not match decoded input bytes");
        }
        return new Definition(pipeline, files, input, computedPipelineFp, computedInputFp);
    }

    static List<Checkpoint> readCheckpoints(WorkflowRunRecord job, Definition definition) {
        String raw = job.checkpoints == null ? "[]" : job.checkpoints;
        JsonNode tree;
        try {
            tree = JSON.readTree(raw);
        } catch (Exception e) {
            throw new PipelineDefinitionException("",
                    "stored checkpoints are not readable JSON: " + e.getMessage(), e);
        }
        if (!(tree instanceof ArrayNode array)) {
            throw new PipelineDefinitionException("", "stored checkpoints are not an array");
        }
        Pipeline pipeline = definition.pipeline();
        if (array.size() > pipeline.getStepsCount()) {
            throw new PipelineDefinitionException("",
                    "stored checkpoints (" + array.size() + ") outnumber pipeline steps ("
                            + pipeline.getStepsCount() + ")");
        }
        DescriptorRegistry registry = buildRegistry(definition.files(), null);
        Map<String, Descriptor> scopeTypes = new LinkedHashMap<>();
        scopeTypes.put("input", definition.input().getDescriptorForType());

        List<Checkpoint> restored = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            PipelineStep step = pipeline.getSteps(i);
            Descriptor stepOutputType = stepOutputType(step, definition.files(), registry, scopeTypes);
            scopeTypes.put(step.getName(), stepOutputType);

            JsonNode entry = array.get(i);
            if (entry == null || !entry.isObject()) {
                throw new PipelineDefinitionException(step.getName(),
                        "checkpoint " + i + " is not a JSON object");
            }
            String stepName = entry.path(NAME_KEY).asText("");
            if (!step.getName().equals(stepName)) {
                throw new PipelineDefinitionException(step.getName(),
                        "checkpoint " + i + " belongs to step '" + stepName
                                + "' but pipeline step " + i + " is '" + step.getName() + "'");
            }
            boolean skipped = entry.path(SKIPPED_KEY).asBoolean(false);
            JsonNode meta = entry.get(PIPELINE_CHECKPOINT_KEY);
            if (meta == null || !meta.isObject()) {
                throw new PipelineDefinitionException(stepName,
                        "checkpoint '" + stepName + "' is missing pipelineCheckpoint metadata");
            }
            String cpPipelineFp = meta.path(PIPELINE_FINGERPRINT_KEY).asText("");
            String cpInputFp = meta.path(INPUT_FINGERPRINT_KEY).asText("");
            EdgeCardinality cardinality = parseCardinality(
                    meta.path(CARDINALITY_KEY).asText(""), stepName);
            int requestCount = requireInt(meta, REQUEST_COUNT_KEY, stepName);
            int responseCount = requireInt(meta, RESPONSE_COUNT_KEY, stepName);

            JsonNode rawMessages = meta.get(MESSAGES_BASE64_KEY);
            if (!(rawMessages instanceof ArrayNode messagesArray)) {
                throw new PipelineDefinitionException(stepName,
                        "checkpoint '" + stepName + "' messagesBase64 is not an array");
            }
            List<DynamicMessage> messages = new ArrayList<>(messagesArray.size());
            for (JsonNode item : messagesArray) {
                if (!item.isTextual()) {
                    throw new PipelineDefinitionException(stepName,
                            "checkpoint '" + stepName + "' message entry is not base64 text");
                }
                try {
                    byte[] decoded = Base64.getDecoder().decode(item.asText());
                    messages.add(DynamicMessage.parseFrom(stepOutputType, decoded));
                } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
                    throw new PipelineDefinitionException(stepName,
                            "checkpoint '" + stepName + "' message cannot be decoded as "
                                    + stepOutputType.getFullName() + ": " + e.getMessage(), e);
                }
            }

            List<BranchOutcome> branches = readBranches(meta.get(BRANCHES_KEY), stepName);
            restored.add(new Checkpoint(
                    cpPipelineFp,
                    cpInputFp,
                    new StepOutcome(stepName, skipped, requestCount, responseCount),
                    cardinality,
                    messages,
                    branches));
        }
        return List.copyOf(restored);
    }

    static String writeCheckpoints(List<Checkpoint> checkpoints, List<FileDescriptor> files) {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(buildRegistry(files, null));
        ArrayNode array = JSON.createArrayNode();
        for (Checkpoint checkpoint : checkpoints) {
            array.add(checkpointEntryNode(checkpoint, transcoder));
        }
        return array.toString();
    }

    static String writeCheckpointEntry(Checkpoint checkpoint, List<FileDescriptor> files) {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(buildRegistry(files, null));
        return checkpointEntryNode(checkpoint, transcoder).toString();
    }

    static ObjectNode checkpointEntryNode(Checkpoint checkpoint,
                                          ProtobufJsonTranscoder transcoder) {
        ObjectNode entry = JSON.createObjectNode();
        entry.put(NAME_KEY, checkpoint.outcome().name());
        entry.put(SKIPPED_KEY, checkpoint.outcome().skipped());
        if (!checkpoint.outcome().skipped()) {
            if (checkpoint.cardinality() == EdgeCardinality.EDGE_CARDINALITY_ONE) {
                entry.set(RESPONSE_KEY, messageJsonObject(checkpoint.messages().getFirst(),
                        transcoder));
            } else {
                ObjectNode streamWrapper = JSON.createObjectNode();
                ArrayNode items = streamWrapper.putArray(ITEMS_KEY);
                for (DynamicMessage message : checkpoint.messages()) {
                    items.add(messageJsonObject(message, transcoder));
                }
                entry.set(RESPONSE_KEY, streamWrapper);
            }
        }
        ObjectNode meta = entry.putObject(PIPELINE_CHECKPOINT_KEY);
        meta.put(PIPELINE_FINGERPRINT_KEY, checkpoint.pipelineFingerprint());
        meta.put(INPUT_FINGERPRINT_KEY, checkpoint.inputFingerprint());
        meta.put(CARDINALITY_KEY, checkpoint.cardinality().name());
        meta.put(REQUEST_COUNT_KEY, checkpoint.outcome().requestCount());
        meta.put(RESPONSE_COUNT_KEY, checkpoint.outcome().responseCount());
        ArrayNode messagesBase64 = meta.putArray(MESSAGES_BASE64_KEY);
        for (DynamicMessage message : checkpoint.messages()) {
            messagesBase64.add(Base64.getEncoder().encodeToString(canonicalBytes(message)));
        }
        ArrayNode branchesNode = meta.putArray(BRANCHES_KEY);
        for (BranchOutcome branch : checkpoint.branches()) {
            ObjectNode b = branchesNode.addObject();
            b.put(BRANCH_ID_KEY, branch.id());
            b.put(BRANCH_INDEX_KEY, branch.index());
            b.put(BRANCH_SUCCEEDED_KEY, branch.succeeded());
            if (branch.kind() != null) {
                b.put(BRANCH_KIND_KEY, branch.kind().name());
            }
            b.put(BRANCH_ERROR_KEY, branch.error());
        }
        return entry;
    }

    static String writeResult(Result result, List<FileDescriptor> files) {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(buildRegistry(files, null));
        if (result.cardinality() == EdgeCardinality.EDGE_CARDINALITY_ONE) {
            return messageJsonObject(result.message(), transcoder).toString();
        }
        ObjectNode streamWrapper = JSON.createObjectNode();
        ArrayNode items = streamWrapper.putArray(ITEMS_KEY);
        for (DynamicMessage message : result.messages()) {
            items.add(messageJsonObject(message, transcoder));
        }
        return streamWrapper.toString();
    }

    static String renderInputJson(DynamicMessage input, List<FileDescriptor> files) {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(buildRegistry(files, null));
        return messageJsonObject(input, transcoder).toString();
    }

    static List<DynamicMessage> parseExternalJsonResponses(ObjectNode response,
                                                           EdgeCardinality cardinality,
                                                           Descriptor outputType,
                                                           int maxStreamMessages,
                                                           String stepName,
                                                           List<FileDescriptor> files,
                                                           DescriptorRegistry extraRegistry)
            throws MalformedProtobufJsonException {
        ProtobufJsonTranscoder transcoder = new ProtobufJsonTranscoder(
                buildRegistry(files, extraRegistry));
        if (cardinality == EdgeCardinality.EDGE_CARDINALITY_ONE) {
            return List.of(transcoder.fromJsonDynamic(response.toString(), outputType));
        }
        JsonNode itemsNode = response.get(ITEMS_KEY);
        if (!(itemsNode instanceof ArrayNode items)) {
            throw new IllegalArgumentException(
                    "streaming external step '" + stepName
                            + "' requires 'response' object {\"items\": [...]}");
        }
        if (items.size() > maxStreamMessages) {
            throw new IllegalArgumentException(
                    "streaming external step '" + stepName + "' supplied " + items.size()
                            + " items, exceeding max_stream_messages " + maxStreamMessages);
        }
        List<DynamicMessage> parsed = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) {
            JsonNode element = items.get(i);
            if (!element.isObject()) {
                throw new IllegalArgumentException(
                        "streaming external step '" + stepName + "' item " + i
                                + " must be a JSON object");
            }
            parsed.add(transcoder.fromJsonDynamic(element.toString(), outputType));
        }
        return List.copyOf(parsed);
    }

    static MethodDescriptor resolveMethod(List<FileDescriptor> files, String qualified) {
        int slash = qualified.indexOf('/');
        if (slash <= 0 || slash == qualified.length() - 1) {
            throw new IllegalArgumentException(
                    "method must be 'package.Service/Method'; got '" + qualified + "'");
        }
        String serviceName = qualified.substring(0, slash);
        String methodName = qualified.substring(slash + 1);
        for (FileDescriptor file : files) {
            for (ServiceDescriptor service : file.getServices()) {
                if (service.getFullName().equals(serviceName)) {
                    MethodDescriptor method = service.findMethodByName(methodName);
                    if (method != null) {
                        return method;
                    }
                }
            }
        }
        return null;
    }

    static DescriptorRegistry buildRegistry(List<FileDescriptor> files,
                                            DescriptorRegistry extraRegistry) {
        DescriptorRegistry registry = DescriptorRegistry.create();
        List<FileDescriptor> pending = new ArrayList<>(files);
        Set<String> seen = new LinkedHashSet<>();
        for (int i = 0; i < pending.size(); i++) {
            FileDescriptor file = pending.get(i);
            if (seen.add(file.getName())) {
                registry.registerFile(file);
                pending.addAll(file.getDependencies());
            }
        }
        if (extraRegistry != null) {
            for (Descriptor descriptor : extraRegistry.registeredDescriptors()) {
                FileDescriptor file = descriptor.getFile();
                if (seen.add(file.getName())) {
                    registry.registerFile(file);
                }
            }
        }
        return registry;
    }

    static byte[] canonicalBytes(Message message) {
        byte[] bytes = new byte[message.getSerializedSize()];
        CodedOutputStream out = CodedOutputStream.newInstance(bytes);
        out.useDeterministicSerialization();
        try {
            message.writeTo(out);
            out.checkNoSpaceLeft();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory protobuf serialization failed", e);
        }
        return bytes;
    }

    static boolean matchesDescriptor(Descriptor left, Descriptor right) {
        return left.getFullName().equals(right.getFullName())
                && DescriptorFingerprints.fingerprintOf(left)
                        .equals(DescriptorFingerprints.fingerprintOf(right));
    }

    private static ObjectNode messageJsonObject(DynamicMessage message,
                                                ProtobufJsonTranscoder transcoder) {
        try {
            JsonNode tree = JSON.readTree(transcoder.toJson(message));
            if (tree instanceof ObjectNode objectNode) {
                return objectNode;
            }
        } catch (Exception ignored) {
            // Fall through when message carries an opaque google.protobuf.Any type URL
            // outside the pipeline's descriptor closure; binary fidelity lives in Base64.
        }
        ObjectNode fallback = JSON.createObjectNode();
        fallback.put(BINARY_BASE64_FALLBACK_KEY,
                Base64.getEncoder().encodeToString(canonicalBytes(message)));
        return fallback;
    }

    private static void collectTopological(FileDescriptor file,
                                           Map<String, FileDescriptorProto> out) {
        if (out.containsKey(file.getName())) {
            return;
        }
        for (FileDescriptor dependency : file.getDependencies()) {
            collectTopological(dependency, out);
        }
        out.put(file.getName(), file.toProto());
    }

    private static Pipeline decodePipeline(String base64) {
        if (base64 == null || base64.isBlank()) {
            throw new PipelineDefinitionException("", "missing pipelineBase64");
        }
        try {
            return Pipeline.parseFrom(Base64.getDecoder().decode(base64));
        } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "invalid pipelineBase64: " + e.getMessage(), e);
        }
    }

    private static Map<String, FileDescriptor> decodeDescriptorMap(String base64) {
        if (base64 == null || base64.isBlank()) {
            throw new PipelineDefinitionException("", "missing descriptorSetBase64");
        }
        FileDescriptorSet set;
        try {
            set = FileDescriptorSet.parseFrom(Base64.getDecoder().decode(base64));
        } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "invalid descriptorSetBase64: " + e.getMessage(), e);
        }
        Map<String, FileDescriptorProto> protos = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            protos.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new LinkedHashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            buildFile(proto, protos, built, new LinkedHashSet<>());
        }
        return built;
    }

    private static FileDescriptor buildFile(FileDescriptorProto proto,
                                            Map<String, FileDescriptorProto> protos,
                                            Map<String, FileDescriptor> built,
                                            Set<String> building) {
        FileDescriptor existing = built.get(proto.getName());
        if (existing != null) {
            return existing;
        }
        if (!building.add(proto.getName())) {
            throw new PipelineDefinitionException("",
                    "cyclic descriptor dependency on '" + proto.getName() + "'");
        }
        List<FileDescriptor> deps = new ArrayList<>(proto.getDependencyCount());
        for (String depName : proto.getDependencyList()) {
            FileDescriptorProto depProto = protos.get(depName);
            if (depProto == null) {
                throw new PipelineDefinitionException("",
                        "unresolved descriptor dependency '" + depName + "' required by '"
                                + proto.getName() + "'");
            }
            deps.add(buildFile(depProto, protos, built, building));
        }
        building.remove(proto.getName());
        try {
            FileDescriptor file = FileDescriptor.buildFrom(
                    proto, deps.toArray(new FileDescriptor[0]));
            built.put(proto.getName(), file);
            return file;
        } catch (DescriptorValidationException e) {
            throw new PipelineDefinitionException("",
                    "descriptor set does not link '" + proto.getName() + "': "
                            + e.getMessage(), e);
        }
    }

    private static List<FileDescriptor> selectRootFiles(JsonNode descriptorFilesNode,
                                                        Map<String, FileDescriptor> allBuilt) {
        if (descriptorFilesNode == null || descriptorFilesNode.isNull()) {
            return List.copyOf(allBuilt.values());
        }
        if (!(descriptorFilesNode instanceof ArrayNode names) || names.isEmpty()) {
            throw new PipelineDefinitionException("",
                    "descriptorFiles must be a non-empty array of file names");
        }
        List<FileDescriptor> selected = new ArrayList<>(names.size());
        for (JsonNode nameNode : names) {
            if (!nameNode.isTextual()) {
                throw new PipelineDefinitionException("",
                        "descriptorFiles entries must be strings");
            }
            FileDescriptor found = allBuilt.get(nameNode.asText());
            if (found == null) {
                throw new PipelineDefinitionException("",
                        "descriptor file '" + nameNode.asText() + "' is absent from descriptorSetBase64");
            }
            selected.add(found);
        }
        return List.copyOf(selected);
    }

    private static void verifyPipeline(Pipeline pipeline, List<FileDescriptor> files) {
        List<PipelineChecker.Finding> findings = new PipelineChecker().verify(pipeline, files);
        if (!findings.isEmpty()) {
            PipelineChecker.Finding first = findings.getFirst();
            throw new PipelineDefinitionException(first.step(),
                    "pipeline does not verify (" + findings.size() + " finding"
                            + (findings.size() == 1 ? "" : "s") + "); first: ["
                            + first.kind() + "] " + first.error());
        }
    }

    private static Descriptor stepOutputType(PipelineStep step,
                                             List<FileDescriptor> files,
                                             DescriptorRegistry registry,
                                             Map<String, Descriptor> scopeTypes) {
        if (step.hasGrpcCall()) {
            if (step.getGrpcCall().hasFanOut()) {
                return registry.findDescriptorByFullName(
                        step.getGrpcCall().getFanOut().getCollectType());
            }
            return resolveMethod(files, step.getGrpcCall().getMethod()).getOutputType();
        }
        if (step.hasStructured()) {
            return registry.findDescriptorByFullName(step.getStructured().hasFanOut()
                    ? step.getStructured().getFanOut().getCollectType()
                    : step.getStructured().getSpec().getTargetType());
        }
        if (step.hasCollect()) {
            return registry.findDescriptorByFullName(step.getCollect().getCollectType());
        }
        Descriptor source = scopeTypes.get(step.getUnnest().getSource());
        if (source == null) {
            throw new PipelineDefinitionException(step.getName(),
                    "unnest source '" + step.getUnnest().getSource() + "' is not in scope");
        }
        return resolveUnnestElementType(source, step.getUnnest().getPath(), step.getName());
    }

    private static Descriptor resolveUnnestElementType(Descriptor root, String path,
                                                       String stepName) {
        Descriptor current = root;
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            String segment = dot < 0 ? path.substring(start) : path.substring(start, dot);
            FieldDescriptor field = current.findFieldByName(segment);
            if (field == null || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new PipelineDefinitionException(stepName,
                        "unnest path '" + path + "' does not resolve on " + root.getFullName());
            }
            current = field.getMessageType();
            if (dot < 0) {
                return current;
            }
            start = dot + 1;
        }
    }

    private static EdgeCardinality parseCardinality(String value, String stepName) {
        try {
            EdgeCardinality cardinality = EdgeCardinality.valueOf(value);
            if (cardinality == EdgeCardinality.EDGE_CARDINALITY_UNSPECIFIED
                    || cardinality == EdgeCardinality.UNRECOGNIZED) {
                throw new PipelineDefinitionException(stepName,
                        "invalid checkpoint cardinality '" + value + "'");
            }
            return cardinality;
        } catch (IllegalArgumentException e) {
            throw new PipelineDefinitionException(stepName,
                    "invalid checkpoint cardinality '" + value + "'", e);
        }
    }

    private static int requireInt(JsonNode parent, String field, String stepName) {
        JsonNode value = parent.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new PipelineDefinitionException(stepName,
                    "checkpoint '" + stepName + "' field '" + field + "' is not an int");
        }
        return value.intValue();
    }

    private static List<BranchOutcome> readBranches(JsonNode rawBranches, String stepName) {
        if (rawBranches == null || rawBranches.isNull()) {
            return List.of();
        }
        if (!(rawBranches instanceof ArrayNode array)) {
            throw new PipelineDefinitionException(stepName,
                    "checkpoint '" + stepName + "' branches is not an array");
        }
        List<BranchOutcome> branches = new ArrayList<>(array.size());
        for (JsonNode node : array) {
            if (!node.isObject()) {
                throw new PipelineDefinitionException(stepName,
                        "checkpoint '" + stepName + "' branch entry is not an object");
            }
            String id = node.path(BRANCH_ID_KEY).asText("");
            int index = requireInt(node, BRANCH_INDEX_KEY, stepName);
            boolean succeeded = node.path(BRANCH_SUCCEEDED_KEY).asBoolean(false);
            FailureKind kind = null;
            JsonNode kindNode = node.get(BRANCH_KIND_KEY);
            if (kindNode != null && !kindNode.isNull()) {
                try {
                    kind = FailureKind.valueOf(kindNode.asText(""));
                } catch (IllegalArgumentException e) {
                    throw new PipelineDefinitionException(stepName,
                            "invalid branch failure kind '" + kindNode.asText("") + "'", e);
                }
            }
            JsonNode errorNode = node.get(BRANCH_ERROR_KEY);
            if (errorNode == null || !errorNode.isTextual()) {
                throw new PipelineDefinitionException(stepName,
                        "checkpoint '" + stepName + "' branch error is missing");
            }
            branches.add(new BranchOutcome(id, index, succeeded, kind, errorNode.asText()));
        }
        return List.copyOf(branches);
    }
}
