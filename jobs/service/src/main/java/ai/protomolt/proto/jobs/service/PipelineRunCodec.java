package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.http.json.ProtobufJsonTranscoder;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.Definition;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.pipeline.PipelineChecker;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Package-private binary-faithful JSONB codec for the {@code protomolt.pipeline.v1}
 * run-definition envelope stored in {@code workflow_run.workflow_definition}, preserving
 * exact protobuf wire bytes, transitive descriptor closure, and SHA-256 identities.
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

    private static final ObjectMapper JSON = new ObjectMapper();

    private PipelineRunCodec() {
    }

    static boolean isPipelineNode(JsonNode node) {
        return node instanceof ObjectNode object && hasPipelineMarker(object);
    }

    static boolean isPipelineDefinition(String workflowDefinition) {
        if (workflowDefinition == null) {
            throw new IllegalArgumentException(
                    "stored workflow definition is not readable JSON: content is null");
        }
        JsonNode tree;
        try {
            tree = JSON.readTree(workflowDefinition);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "stored workflow definition is not readable JSON: " + e.getMessage(), e);
        }
        if (!(tree instanceof ObjectNode object)) {
            throw new IllegalArgumentException(
                    "stored workflow definition is not a JSON object");
        }
        if (!hasPipelineMarker(object)) {
            return false;
        }
        readDefinitionEnvelope(object);
        return true;
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
        root.put(PIPELINE_BASE64_KEY, Base64.getEncoder().encodeToString(
                MessageFingerprints.deterministicBytes(pipeline)));
        root.put(DESCRIPTOR_SET_BASE64_KEY, Base64.getEncoder().encodeToString(
                MessageFingerprints.deterministicBytes(descriptorSet)));
        ArrayNode fileNames = root.putArray(DESCRIPTOR_FILES_KEY);
        for (FileDescriptor file : sortedRootFiles) {
            fileNames.add(file.getName());
        }
        root.put(INPUT_BASE64_KEY, Base64.getEncoder().encodeToString(
                MessageFingerprints.deterministicBytes(canonicalInput)));
        root.put(PIPELINE_FINGERPRINT_KEY, pipelineFingerprint);
        root.put(INPUT_FINGERPRINT_KEY, inputFingerprint);
        return root;
    }

    record PartialSubmission(Pipeline pipeline, List<FileDescriptor> files,
                             Descriptor inputType, ProtobufJsonTranscoder transcoder) {
    }

    static PartialSubmission decodeSubmissionEnvelope(ObjectNode envelope,
                                                      DescriptorRegistry extraRegistry) {
        requirePipelineFormat(envelope);
        Pipeline pipeline = decodePipeline(textOrNull(envelope, PIPELINE_BASE64_KEY));
        Map<String, FileDescriptor> allBuilt = decodeDescriptorMap(
                textOrNull(envelope, DESCRIPTOR_SET_BASE64_KEY));
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
        } catch (JsonProcessingException e) {
            throw new PipelineDefinitionException("",
                    "stored pipeline definition is not readable JSON: " + e.getMessage(), e);
        }
        if (!(tree instanceof ObjectNode envelope)) {
            throw new PipelineDefinitionException("",
                    "stored workflow definition is not a JSON object");
        }
        return readDefinitionEnvelope(envelope);
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

    static boolean matchesDescriptor(Descriptor left, Descriptor right) {
        return left.getFullName().equals(right.getFullName())
                && DescriptorFingerprints.fingerprintOf(left)
                        .equals(DescriptorFingerprints.fingerprintOf(right));
    }

    private static boolean hasPipelineMarker(ObjectNode object) {
        return object.has(FORMAT_KEY)
                || object.has(PIPELINE_BASE64_KEY)
                || object.has(DESCRIPTOR_SET_BASE64_KEY)
                || object.has(DESCRIPTOR_FILES_KEY)
                || object.has(INPUT_BASE64_KEY)
                || object.has(PIPELINE_FINGERPRINT_KEY)
                || object.has(INPUT_FINGERPRINT_KEY);
    }

    private static void requirePipelineFormat(ObjectNode envelope) {
        JsonNode formatNode = envelope.get(FORMAT_KEY);
        if (formatNode == null || !formatNode.isTextual()) {
            throw new PipelineDefinitionException("",
                    "stored pipeline definition is missing format '" + PIPELINE_FORMAT_V1 + "'");
        }
        String format = formatNode.asText();
        if (!PIPELINE_FORMAT_V1.equals(format)) {
            throw new PipelineDefinitionException("",
                    "unsupported pipeline definition format '" + format + "'");
        }
    }

    private static Definition readDefinitionEnvelope(ObjectNode envelope) {
        requirePipelineFormat(envelope);
        Pipeline pipeline = decodePipeline(textOrNull(envelope, PIPELINE_BASE64_KEY));
        Map<String, FileDescriptor> allBuilt = decodeDescriptorMap(
                textOrNull(envelope, DESCRIPTOR_SET_BASE64_KEY));
        List<FileDescriptor> files = selectRootFiles(envelope.get(DESCRIPTOR_FILES_KEY), allBuilt);
        verifyPipeline(pipeline, files);

        String storedPipelineFp = textOrNull(envelope, PIPELINE_FINGERPRINT_KEY);
        String computedPipelineFp = PipelineExecutor.pipelineFingerprint(pipeline, files);
        if (!computedPipelineFp.equals(storedPipelineFp)) {
            throw new PipelineDefinitionException("",
                    "stored pipeline fingerprint does not match reconstructed pipeline and descriptors");
        }

        DescriptorRegistry registry = buildRegistry(files, null);
        Descriptor inputType = registry.findDescriptorByFullName(pipeline.getInputType());
        String inputBase64 = textOrNull(envelope, INPUT_BASE64_KEY);
        if (inputBase64 == null) {
            throw new PipelineDefinitionException("",
                    "stored pipeline definition is missing inputBase64");
        }
        DynamicMessage input = decodeInput(inputType, inputBase64);
        String storedInputFp = textOrNull(envelope, INPUT_FINGERPRINT_KEY);
        String computedInputFp = PipelineExecutor.inputFingerprint(input);
        if (!computedInputFp.equals(storedInputFp)) {
            throw new PipelineDefinitionException("",
                    "stored input fingerprint does not match decoded input bytes");
        }
        return new Definition(pipeline, files, input, computedPipelineFp, computedInputFp);
    }

    private static DynamicMessage decodeInput(Descriptor inputType, String inputBase64) {
        try {
            byte[] rawInput = Base64.getDecoder().decode(inputBase64);
            return DynamicMessage.parseFrom(inputType, rawInput);
        } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "stored pipeline inputBase64 cannot be decoded as "
                            + inputType.getFullName() + ": " + e.getMessage(), e);
        }
    }

    private static String textOrNull(ObjectNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw new PipelineDefinitionException("",
                    "pipeline envelope field '" + field + "' must be a string");
        }
        return value.asText();
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
                        "descriptor file '" + nameNode.asText()
                                + "' is absent from descriptorSetBase64");
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
}
