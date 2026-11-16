package ai.protomolt.proto.jobs.service;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.Definition;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.pipeline.PipelineChecker;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineRunDefinition;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.ByteString;
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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds and reads the {@link PipelineRunDefinition} a durable pipeline run stores in
 * {@code workflow_run.pipeline_definition_pb}: the pipeline, its descriptor closure, the input
 * bytes and both fingerprints. Reading rebuilds the descriptors, re-checks the pipeline and
 * recomputes both fingerprints, and refuses a definition that does not reproduce what it
 * claims.
 */
final class PipelineRunCodec {

    private PipelineRunCodec() {
    }

    static PipelineRunDefinition encodeDefinition(Pipeline pipeline, List<FileDescriptor> files,
                                                  DynamicMessage canonicalInput) {
        List<FileDescriptor> sortedRootFiles = files.stream()
                .sorted(Comparator.comparing(FileDescriptor::getName))
                .toList();
        Map<String, FileDescriptorProto> closure = new LinkedHashMap<>();
        for (FileDescriptor file : sortedRootFiles) {
            collectTopological(file, closure);
        }
        PipelineRunDefinition.Builder definition = PipelineRunDefinition.newBuilder()
                .setPipeline(pipeline)
                .setDescriptorSet(FileDescriptorSet.newBuilder().addAllFile(closure.values()))
                .setInput(ByteString.copyFrom(
                        MessageFingerprints.deterministicBytes(canonicalInput)))
                .setPipelineFingerprint(
                        PipelineExecutor.pipelineFingerprint(pipeline, sortedRootFiles))
                .setInputFingerprint(PipelineExecutor.inputFingerprint(canonicalInput));
        for (FileDescriptor file : sortedRootFiles) {
            definition.addRootFiles(file.getName());
        }
        PipelineRunDefinition encoded = definition.build();
        // Refuse here what readDefinition would refuse, so nothing unreadable is stored.
        requireValid(encoded);
        return encoded;
    }

    static Definition readDefinition(WorkflowRunRecord job) {
        if (job == null || job.pipelineDefinition == null) {
            throw new PipelineDefinitionException("", "job has no stored pipeline definition");
        }
        PipelineRunDefinition stored;
        try {
            stored = PipelineRunDefinition.parseFrom(job.pipelineDefinition);
        } catch (InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "stored pipeline definition is not a readable PipelineRunDefinition: "
                            + e.getMessage(), e);
        }
        return readDefinition(stored);
    }

    static Definition readDefinition(PipelineRunDefinition stored) {
        requireValid(stored);
        Pipeline pipeline = stored.getPipeline();
        Map<String, FileDescriptor> allBuilt = buildDescriptors(stored.getDescriptorSet());
        List<FileDescriptor> files = selectRootFiles(stored.getRootFilesList(), allBuilt);
        verifyPipeline(pipeline, files);
        String computedPipelineFp = PipelineExecutor.pipelineFingerprint(pipeline, files);
        if (!computedPipelineFp.equals(stored.getPipelineFingerprint())) {
            throw new PipelineDefinitionException("",
                    "stored pipeline fingerprint does not match reconstructed pipeline and descriptors");
        }
        DescriptorRegistry registry = buildRegistry(files, null);
        Descriptor inputType = registry.findDescriptorByFullName(pipeline.getInputType());
        DynamicMessage input = decodeInput(inputType, stored.getInput());
        String computedInputFp = PipelineExecutor.inputFingerprint(input);
        if (!computedInputFp.equals(stored.getInputFingerprint())) {
            throw new PipelineDefinitionException("",
                    "stored input fingerprint does not match decoded input bytes");
        }
        return new Definition(pipeline, files, input, computedPipelineFp, computedInputFp);
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
            // The run is defined against the descriptors it was submitted with. A type the
            // supplemental registry also defines, under any file name, never replaces them.
            for (Descriptor descriptor : extraRegistry.registeredDescriptors()) {
                if (!registry.isRegistered(descriptor.getFullName())) {
                    registry.register(descriptor);
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

    private static void requireValid(PipelineRunDefinition stored) {
        ValidationResult result =
                ProtoValidator.forMessageType(PipelineRunDefinition.getDescriptor())
                        .validate(stored);
        if (!result.valid()) {
            ValidationResult.Violation first = result.violations().getFirst();
            throw new PipelineDefinitionException("",
                    "pipeline run definition is invalid at " + first.path() + ": "
                            + first.message());
        }
    }

    private static DynamicMessage decodeInput(Descriptor inputType, ByteString input) {
        try {
            return DynamicMessage.parseFrom(inputType, input);
        } catch (InvalidProtocolBufferException e) {
            throw new PipelineDefinitionException("",
                    "stored pipeline input cannot be decoded as "
                            + inputType.getFullName() + ": " + e.getMessage(), e);
        }
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

    private static Map<String, FileDescriptor> buildDescriptors(FileDescriptorSet set) {
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

    private static List<FileDescriptor> selectRootFiles(List<String> rootFiles,
                                                        Map<String, FileDescriptor> allBuilt) {
        List<FileDescriptor> selected = new ArrayList<>(rootFiles.size());
        for (String name : rootFiles) {
            FileDescriptor found = allBuilt.get(name);
            if (found == null) {
                throw new PipelineDefinitionException("",
                        "root file '" + name + "' is absent from the stored descriptor set");
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
