package ai.protomolt.proto.samples;

import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.validate.ProtoValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Seeds the fixed sample reviewer policy before an authoring coordinator starts. */
public final class AuthoringPolicySeed {
    private static final String OPERATION_ID = "00000000-0000-4000-8000-000000000442";
    private static final String MEDIA_TYPE = "application/x-protobuf";
    private static final String MANIFEST = ".authoring-policy-seed-v1.json";
    private static final String LOCK = ".authoring-policy-seed.lock";

    private AuthoringPolicySeed() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("usage: authoring-policy-seed <workspace> <artifact-dir> <fixture-target> <policy-sha-file>");
        }
        seed(Path.of(args[0]), Path.of(args[1]), args[2], Path.of(args[3]));
    }

    /** Package-local for deterministic filesystem tests. */
    static String seed(Path suppliedWorkspace, Path suppliedArtifacts, String target, Path suppliedShaFile)
            throws Exception {
        requireFixtureTarget(target);
        Files.createDirectories(suppliedWorkspace);
        Path workspace = suppliedWorkspace.toRealPath();
        Path artifactsPath = inside(workspace, suppliedArtifacts, true);
        Path shaFile = inside(workspace, suppliedShaFile, false);
        if (shaFile.equals(workspace.resolve(MANIFEST)) || shaFile.equals(workspace.resolve(LOCK))) {
            throw new IllegalArgumentException("policy SHA path is reserved");
        }
        if (artifactsPath.equals(workspace) || shaFile.equals(workspace)
                || shaFile.startsWith(artifactsPath) || artifactsPath.startsWith(shaFile)) {
            throw new IllegalArgumentException("policy paths overlap");
        }
        PolicyBytes policy = policy(target);
        byte[] intent = intent(workspace, artifactsPath, shaFile, target, policy);
        Path manifest = workspace.resolve(MANIFEST);
        try (FileChannel channel = FileChannel.open(workspace.resolve(LOCK),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock ignored = channel.lock()) {
            if (Files.exists(manifest, LinkOption.NOFOLLOW_LINKS)) {
                requireRegular(manifest);
                if (Files.size(manifest) != intent.length || !Arrays.equals(Files.readAllBytes(manifest), intent)) {
                    throw new IllegalStateException("authoring policy seed intent differs from workspace manifest");
                }
            } else {
                if (Files.exists(shaFile, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalStateException("policy ready file exists without seed intent");
                }
                publish(manifest, intent);
            }

            boolean ready = Files.exists(shaFile, LinkOption.NOFOLLOW_LINKS);
            if (ready) {
                requireRegular(shaFile);
                if (Files.size(shaFile) != 65 || !Arrays.equals(Files.readAllBytes(shaFile), readyBytes(policy.sha256()))) {
                    throw new IllegalStateException("authoring policy SHA differs from ready file");
                }
            }
            Files.createDirectories(artifactsPath);
            var artifacts = new FileSystemArtifactRepository(artifactsPath);
            ensureArtifact(artifacts, policy.descriptors(), ready);
            ensureArtifact(artifacts, policy.input(), ready);
            ensureArtifact(artifacts, policy.output(), ready);
            ensureArtifact(artifacts, policy.policy(), ready);
            if (!ready) {
                Files.createDirectories(shaFile.getParent());
                publish(shaFile, readyBytes(policy.sha256()));
            }
            return policy.sha256();
        }
    }

    private record ArtifactBytes(ArtifactReference reference, byte[] bytes) {}
    private record PolicyBytes(ArtifactBytes descriptors, ArtifactBytes input,
            ArtifactBytes output, ArtifactBytes policy) {
        String sha256() { return policy.reference().getSha256(); }
    }

    private static PolicyBytes policy(String target) throws Exception {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(WriteRecordRequest.getDescriptor().getFile(), files);
        FileDescriptorSet.Builder closure = FileDescriptorSet.newBuilder();
        files.values().forEach(file -> closure.addFile(file.toProto()));
        FileDescriptorSet set = closure.build();
        // Descriptor options may carry imported custom extensions. Validate
        // closure and compilation rather than discarding those option bytes.
        var names = new java.util.HashSet<String>();
        for (var file : set.getFileList()) {
            if (file.getName().isBlank() || !names.add(file.getName())) {
                throw new IllegalArgumentException("policy descriptor filenames are duplicated");
            }
        }
        for (var file : set.getFileList()) {
            if (!names.containsAll(file.getDependencyList())) {
                throw new IllegalArgumentException("policy descriptor closure is incomplete");
            }
        }
        if (set.getFileCount() == 0 || GoogleDescriptorLoader.fromDescriptorSet(set).isEmpty()) {
            throw new IllegalArgumentException("policy descriptor closure is empty");
        }
        ArtifactBytes descriptors = artifact(set.toByteArray());

        String expectedContent = "fixture\n record";
        WriteRecordRequest input = WriteRecordRequest.newBuilder().setOperationId(OPERATION_ID)
                .setContent(" \tfixture\r\n record\t ").build();
        WriteRecordResponse output = WriteRecordResponse.newBuilder().setOperationId(OPERATION_ID)
                .setContentSha256(sha(expectedContent.getBytes(java.nio.charset.StandardCharsets.UTF_8))).build();
        validate(input);
        validate(output);
        ArtifactBytes inputBytes = artifact(input.toByteArray());
        ArtifactBytes outputBytes = artifact(output.toByteArray());
        WorkflowAuthoringPolicy policy = WorkflowAuthoringPolicy.newBuilder()
                .setDescriptors(descriptors.reference())
                .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("remote-smoke")
                        .setInput(inputBytes.reference()).setExpectedOutput(outputBytes.reference()))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder().setTarget(target)
                        .setMethod("ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/NormalizeText"))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder().setTarget(target)
                        .setMethod("ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/WriteRecord"))
                .build();
        validate(policy);
        return new PolicyBytes(descriptors, inputBytes, outputBytes, artifact(policy.toByteArray()));
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }

    private static ArtifactBytes artifact(byte[] bytes) {
        return new ArtifactBytes(ArtifactReference.newBuilder().setSha256(sha(bytes))
                .setMediaType(MEDIA_TYPE).setSizeBytes(bytes.length).setRedacted(false).build(), bytes);
    }

    private static void ensureArtifact(FileSystemArtifactRepository artifacts, ArtifactBytes expected,
            boolean ready) throws IOException {
        var existing = artifacts.find(expected.reference().getSha256());
        if (existing.isEmpty()) {
            if (ready) throw new IllegalStateException("ready authoring policy artifact is missing");
            ArtifactReference saved = artifacts.save(expected.bytes(), MEDIA_TYPE, false);
            if (!saved.equals(expected.reference())) {
                throw new IllegalStateException("saved authoring artifact metadata differs");
            }
            existing = artifacts.find(expected.reference().getSha256());
        }
        var stored = existing.orElseThrow(() -> new IllegalStateException("saved authoring artifact is missing"));
        if (!stored.reference().equals(expected.reference())
                || !Arrays.equals(stored.content(), expected.bytes())) {
            throw new IllegalStateException("stored authoring artifact differs from seed intent");
        }
    }

    private static byte[] intent(Path workspace, Path artifacts, Path shaFile, String target,
            PolicyBytes policy) throws IOException {
        var json = new ObjectMapper().createObjectNode();
        json.put("version", 1);
        json.put("fixtureTarget", target);
        json.put("artifactDirectory", workspace.relativize(artifacts).toString());
        json.put("shaFile", workspace.relativize(shaFile).toString());
        json.put("descriptorsSha256", policy.descriptors().reference().getSha256());
        json.put("inputSha256", policy.input().reference().getSha256());
        json.put("expectedOutputSha256", policy.output().reference().getSha256());
        json.put("policySha256", policy.sha256());
        return new ObjectMapper().writeValueAsBytes(json);
    }

    private static byte[] readyBytes(String sha) {
        return (sha + "\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    }

    private static void publish(Path target, byte[] bytes) throws IOException {
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("authoring policy file already exists");
        }
        Path temp = Files.createTempFile(target.getParent(), ".authoring-policy-seed-", ".tmp");
        try {
            try (FileChannel output = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) output.write(buffer);
                output.force(true);
            }
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE);
            forceDirectory(target.getParent());
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void requireRegular(Path path) {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("authoring policy file is not a regular file");
        }
    }

    private static Path inside(Path workspace, Path supplied, boolean directory) throws IOException {
        Path normalized = supplied.toAbsolutePath().normalize();
        if (!normalized.startsWith(workspace)) throw new IllegalArgumentException("policy path leaves workspace");
        for (Path part = normalized; !part.equals(workspace); part = part.getParent()) {
            if (Files.isSymbolicLink(part)) {
                throw new IllegalArgumentException("policy path contains a symbolic link");
            }
        }
        if (Files.exists(normalized, LinkOption.NOFOLLOW_LINKS)) {
            if (Files.isSymbolicLink(normalized) || (directory && !Files.isDirectory(normalized))) {
                throw new IllegalArgumentException("policy path is not a directory or is a symbolic link");
            }
            if (!normalized.toRealPath().startsWith(workspace)) {
                throw new IllegalArgumentException("policy path leaves workspace");
            }
        }
        return normalized;
    }

    private static void requireFixtureTarget(String target) {
        if (target == null || target.length() > 512
                || !target.matches("[A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}")) {
            throw new IllegalArgumentException("fixture target must be host:port");
        }
        int colon = target.lastIndexOf(':');
        if (colon < 1 || colon == target.length() - 1 || target.substring(0, colon).contains(":")) {
            throw new IllegalArgumentException("fixture target must be host:port");
        }
        try {
            int port = Integer.parseInt(target.substring(colon + 1));
            if (port >= 1 && port <= 65535) return;
        } catch (NumberFormatException ignored) {
            // Reject malformed targets before publishing an intent.
        }
        throw new IllegalArgumentException("fixture target port is invalid");
    }

    private static void validate(Message message) {
        rejectUnknown(message);
        if (!ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message).valid()) {
            throw new IllegalArgumentException("authoring policy seed contract is invalid");
        }
    }

    private static void rejectUnknown(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("authoring policy seed has unknown fields");
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) rejectUnknown((Message) nested);
            } else {
                rejectUnknown((Message) field.getValue());
            }
        }
    }

    private static String sha(byte[] bytes) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
