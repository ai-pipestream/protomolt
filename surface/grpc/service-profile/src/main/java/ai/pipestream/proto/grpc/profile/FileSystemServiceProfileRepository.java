package ai.pipestream.proto.grpc.profile;

import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Binary-protobuf filesystem repository with content-addressed descriptor artifacts.
 *
 * <p>Each replacement is written beside its target and moved into place, so readers see either
 * the old complete message or the new complete message. Profile names and artifact fingerprints
 * are validated before they can become path components.</p>
 */
public final class FileSystemServiceProfileRepository implements ServiceProfileRepository {

    private static final String PROFILE_SUFFIX = ".pb";
    private static final String ARTIFACT_SUFFIX = ".pb";
    private static final int MAX_PROFILES = 256;

    private final Path profiles;
    private final Path artifacts;

    /** Creates a repository rooted at {@code root}, creating its storage directories if needed. */
    public FileSystemServiceProfileRepository(Path root) throws IOException {
        if (root == null) {
            throw new IllegalArgumentException("root must not be null");
        }
        Path normalized = root.toAbsolutePath().normalize();
        Files.createDirectories(normalized);
        profiles = normalized.resolve("profiles");
        artifacts = normalized.resolve("descriptor-artifacts");
        Files.createDirectories(profiles);
        Files.createDirectories(artifacts);
    }

    @Override
    public Optional<ServiceProfile> find(String name) throws IOException {
        ServiceProfileValidation.validateName(name, "name");
        Path path = profiles.resolve(name + PROFILE_SUFFIX);
        if (!isDirectChild(path, profiles) || !Files.isRegularFile(path)) {
            return Optional.empty();
        }
        try {
            ServiceProfile profile = ServiceProfile.parseFrom(readBounded(path,
                    ServiceProfileValidation.MAX_PROFILE_BYTES, "service profile"));
            ServiceProfileValidation.validate(profile);
            if (!profile.getName().equals(name)) {
                throw new IOException("profile name does not match its storage path: " + name);
            }
            return Optional.of(profile);
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new IOException("invalid service profile stored at " + path, e);
        }
    }

    @Override
    public List<ServiceProfile> list() throws IOException {
        List<Path> paths = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(profiles, "*" + PROFILE_SUFFIX)) {
            for (Path path : stream) {
                if (Files.isRegularFile(path) && isDirectChild(path, profiles)) {
                    if (paths.size() == MAX_PROFILES) {
                        throw new IOException("service workspace exceeds the maximum of "
                                + MAX_PROFILES + " profiles");
                    }
                    paths.add(path);
                }
            }
        }
        paths.sort(Comparator.comparing(path -> path.getFileName().toString()));
        List<ServiceProfile> result = new ArrayList<>(paths.size());
        for (Path path : paths) {
            String fileName = path.getFileName().toString();
            result.add(find(fileName.substring(0, fileName.length() - PROFILE_SUFFIX.length()))
                    .orElseThrow(() -> new IOException("profile disappeared while listing: " + path)));
        }
        return List.copyOf(result);
    }

    @Override
    public void save(ServiceProfile profile) throws IOException {
        try {
            ServiceProfileValidation.validate(profile);
        } catch (IllegalArgumentException e) {
            throw e;
        }
        Path path = profiles.resolve(profile.getName() + PROFILE_SUFFIX);
        if (!isDirectChild(path, profiles)) {
            throw new IllegalArgumentException("profile name escapes repository root");
        }
        writeAtomically(path, profile.toByteArray());
    }

    @Override
    public Optional<DescriptorArtifact> findDescriptorArtifact(String fingerprint) throws IOException {
        ServiceProfileValidation.validateFingerprint(fingerprint);
        Path path = artifacts.resolve(fingerprint + ARTIFACT_SUFFIX);
        if (!isDirectChild(path, artifacts) || !Files.isRegularFile(path)) {
            return Optional.empty();
        }
        try {
            DescriptorArtifact artifact = DescriptorArtifact.parseFrom(readBounded(path,
                    ServiceProfileValidation.MAX_DESCRIPTOR_ARTIFACT_BYTES
                            + ServiceProfileValidation.MAX_PROFILE_BYTES,
                    "descriptor artifact"));
            ServiceProfileValidation.validate(artifact);
            if (!artifact.getFingerprint().equals(fingerprint)) {
                throw new IOException("descriptor artifact fingerprint does not match its path: "
                        + fingerprint);
            }
            return Optional.of(artifact);
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new IOException("invalid descriptor artifact stored at " + path, e);
        }
    }

    @Override
    public void saveDescriptorArtifact(DescriptorArtifact artifact) throws IOException {
        ServiceProfileValidation.validate(artifact);
        Path path = artifacts.resolve(artifact.getFingerprint() + ARTIFACT_SUFFIX);
        if (!isDirectChild(path, artifacts)) {
            throw new IllegalArgumentException("artifact fingerprint escapes repository root");
        }
        writeAtomically(path, artifact.toByteArray());
    }

    private static boolean isDirectChild(Path path, Path parent) {
        return path.getParent().equals(parent) && path.getFileName() != null;
    }

    private static byte[] readBounded(Path path, long maximumBytes, String kind) throws IOException {
        long size = Files.size(path);
        if (size > maximumBytes) {
            throw new IOException(kind + " at " + path + " exceeds the maximum size of "
                    + maximumBytes + " bytes");
        }
        return Files.readAllBytes(path);
    }

    private static void writeAtomically(Path target, byte[] bytes) throws IOException {
        Path parent = target.getParent();
        Files.createDirectories(parent);
        Path temporary = parent.resolve("." + target.getFileName() + ".tmp-" + UUID.randomUUID());
        try {
            Files.write(temporary, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }
}
