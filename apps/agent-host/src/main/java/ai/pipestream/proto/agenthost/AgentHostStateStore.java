package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

/** Atomic JSON state file with owner-only permissions where POSIX modes are available. */
final class AgentHostStateStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Set<PosixFilePermission> OWNER_ONLY = Set.of(
            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);

    private final Path path;

    AgentHostStateStore(Path path) {
        this.path = path.toAbsolutePath().normalize();
    }

    AgentHostState loadOrCreate(String identity, AgentRole role, String provider,
                                Path workspace) {
        AgentHostState state;
        if (Files.notExists(path)) {
            state = AgentHostState.initial(identity, role, provider,
                    workspace.toAbsolutePath().normalize().toString());
            save(state);
            return state;
        }
        try {
            state = MAPPER.readValue(path.toFile(), AgentHostState.class);
        } catch (IOException | RuntimeException e) {
            throw new AgentHostException("agent state is unreadable: " + path, e);
        }
        String normalizedWorkspace = workspace.toAbsolutePath().normalize().toString();
        if (!state.identity().equals(identity) || state.role() != role
                || !state.provider().equals(provider)
                || !state.workspace().equals(normalizedWorkspace)) {
            throw new AgentHostException("agent state belongs to a different host identity");
        }
        return state;
    }

    void save(AgentHostState state) {
        Path parent = path.getParent();
        if (parent == null) {
            throw new AgentHostException("agent state path needs a parent directory");
        }
        Path temporary = parent.resolve(path.getFileName() + ".tmp");
        try {
            Files.createDirectories(parent);
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(temporary.toFile(), state);
            ownerOnly(temporary);
            try {
                Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
            }
            ownerOnly(path);
        } catch (IOException e) {
            throw new AgentHostException("could not persist agent state: " + path, e);
        }
    }

    private static void ownerOnly(Path target) throws IOException {
        if (Files.getFileStore(target).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(target, OWNER_ONLY);
        }
    }
}
