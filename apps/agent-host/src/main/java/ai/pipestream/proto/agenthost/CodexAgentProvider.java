package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** A resumable {@code codex exec --json} session with schema-constrained final output. */
final class CodexAgentProvider implements AgentProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_PROCESS_OUTPUT = 4 * 1024 * 1024;

    private final Path workspace;
    private final Path schema;
    private final Path outputDirectory;
    private final List<String> executable;
    private final Duration timeout;
    private final String model;

    private String sessionId;

    CodexAgentProvider(Path workspace, Path statePath, String savedSessionId,
                       AgentRole role, List<String> executable, String model,
                       Duration timeout) {
        this.workspace = workspace.toAbsolutePath().normalize();
        this.outputDirectory = statePath.toAbsolutePath().normalize().getParent();
        if (outputDirectory == null) {
            throw new IllegalArgumentException("state path needs a parent directory");
        }
        this.schema = outputDirectory.resolve(statePath.getFileName() + ".schema.json");
        this.sessionId = savedSessionId == null ? "" : savedSessionId;
        this.executable = List.copyOf(executable);
        this.model = model;
        this.timeout = timeout;
        writeSchema(role);
    }

    @Override
    public String name() {
        return "codex";
    }

    @Override
    public synchronized String sessionId() {
        return sessionId;
    }

    @Override
    public synchronized String prompt(String prompt) {
        Path output;
        try {
            output = Files.createTempFile(outputDirectory, "codex-agent-turn-", ".json");
        } catch (IOException e) {
            throw new AgentHostException("could not create Codex output file", e);
        }
        List<String> command = command(output);
        Process process = null;
        try (ExecutorService readers = java.util.concurrent.Executors
                .newVirtualThreadPerTaskExecutor()) {
            process = new ProcessBuilder(command).directory(workspace.toFile()).start();
            Process child = process;
            child.getOutputStream().write(prompt.getBytes(StandardCharsets.UTF_8));
            child.getOutputStream().close();
            Future<byte[]> stdout = readers.submit(
                    () -> readBounded(child.getInputStream(), MAX_PROCESS_OUTPUT));
            Future<byte[]> stderr = readers.submit(
                    () -> readBounded(child.getErrorStream(), MAX_PROCESS_OUTPUT));
            if (!child.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                child.destroyForcibly();
                throw new AgentHostException("Codex turn exceeded " + timeout);
            }
            byte[] out = get(stdout, "stdout");
            byte[] err = get(stderr, "stderr");
            if (child.exitValue() != 0) {
                throw new AgentHostException("Codex exited with " + child.exitValue()
                        + ": " + boundedMessage(err));
            }
            captureSessionId(out);
            String response = Files.readString(output).trim();
            if (response.isEmpty()) {
                throw new AgentHostException("Codex returned no final message");
            }
            return response;
        } catch (IOException e) {
            throw new AgentHostException("could not run Codex", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (process != null) {
                process.destroyForcibly();
            }
            throw new AgentHostException("Codex turn was interrupted", e);
        } finally {
            try {
                Files.deleteIfExists(output);
            } catch (IOException ignored) {
                // The bounded final response contains no credential material. A stale temp
                // file can be cleaned on the next host maintenance pass.
            }
        }
    }

    private List<String> command(Path output) {
        if (executable.isEmpty()) {
            throw new AgentHostException("Codex command is empty");
        }
        List<String> command = new ArrayList<>(executable);
        if (sessionId.isBlank()) {
            command.addAll(List.of("exec", "--json", "--color", "never",
                    "--approve-for-me", "-C", workspace.toString()));
            if (model != null) {
                command.addAll(List.of("--model", model));
            }
            command.addAll(List.of("--output-schema", schema.toString(),
                    "--output-last-message", output.toString(), "-"));
        } else {
            command.addAll(List.of("exec", "resume", "--json"));
            if (model != null) {
                command.addAll(List.of("--model", model));
            }
            command.addAll(List.of("--output-schema", schema.toString(),
                    "--output-last-message", output.toString(), sessionId, "-"));
        }
        return command;
    }

    private void captureSessionId(byte[] output) {
        String text = new String(output, StandardCharsets.UTF_8);
        for (String line : text.split("\\R")) {
            if (line.isBlank()) {
                continue;
            }
            try {
                JsonNode event = MAPPER.readTree(line);
                if ("thread.started".equals(event.path("type").asText())
                        && event.path("thread_id").isTextual()) {
                    sessionId = event.path("thread_id").asText();
                }
            } catch (IOException e) {
                throw new AgentHostException("Codex JSON event stream is invalid", e);
            }
        }
        if (sessionId.isBlank()) {
            throw new AgentHostException("Codex returned no thread id");
        }
    }

    private void writeSchema(AgentRole role) {
        try {
            Files.createDirectories(outputDirectory);
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(
                    schema.toFile(), AgentTurn.outputSchema(role));
        } catch (IOException e) {
            throw new AgentHostException("could not write Codex output schema", e);
        }
    }

    private static byte[] readBounded(InputStream input, int limit) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer)) >= 0) {
            if (output.size() + count > limit) {
                throw new IOException("process output exceeded " + limit + " bytes");
            }
            output.write(buffer, 0, count);
        }
        return output.toByteArray();
    }

    private static byte[] get(Future<byte[]> future, String stream) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AgentHostException("interrupted reading Codex " + stream, e);
        } catch (ExecutionException e) {
            throw new AgentHostException("could not read Codex " + stream, e.getCause());
        }
    }

    private static String boundedMessage(byte[] bytes) {
        String message = new String(bytes, StandardCharsets.UTF_8).trim();
        return message.length() > 2_000 ? message.substring(0, 2_000) : message;
    }

    @Override
    public void close() {
        // One Codex process is launched per turn. The persisted thread id carries continuity.
    }
}
