package ai.protomolt.proto.agenthost;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Google's Antigravity CLI ({@code agy}) driven one process per turn over its NDJSON pipe:
 * the prompt goes in as one {@code user} line on stdin, the conversation is resumed with
 * {@code --conversation}, and the final answer is the {@code result} event, schema-enforced
 * through {@code --json-schema} so the reply is the host's closed command batch rather than
 * prose around it. Token usage is read from each result; because every turn is its own
 * process the numbers are per turn and are summed here.
 *
 * <p>Requires Antigravity CLI 1.1.24 or later: earlier builds hang on exit when both stdout
 * and stderr are pipes, which is what a child process launched from here always has.</p>
 */
final class AntigravityAgentProvider implements AgentProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path workspace;
    private final Path schema;
    private final List<String> executable;
    private final String model;
    private final Duration timeout;

    private String conversationId;
    private Usage usage = new Usage(0, 0);

    AntigravityAgentProvider(Path workspace, Path statePath, String savedConversationId,
                             AgentRole role, List<String> executable, String model,
                             Duration timeout) {
        this.workspace = workspace.toAbsolutePath().normalize();
        Path stateDirectory = statePath.toAbsolutePath().normalize().getParent();
        if (stateDirectory == null) {
            throw new IllegalArgumentException("state path needs a parent directory");
        }
        this.schema = stateDirectory.resolve(statePath.getFileName() + ".schema.json");
        this.conversationId = savedConversationId == null ? "" : savedConversationId;
        this.executable = List.copyOf(executable);
        this.model = model;
        this.timeout = timeout;
        writeSchema(role);
    }

    @Override
    public String name() {
        return "antigravity";
    }

    @Override
    public synchronized String sessionId() {
        return conversationId;
    }

    @Override
    public synchronized Optional<Usage> usage() {
        return Optional.of(usage);
    }

    @Override
    public synchronized String prompt(String prompt) {
        List<String> command = command();
        Process process = null;
        try (ExecutorService readers = Executors.newVirtualThreadPerTaskExecutor()) {
            process = new ProcessBuilder(command).directory(workspace.toFile()).start();
            Process child = process;
            ObjectNode line = MAPPER.createObjectNode();
            line.put("event", "user");
            line.putObject("message").put("content", prompt);
            child.getOutputStream().write(MAPPER.writeValueAsBytes(line));
            child.getOutputStream().write('\n');
            child.getOutputStream().close();
            Future<byte[]> stdout = readers.submit(
                    () -> ProcessOutput.readBounded(child.getInputStream()));
            Future<byte[]> stderr = readers.submit(
                    () -> ProcessOutput.readBounded(child.getErrorStream()));
            if (!child.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                child.destroyForcibly();
                throw new AgentHostException("Antigravity turn exceeded " + timeout);
            }
            byte[] out = ProcessOutput.get(stdout, "stdout");
            byte[] err = ProcessOutput.get(stderr, "stderr");
            if (child.exitValue() != 0) {
                throw new AgentHostException("Antigravity exited with " + child.exitValue()
                        + ": " + ProcessOutput.tail(err));
            }
            return finalAnswer(out);
        } catch (IOException e) {
            throw new AgentHostException("could not run Antigravity", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (process != null) {
                process.destroyForcibly();
            }
            throw new AgentHostException("Antigravity turn was interrupted", e);
        }
    }

    private List<String> command() {
        if (executable.isEmpty()) {
            throw new AgentHostException("Antigravity command is empty");
        }
        List<String> command = new ArrayList<>(executable);
        command.addAll(List.of("--input-format", "stream-json", "--output-format", "stream-json",
                "--json-schema", schema.toString(), "--dangerously-skip-permissions"));
        if (model != null && !model.isBlank()) {
            command.addAll(List.of("--model", model));
        }
        if (!conversationId.isBlank()) {
            command.addAll(List.of("--conversation", conversationId));
        }
        return command;
    }

    /**
     * The terminal {@code result} event of the stream. Its parsed {@code structured_output}
     * is the answer when the schema was enforced; otherwise the {@code response} text is.
     * Every event carries the conversation id, which is saved so the next turn resumes it.
     */
    private String finalAnswer(byte[] out) {
        JsonNode result = null;
        for (String line : new String(out, StandardCharsets.UTF_8).split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode event;
            try {
                event = MAPPER.readTree(line);
            } catch (IOException e) {
                continue;
            }
            String id = event.path("conversation_id").asText("");
            if (id.isEmpty()) {
                id = event.path("step_update").path("conversation_id").asText("");
            }
            if (id.isEmpty()) {
                id = event.path("result").path("conversation_id").asText("");
            }
            if (!id.isEmpty()) {
                conversationId = id;
            }
            if ("result".equals(event.path("event").asText())) {
                result = event.path("result");
            }
        }
        if (result == null) {
            throw new AgentHostException("Antigravity ended the stream without a result");
        }
        JsonNode counted = result.path("usage");
        usage = usage.plus(counted.path("input_tokens").asLong(),
                counted.path("output_tokens").asLong());
        String status = result.path("status").asText("");
        if (!"SUCCESS".equals(status)) {
            throw new AgentHostException("Antigravity turn ended with status " + status
                    + (result.hasNonNull("error") ? ": " + result.path("error").asText() : ""));
        }
        JsonNode structured = result.path("structured_output");
        if (structured.isObject()) {
            return structured.toString();
        }
        String response = result.path("response").asText("").trim();
        if (response.isEmpty()) {
            throw new AgentHostException("Antigravity returned no final message");
        }
        return response;
    }

    private void writeSchema(AgentRole role) {
        try {
            Files.createDirectories(schema.getParent());
            Files.writeString(schema, AgentTurn.outputSchema(role).toString());
        } catch (IOException e) {
            throw new AgentHostException("could not write the Antigravity output schema", e);
        }
    }

    @Override
    public void close() {
        // Each turn is its own process; nothing outlives a prompt.
    }
}
