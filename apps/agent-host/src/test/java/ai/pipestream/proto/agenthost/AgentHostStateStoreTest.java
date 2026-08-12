package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AgentHostStateStoreTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path temporary;

    @Test
    void roundTripsProviderSessionCursorAndPendingCommand() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        AgentHostStateStore store = new AgentHostStateStore(temporary.resolve("host/state.json"));
        AgentHostState initial = store.loadOrCreate("kimi", AgentRole.WORKER,
                "kimi", workspace);
        AgentTurn.Command command = new AgentTurn.Command("host-ack",
                MAPPER.createObjectNode().put("reason", "observed"));
        AgentHostState changed = initial.withProviderSession("session-7")
                .withCursor(12)
                .withPending(new AgentHostState.PendingTurn(
                        14, List.of(14L), List.of(command), 0, false));
        store.save(changed);

        AgentHostState restored = store.loadOrCreate("kimi", AgentRole.WORKER,
                "kimi", workspace);
        assertThat(restored.providerSessionId()).isEqualTo("session-7");
        assertThat(restored.cursor()).isEqualTo(12);
        assertThat(restored.pending().commands().get(0).arguments().path("reason").asText())
                .isEqualTo("observed");
        assertThat(Files.exists(temporary.resolve("host/state.json.tmp"))).isFalse();
    }

    @Test
    void refusesToReuseStateForAnotherIdentity() throws Exception {
        Path workspace = Files.createDirectory(temporary.resolve("workspace"));
        AgentHostStateStore store = new AgentHostStateStore(temporary.resolve("state.json"));
        store.loadOrCreate("kimi", AgentRole.WORKER, "kimi", workspace);
        assertThatThrownBy(() -> store.loadOrCreate(
                "codex", AgentRole.WORKER, "kimi", workspace))
                .isInstanceOf(AgentHostException.class)
                .hasMessageContaining("different host identity");
    }
}
