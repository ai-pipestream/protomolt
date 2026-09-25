package ai.protomolt.proto.acp.agent;

import ai.protomolt.proto.acp.PromptContext;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.service.ProtoMoltCatalog;
import ai.protomolt.proto.grpc.service.ProtoMoltGrpcServer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class RemoteCatalogLineRunnerTest {

    @Test
    void forwardsACommandWithTheOperatorCredentialAndRejectsWrongCredentials() {
        String token = "remote-test-token";
        try (ProtoMoltGrpcServer server = ProtoMoltGrpcServer.start("127.0.0.1", 0,
                ProtoMoltCatalog.full(ActionContext.create()), token)) {
            String target = "127.0.0.1:" + server.port();
            try (RemoteCatalogLineRunner authorized =
                         RemoteCatalogLineRunner.connect(target, false, token)) {
                RecordingContext result = new RecordingContext();
                authorized.run("list-types {}", result);
                assertThat(result.messages).hasSize(1);
                assertThat(result.messages.get(0)).contains("types");
                assertThat(result.messages.get(0)).doesNotContain(token);
            }
            try (RemoteCatalogLineRunner refused =
                         RemoteCatalogLineRunner.connect(target, false, "wrong-token")) {
                RecordingContext result = new RecordingContext();
                refused.run("list-types {}", result);
                assertThat(result.messages).containsExactly(
                        "UNAUTHENTICATED: remote coordinator refused or could not complete the command");
            }
        }
    }

    private static final class RecordingContext implements PromptContext {
        private final List<String> messages = new ArrayList<>();
        @Override public void sendMessage(String text) { messages.add(text); }
        @Override public void sendThought(String text) { }
    }
}
