package ai.protomolt.proto.mcp.transport;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

class SessionFailureTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void stdioReportsAnAsynchronousOutputFailure() {
        var server = new McpServer(ActionCatalog.empty(ActionContext.create()), null, "test", "1");
        var input = new ByteArrayInputStream("""
                {"jsonrpc":"2.0","id":1,"method":"initialize"}
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                {"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"absent","arguments":{}}}
                """.getBytes(StandardCharsets.UTF_8));
        var failure = new IOException("output unavailable");
        var output = new OutputStream() {
            private int writes;
            @Override public void write(int value) throws IOException {
                throw new AssertionError("Buffered writer should write a byte array");
            }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                if (++writes > 1) {
                    throw failure;
                }
            }
        };
        assertThatThrownBy(() -> server.run(input, output))
                .isInstanceOf(IllegalStateException.class).hasRootCause(failure);
    }

    private McpServer.Session readySession() throws Exception {
        var server = new McpServer(ActionCatalog.empty(ActionContext.create()), null, "test", "1");
        var session = server.openSession();
        session.handle(mapper.readTree("""
                {"jsonrpc":"2.0","id":1,"method":"initialize"}
                """));
        session.handle(mapper.readTree("""
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """));
        return session;
    }

    private JsonNode request() throws Exception {
        return mapper.readTree("""
                {"jsonrpc":"2.0","id":2,"method":"tools/call",
                 "params":{"name":"absent","arguments":{}}}
                """);
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Unexpected interruption", e);
        }
    }

    @Test
    void cancelledTaskCannotRemoveAReplacementRequestWithTheSameId() throws Exception {
        var firstEntered = new CountDownLatch(1);
        var firstInterrupted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var secondEntered = new CountDownLatch(1);
        var releaseSecond = new CountDownLatch(1);
        var firstThread = new AtomicReference<Thread>();
        try (var session = readySession()) {
            try {
                var first = session.submit(request(), response -> {
                    firstThread.set(Thread.currentThread());
                    firstEntered.countDown();
                    try {
                        assertThat(releaseFirst.await(5, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException expected) {
                        // Simulate a callback finishing cleanup after cancellation.
                        firstInterrupted.countDown();
                        await(releaseFirst);
                    }
                });
                await(firstEntered);
                session.handle(mapper.readTree("""
                        {"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}
                        """));
                await(firstInterrupted);
                assertThat(first.isCancelled()).isTrue();
                var second = session.submit(request(), response -> {
                    secondEntered.countDown();
                    await(releaseSecond);
                });
                await(secondEntered);
                releaseFirst.countDown();
                firstThread.get().join(5000);
                assertThat(firstThread.get().isAlive()).isFalse();
                assertThatThrownBy(() -> session.submit(request()))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate request id");
                releaseSecond.countDown();
                assertThat(second.get(5, TimeUnit.SECONDS)).isPresent();
                session.awaitInFlight(1000);
            } finally {
                releaseFirst.countDown();
                releaseSecond.countDown();
            }
        }
    }

    @Test
    void drainTimeoutAndInterruptionAreExplicitAndToolErrorsRemainResponses() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var session = readySession()) {
            try {
                var future = session.submit(request(), response -> {
                    entered.countDown();
                    await(release);
                });
                await(entered);
                assertThatThrownBy(() -> session.awaitInFlight(1))
                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("Timed out");
                Thread.currentThread().interrupt();
                try {
                    assertThatThrownBy(() -> session.awaitInFlight(1000))
                            .isInstanceOf(IllegalStateException.class).hasCauseInstanceOf(InterruptedException.class);
                    assertThat(Thread.currentThread().isInterrupted()).isTrue();
                } finally {
                    Thread.interrupted();
                }
                release.countDown();
                assertThat(future.get(5, TimeUnit.SECONDS).orElseThrow()
                        .path("result").path("isError").asBoolean()).isTrue();
                session.awaitInFlight(1000);
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    void completedDeliveryFailureRemainsVisibleToTheSession() throws Exception {
        var server = new McpServer(ActionCatalog.empty(ActionContext.create()), null, "test", "1");
        var session = server.openSession();
        var failure = new IllegalStateException("delivery failed");
        try {
            session.handle(mapper.readTree("""
                    {"jsonrpc":"2.0","id":1,"method":"initialize"}
                    """));
            session.handle(mapper.readTree("""
                    {"jsonrpc":"2.0","method":"notifications/initialized"}
                    """));
            JsonNode request = mapper.readTree("""
                    {"jsonrpc":"2.0","id":2,"method":"tools/call",
                     "params":{"name":"absent","arguments":{}}}
                    """);
            var future = session.submit(request, response -> { throw failure; });
            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class).hasCause(failure);
            assertThatThrownBy(() -> session.awaitInFlight(1000))
                    .isInstanceOf(IllegalStateException.class).hasCause(failure);
            assertThatThrownBy(() -> session.submit(request))
                    .isInstanceOf(IllegalStateException.class).hasCause(failure);
            assertThatThrownBy(session::close)
                    .isInstanceOf(IllegalStateException.class).hasCause(failure);
        } finally {
            try {
                session.close();
            } catch (IllegalStateException expected) {
                assertThat(expected).hasCause(failure);
            }
        }
    }
}
