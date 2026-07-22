package ai.pipestream.proto.graph;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Shared JDK-{@code HttpServer} response plumbing for the fake-Graph tests in this package, so the
 * way the fake serves a JSON body lives in one place rather than being copied per test.
 */
final class FakeGraphSupport {

    private FakeGraphSupport() {
    }

    /** Writes {@code body} as a JSON response with {@code status}. */
    static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("content-type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
