package ai.pipestream.proto.serve;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Answers every request with 503 and one clear sentence. Mounted where a surface exists in
 * some configurations but is deliberately off in this one (the console in token mode), so
 * an operator sees why instead of a half-working page or a bare 404.
 */
final class DisabledSurfaceHandler implements HttpHandler {

    private final byte[] body;

    DisabledSurfaceHandler(String reason) {
        this.body = ("{\"error\": \"" + reason + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(503, body.length);
            exchange.getResponseBody().write(body);
        }
    }
}
