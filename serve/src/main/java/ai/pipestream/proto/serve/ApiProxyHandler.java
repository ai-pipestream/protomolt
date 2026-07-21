package ai.pipestream.proto.serve;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.function.IntSupplier;

/**
 * Bridges the console's same-origin API base onto a loopback port this process runs:
 * {@code /api/protomolt/*} to the in-process registry and {@code /api/serve/*} back onto
 * this server's own verbs. Strips the prefix, forwards the method, body, and the headers
 * that matter, and streams the answer back — a same-origin door to services already
 * listening on localhost, not a general-purpose proxy.
 */
final class ApiProxyHandler implements HttpHandler {

    /** Request headers worth forwarding; everything else is hop-local. */
    private static final List<String> FORWARDED = List.of(
            "Content-Type", "Accept", "api_token", "Authorization");

    /** Same cap as the REST hosts and the registry: nothing reads request bodies unbounded. */
    private static final int MAX_BODY_BYTES = 16 * 1024 * 1024;

    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private final String prefix;
    private final IntSupplier targetPort;
    private final String unavailableMessage;

    ApiProxyHandler(String prefix, IntSupplier targetPort, String unavailableMessage) {
        this.prefix = prefix;
        this.targetPort = targetPort;
        this.unavailableMessage = unavailableMessage;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            int port = targetPort.getAsInt();
            if (port <= 0) {
                byte[] body = ("{\"error\": \"" + unavailableMessage + "\"}")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(503, body.length);
                exchange.getResponseBody().write(body);
                return;
            }
            // The raw path: subjects arrive with encoded slashes (%2F) that must survive
            // the hop, or the registry sees path segments where a name should be.
            String path = exchange.getRequestURI().getRawPath().substring(prefix.length());
            if (path.isEmpty()) {
                path = "/";
            } else if (!path.startsWith("/")) {
                // com.sun.net.httpserver matches a context by plain string prefix, so /api/servexyz
                // lands on the /api/serve context; only a segment boundary belongs to this bridge.
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            if (path.startsWith("/api/")) {
                // The bridge is one hop; nesting it would bounce requests around loopback.
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            String query = exchange.getRequestURI().getRawQuery();
            URI target = URI.create("http://127.0.0.1:" + port + path
                    + (query == null ? "" : "?" + query));

            byte[] requestBody = BoundedBodies.read(exchange.getRequestBody(), MAX_BODY_BYTES);
            if (requestBody == null) {
                byte[] body = ("{\"error\": \"request body exceeds " + MAX_BODY_BYTES
                        + " bytes\"}").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(413, body.length);
                exchange.getResponseBody().write(body);
                return;
            }
            HttpRequest.Builder request = HttpRequest.newBuilder(target)
                    .timeout(Duration.ofSeconds(60))
                    .method(exchange.getRequestMethod(),
                            HttpRequest.BodyPublishers.ofByteArray(requestBody));
            for (String header : FORWARDED) {
                String value = exchange.getRequestHeaders().getFirst(header);
                if (value != null) {
                    request.header(header, value);
                }
            }
            HttpResponse<byte[]> response;
            try {
                response = CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exchange.sendResponseHeaders(502, -1);
                return;
            } catch (IOException e) {
                byte[] body = ("{\"error\": \"upstream unreachable: " + e.getMessage()
                        + "\"}").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(502, body.length);
                exchange.getResponseBody().write(body);
                return;
            }
            response.headers().firstValue("Content-Type").ifPresent(type ->
                    exchange.getResponseHeaders().set("Content-Type", type));
            byte[] body = response.body();
            exchange.sendResponseHeaders(response.statusCode(), body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                exchange.getResponseBody().write(body);
            }
        }
    }
}
