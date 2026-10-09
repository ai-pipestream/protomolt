package ai.protomolt.proto.repo.container.ledger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only intermediary. Buffers one actual signed PUT; never fabricates a storage response. */
final class DelayedS3PutGateway implements AutoCloseable {
    private static final int MAX_BODY = 1024 * 1024;
    private static final int MAX_HEADERS = 32 * 1024;
    private static final int MAX_RESPONSE = 1024 * 1024;
    private final HttpServer server;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final CompletableFuture<Request> captured = new CompletableFuture<>();
    private final CompletableFuture<Void> handlerDone = new CompletableFuture<>();
    private final CountDownLatch closeInbound = new CountDownLatch(1);
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicBoolean forwarded = new AtomicBoolean();
    private final String target;
    private final URI origin;
    private boolean disconnected;

    private record Request(String target, Map<String, List<String>> headers, byte[] body) {
        @Override public String toString() { return "BufferedSignedPut[private]"; }
    }
    record Result(int status, String version) {}

    DelayedS3PutGateway(URI origin, String target) throws IOException {
        requireUpstream(origin);
        this.origin = origin;
        this.target = Objects.requireNonNull(target);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 1);
        server.setExecutor(workers);
        server.createContext("/", this::capture);
        server.start();
    }

    URI endpoint() { return URI.create("http://127.0.0.1:" + server.getAddress().getPort()); }
    int requests() { return requests.get(); }

    private void capture(HttpExchange exchange) {
        Throwable problem = null;
        try {
            if (requests.incrementAndGet() != 1) throw new IllegalStateException("Unexpected SDK retry or concurrent request");
            // An HTTP proxy changes transport routing, not the SDK endpoint or its signature.
            var requested = exchange.getRequestURI();
            if (!exchange.getRequestMethod().equals("PUT") || !requested.isAbsolute()
                    || !requested.equals(origin.resolve(target)))
                throw new IllegalArgumentException("Gateway accepts only its exact PUT target");
            var headers = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
            int headerBytes = 0;
            for (var entry : exchange.getRequestHeaders().entrySet()) {
                for (var value : entry.getValue()) headerBytes = Math.addExact(headerBytes, entry.getKey().length() + value.length() + 4);
                headers.put(entry.getKey(), List.copyOf(entry.getValue()));
            }
            if (headerBytes > MAX_HEADERS) throw new IllegalArgumentException("Request headers exceed gateway bound");
            if (headers.containsKey("Transfer-Encoding") || headers.containsKey("Expect") || headers.containsKey("Content-Encoding"))
                throw new IllegalArgumentException("Gateway does not accept streaming or Expect framing");
            var authorization = single(headers, "Authorization");
            if (!authorization.startsWith("AWS4-HMAC-SHA256 ") || !authorization.contains("SignedHeaders=")
                    || !authorization.contains("Signature=")) throw new IllegalArgumentException("Expected signed AWS request");
            var signed = authorization.substring(authorization.indexOf("SignedHeaders=") + "SignedHeaders=".length()).split(",", 2)[0];
            var signedNames = Set.copyOf(Arrays.asList(signed.split(";")));
            if (signedNames.contains("connection") || signedNames.contains("proxy-connection") || !signedNames.contains("host")
                    || !signedNames.contains("x-amz-checksum-sha256"))
                throw new IllegalArgumentException("Signed headers cannot be altered or omit the checksum/host");
            for (var name : signedNames) single(headers, name);
            if (!single(headers, "Host").equals(origin.getRawAuthority()))
                throw new IllegalArgumentException("Proxy request changed original Host authority");
            long length = Long.parseLong(single(headers, "Content-Length"));
            if (length < 0 || length > MAX_BODY) throw new IllegalArgumentException("Request body exceeds gateway bound");
            byte[] body = exchange.getRequestBody().readNBytes(MAX_BODY + 1);
            if (body.length != length) throw new IllegalArgumentException("Request body differs from declared length");
            var sha = java.security.MessageDigest.getInstance("SHA-256").digest(body);
            if (!Base64.getEncoder().encodeToString(sha).equals(single(headers, "x-amz-checksum-sha256")))
                throw new IllegalArgumentException("Captured payload checksum differs");
            var signaturePayload = single(headers, "x-amz-content-sha256");
            if (!signaturePayload.equals(HexFormat.of().formatHex(sha)))
                throw new IllegalArgumentException("Captured payload differs from signing digest");
            captured.complete(new Request(target, Collections.unmodifiableMap(headers), body));
            if (!closeInbound.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("Gateway inbound drain was not requested");
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            problem = failure;
            captured.completeExceptionally(failure);
        } finally {
            exchange.close();
            if (problem == null) handlerDone.complete(null);
            else handlerDone.completeExceptionally(problem);
        }
    }

    void awaitCaptured() throws Exception { captured.get(5, TimeUnit.SECONDS); }

    /** Caller must first prove the SDK invocation has returned; no upstream write has occurred. */
    void disconnectClient() throws Exception {
        closeInbound.countDown();
        handlerDone.get(5, TimeUnit.SECONDS);
        if (requests.get() != 1) throw new IllegalStateException("Gateway observed more than one request");
        disconnected = true;
    }

    /** Delivers the original signed bytes after the client and inbound gateway handler have stopped. */
    Result forwardTo(URI upstream) throws Exception {
        requireUpstream(upstream);
        if (!origin.equals(upstream)) throw new IllegalArgumentException("Delayed PUT cannot change its original upstream");
        if (!disconnected || !handlerDone.isDone())
            throw new IllegalStateException("Delayed PUT requires one drained inbound request");
        var request = captured.get(5, TimeUnit.SECONDS);
        if (!forwarded.compareAndSet(false, true)) throw new IllegalStateException("Delayed PUT permits only one forwarding attempt");
        var header = new StringBuilder("PUT ").append(request.target()).append(" HTTP/1.1\r\n");
        request.headers().forEach((name, values) -> {
            if (!name.equalsIgnoreCase("Connection") && !name.equalsIgnoreCase("Proxy-Connection"))
                values.forEach(value -> header.append(name).append(": ").append(value).append("\r\n"));
        });
        header.append("Connection: close\r\n\r\n");
        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress(upstream.getHost(), upstream.getPort() < 0 ? 80 : upstream.getPort()), 5000);
            socket.setSoTimeout(5000);
            socket.getOutputStream().write(header.toString().getBytes(StandardCharsets.ISO_8859_1));
            socket.getOutputStream().write(request.body());
            socket.getOutputStream().flush();
            var response = socket.getInputStream().readNBytes(MAX_RESPONSE + 1);
            if (response.length > MAX_RESPONSE) throw new IOException("Upstream response exceeds gateway bound");
            var text = new String(response, StandardCharsets.ISO_8859_1);
            int end = text.indexOf("\r\n\r\n");
            if (end < 0 || end > MAX_HEADERS) throw new IOException("Invalid upstream response headers");
            var lines = text.substring(0, end).split("\r\n");
            var status = lines[0].split(" ", 3);
            if (status.length < 2 || !status[0].equals("HTTP/1.1")) throw new IOException("Invalid upstream status line");
            var result = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
            for (int i = 1; i < lines.length; i++) {
                int colon = lines[i].indexOf(':');
                if (colon <= 0) throw new IOException("Invalid upstream header");
                result.computeIfAbsent(lines[i].substring(0, colon), ignored -> new ArrayList<>()).add(lines[i].substring(colon + 1).trim());
            }
            return new Result(Integer.parseInt(status[1]), result.containsKey("x-amz-version-id") ? single(result, "x-amz-version-id") : "");
        }
    }

    private static String single(Map<String, List<String>> headers, String name) {
        var values = headers.get(name);
        if (values == null || values.size() != 1 || values.getFirst().isBlank())
            throw new IllegalArgumentException("Expected one " + name + " header");
        return values.getFirst();
    }

    private static void requireUpstream(URI upstream) {
        if (!"http".equals(upstream.getScheme()) || upstream.getHost() == null || upstream.getUserInfo() != null
                || (upstream.getPath() != null && !upstream.getPath().isEmpty()) || upstream.getQuery() != null
                || upstream.getFragment() != null)
            throw new IllegalArgumentException("Gateway requires a plain HTTP test upstream without a path");
    }

    @Override public void close() throws Exception {
        closeInbound.countDown();
        server.stop(0);
        workers.shutdown();
        if (!workers.awaitTermination(5, TimeUnit.SECONDS)) throw new IllegalStateException("Gateway workers did not drain");
        if (requests.get() != 0) handlerDone.get(5, TimeUnit.SECONDS);
    }
}
