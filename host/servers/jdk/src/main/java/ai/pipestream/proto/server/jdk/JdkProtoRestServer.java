package ai.pipestream.proto.server.jdk;

import ai.pipestream.proto.openapi.ProtoOpenApiGenerator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.RequestTooLargeException;
import ai.pipestream.proto.server.ProtoRestHttpSupport;
import ai.pipestream.proto.server.ProtoRestServerHost;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Zero-extra-dep host: JDK {@link HttpServer} + virtual threads.
 */
public final class JdkProtoRestServer implements ProtoRestServerHost {

    public static final String ENGINE_ID = "jdk";

    private static final Logger LOG = LoggerFactory.getLogger(JdkProtoRestServer.class);

    private final ProtoToolsServerConfig config;
    private final ProtoRestGateway gateway;
    private final ProtoOpenApiGenerator openApiGenerator;
    private final Map<String, com.sun.net.httpserver.HttpHandler> extraContexts = new LinkedHashMap<>();
    private final AtomicReference<HttpServer> httpServer = new AtomicReference<>();
    private volatile ExecutorService executor;
    private volatile String cachedOpenApiJson;

    public JdkProtoRestServer(ProtoRestGateway gateway) {
        this(ProtoToolsServerConfig.defaults(), gateway, new ProtoOpenApiGenerator());
    }

    public JdkProtoRestServer(ProtoToolsServerConfig config, ProtoRestGateway gateway) {
        this(config, gateway, new ProtoOpenApiGenerator(
                "Protobuf REST Gateway",
                "1.0.0",
                "/",
                config.restPathPrefix()));
    }

    public JdkProtoRestServer(
            ProtoToolsServerConfig config,
            ProtoRestGateway gateway,
            ProtoOpenApiGenerator openApiGenerator) {
        this.config = Objects.requireNonNull(config, "config");
        this.gateway = Objects.requireNonNull(gateway, "gateway");
        this.openApiGenerator = Objects.requireNonNull(openApiGenerator, "openApiGenerator");
    }

    /**
     * Mounts an additional handler at {@code path} (JDK prefix matching) when the server
     * starts — e.g. a documentation UI next to the gateway. Call before {@link #start()}.
     */
    public JdkProtoRestServer withContext(String path, com.sun.net.httpserver.HttpHandler handler) {
        if (httpServer.get() != null) {
            throw new IllegalStateException("Server already started");
        }
        extraContexts.put(Objects.requireNonNull(path, "path"),
                Objects.requireNonNull(handler, "handler"));
        return this;
    }

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    @Override
    public int start() {
        if (httpServer.get() != null) {
            throw new IllegalStateException("Server already started");
        }
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(config.host(), config.port()), 0);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start JDK HttpServer", e);
        }
        ExecutorService workerPool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            server.createContext(config.healthPath(), this::handleHealth);
            server.createContext(config.openApiPath(), this::handleOpenApi);
            server.createContext(config.restPathPrefix(), this::handleRest);
            extraContexts.forEach(server::createContext);
            server.setExecutor(workerPool);
            server.start();
        } catch (Throwable t) {
            // The socket is bound after create(); release it before rethrowing.
            server.stop(0);
            workerPool.shutdownNow();
            throw new IllegalStateException("Failed to start JDK HttpServer", t);
        }
        this.executor = workerPool;
        httpServer.set(server);
        int bound = server.getAddress().getPort();
        LOG.info("Proto REST JDK host on {}:{} ({} , {})",
                config.host(), bound, config.restPathPrefix(), config.openApiPath());
        return bound;
    }

    @Override
    public int actualPort() {
        HttpServer server = httpServer.get();
        if (server == null) {
            throw new IllegalStateException("Server not started");
        }
        return server.getAddress().getPort();
    }

    @Override
    public ProtoToolsServerConfig config() {
        return config;
    }

    @Override
    public ProtoRestGateway gateway() {
        return gateway;
    }

    public void invalidateOpenApiCache() {
        cachedOpenApiJson = null;
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        // createContext prefix-matches; only the exact path is a valid route.
        if (!config.healthPath().equals(exchange.getRequestURI().getPath())) {
            write(exchange, 404, "{\"error\":\"Not found\"}", null);
            return;
        }
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            write(exchange, 405, "{\"error\":\"Method not allowed\"}", "GET");
            return;
        }
        write(exchange, 200, "{\"status\":\"UP\"}", null);
    }

    private void handleOpenApi(HttpExchange exchange) throws IOException {
        if (!config.openApiPath().equals(exchange.getRequestURI().getPath())) {
            write(exchange, 404, "{\"error\":\"Not found\"}", null);
            return;
        }
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            write(exchange, 405, "{\"error\":\"Method not allowed\"}", "GET");
            return;
        }
        write(exchange, 200, openApiJson(), null);
    }

    private void handleRest(HttpExchange exchange) throws IOException {
        try {
            if (!ProtoRestHttpSupport.isAllowedHttpMethod(exchange.getRequestMethod())) {
                write(exchange, 405, "{\"error\":\"Method not allowed\"}",
                        ProtoRestHttpSupport.REST_ALLOW_HEADER);
                return;
            }
            var route = ProtoRestHttpSupport.parseServiceMethod(
                    exchange.getRequestURI().getPath(), config.restPathPrefix());
            if (route.isEmpty()) {
                write(exchange, 404,
                        "{\"error\":\"Expected " + config.restPathPrefix() + "/{service}/{method}\"}", null);
                return;
            }
            String[] parts = route.get();
            String body = ProtoRestHttpSupport.bodyOrEmptyJson(readBody(exchange, config.maxRequestBytes()));
            Map<String, String> headers = flattenHeaders(exchange.getRequestHeaders());
            Map<String, String> query = ProtoRestHttpSupport.parseQuery(exchange.getRequestURI().getRawQuery());
            write(exchange, 200,
                    gateway.invoke(exchange.getRequestMethod(), parts[0], parts[1], body, headers, query),
                    null);
        } catch (Throwable err) {
            ProtoRestHttpSupport.logIfServerError(LOG, err);
            write(exchange, ProtoRestHttpSupport.statusFor(err), ProtoRestHttpSupport.errorJson(err),
                    ProtoRestHttpSupport.allowHeaderFor(err).orElse(null));
        }
    }

    private String openApiJson() {
        String cached = cachedOpenApiJson;
        if (cached == null) {
            cached = openApiGenerator.generateJson(gateway.getRegistry());
            cachedOpenApiJson = cached;
        }
        return cached;
    }

    private static String readBody(HttpExchange exchange, int maxRequestBytes) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                if (out.size() + read > maxRequestBytes) {
                    throw new RequestTooLargeException(maxRequestBytes);
                }
                out.write(buffer, 0, read);
            }
            return out.toString(StandardCharsets.UTF_8);
        }
    }

    private static Map<String, String> flattenHeaders(Headers headers) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null || entry.getValue().isEmpty()) {
                continue;
            }
            out.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue().getFirst());
        }
        return out;
    }

    private static void write(HttpExchange exchange, int status, String body, String allowHeader)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("content-type", "application/json");
        if (allowHeader != null) {
            exchange.getResponseHeaders().set("allow", allowHeader);
        }
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        HttpServer server = httpServer.getAndSet(null);
        if (server != null) {
            // Small grace period so in-flight exchanges can finish.
            server.stop(1);
        }
        ExecutorService workerPool = executor;
        executor = null;
        if (workerPool != null) {
            workerPool.shutdown();
        }
    }
}
