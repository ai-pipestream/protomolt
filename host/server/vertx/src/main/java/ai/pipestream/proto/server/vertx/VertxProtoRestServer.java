package ai.pipestream.proto.server.vertx;

import ai.pipestream.proto.openapi.ProtoOpenApiGenerator;
import ai.pipestream.proto.rest.MalformedRequestException;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.server.ProtoRestHttpSupport;
import ai.pipestream.proto.server.ProtoRestServerHost;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Vert.x 5 host for protobuf JSON/REST.
 *
 * <p>Quarkus 3.x still rides Vert.x 4 — use {@code servers/quarkus} (JAX-RS adapter)
 * there. This module targets Vert.x 5 directly.
 */
public final class VertxProtoRestServer implements ProtoRestServerHost {

    public static final String ENGINE_ID = "vertx";

    private static final Logger LOG = LoggerFactory.getLogger(VertxProtoRestServer.class);

    private final Vertx vertx;
    private final boolean ownsVertx;
    private final ProtoToolsServerConfig config;
    private final ProtoRestGateway gateway;
    private final ProtoOpenApiGenerator openApiGenerator;
    private final AtomicBoolean starting = new AtomicBoolean();
    private final AtomicReference<HttpServer> httpServer = new AtomicReference<>();
    private volatile String cachedOpenApiJson;

    public VertxProtoRestServer(ProtoToolsServerConfig config, ProtoRestGateway gateway) {
        this(Vertx.vertx(), true, config, gateway, new ProtoOpenApiGenerator(
                "Protobuf REST Gateway",
                "1.0.0",
                "/",
                config.restPathPrefix()));
    }

    public VertxProtoRestServer(
            Vertx vertx,
            ProtoToolsServerConfig config,
            ProtoRestGateway gateway,
            ProtoOpenApiGenerator openApiGenerator) {
        this(vertx, false, config, gateway, openApiGenerator);
    }

    private VertxProtoRestServer(
            Vertx vertx,
            boolean ownsVertx,
            ProtoToolsServerConfig config,
            ProtoRestGateway gateway,
            ProtoOpenApiGenerator openApiGenerator) {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.ownsVertx = ownsVertx;
        this.config = Objects.requireNonNull(config, "config");
        this.gateway = Objects.requireNonNull(gateway, "gateway");
        this.openApiGenerator = Objects.requireNonNull(openApiGenerator, "openApiGenerator");
    }

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    /** Mount under an existing Vert.x / future Quarkus-on-Vert.x-5 router. */
    public Router createRouter() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create().setBodyLimit(config.maxRequestBytes()));

        router.get(config.healthPath()).handler(ctx ->
                ctx.response().putHeader("content-type", "application/json").end("{\"status\":\"UP\"}"));
        router.route(config.healthPath()).handler(ctx -> methodNotAllowed(ctx, "GET"));

        router.get(config.openApiPath()).handler(ctx ->
                ctx.response().putHeader("content-type", "application/json").end(openApiJson()));
        router.route(config.openApiPath()).handler(ctx -> methodNotAllowed(ctx, "GET"));

        String invokePath = config.restPathPrefix() + "/:serviceName/:methodName";
        router.route(HttpMethod.GET, invokePath).handler(this::handleInvoke);
        router.route(HttpMethod.POST, invokePath).handler(this::handleInvoke);
        router.route(HttpMethod.PUT, invokePath).handler(this::handleInvoke);
        router.route(HttpMethod.PATCH, invokePath).handler(this::handleInvoke);
        router.route(HttpMethod.DELETE, invokePath).handler(this::handleInvoke);
        router.route(invokePath).handler(ctx -> methodNotAllowed(ctx, ProtoRestHttpSupport.REST_ALLOW_HEADER));
        return router;
    }

    @Override
    public int start() {
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicReference<HttpServer> started = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Future<HttpServer> future = startAsync();
        future.onComplete(ar -> {
            if (ar.succeeded()) {
                started.set(ar.result());
            } else {
                error.set(ar.cause());
            }
            latch.countDown();
        });
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                // A listen that succeeds after this timeout would leak a bound server.
                future.onSuccess(server -> {
                    httpServer.compareAndSet(server, null);
                    server.close();
                });
                starting.set(false);
                throw new IllegalStateException("Timed out starting Vert.x server");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.onSuccess(server -> {
                httpServer.compareAndSet(server, null);
                server.close();
            });
            starting.set(false);
            throw new IllegalStateException("Interrupted starting Vert.x server", e);
        }
        if (error.get() != null) {
            throw new IllegalStateException("Failed to start Vert.x server", error.get());
        }
        return started.get().actualPort();
    }

    public Future<HttpServer> startAsync() {
        if (!starting.compareAndSet(false, true)) {
            throw new IllegalStateException("Server already started");
        }
        return vertx.createHttpServer()
                .requestHandler(createRouter())
                .listen(config.port(), config.host())
                .onSuccess(server -> {
                    httpServer.set(server);
                    LOG.info("Proto REST Vert.x 5 host on {}:{} ({} , {})",
                            config.host(), server.actualPort(), config.restPathPrefix(), config.openApiPath());
                })
                .onFailure(err -> starting.set(false));
    }

    @Override
    public int actualPort() {
        HttpServer server = httpServer.get();
        if (server == null) {
            throw new IllegalStateException("Server not started");
        }
        return server.actualPort();
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

    private void handleInvoke(RoutingContext ctx) {
        // Vert.x param routes match trailing slashes; every host treats prefix/S/M/ as 404.
        if (ctx.request().path().endsWith("/")) {
            ctx.response()
                    .setStatusCode(404)
                    .putHeader("content-type", "application/json")
                    .end("{\"error\":\"Expected " + config.restPathPrefix() + "/{service}/{method}\"}");
            return;
        }
        String service = ctx.pathParam("serviceName");
        String method = ctx.pathParam("methodName");
        String verb = ctx.request().method().name();
        String body = ProtoRestHttpSupport.bodyOrEmptyJson(ctx.body() == null ? null : ctx.body().asString());
        Map<String, String> headers = new HashMap<>();
        // Keep the first value for repeated headers/params, matching the host contract.
        ctx.request().headers().forEach(e -> headers.putIfAbsent(e.getKey().toLowerCase(Locale.ROOT), e.getValue()));
        Map<String, String> query = new HashMap<>();
        try {
            ctx.queryParams().forEach(e -> query.putIfAbsent(e.getKey(), e.getValue()));
        } catch (RuntimeException e) {
            respondError(ctx, new MalformedRequestException("Malformed percent-encoding in query string", e));
            return;
        }

        vertx.executeBlocking(() -> gateway.invoke(verb, service, method, body, headers, query), false)
                .onSuccess(json -> ctx.response().putHeader("content-type", "application/json").end(json))
                .onFailure(err -> respondError(ctx, err));
    }

    private static void respondError(RoutingContext ctx, Throwable err) {
        ProtoRestHttpSupport.logIfServerError(LOG, err);
        var response = ctx.response()
                .setStatusCode(ProtoRestHttpSupport.statusFor(err))
                .putHeader("content-type", "application/json");
        ProtoRestHttpSupport.allowHeaderFor(err).ifPresent(allow -> response.putHeader("allow", allow));
        response.end(ProtoRestHttpSupport.errorJson(err));
    }

    private static void methodNotAllowed(RoutingContext ctx, String allow) {
        ctx.response()
                .setStatusCode(405)
                .putHeader("content-type", "application/json")
                .putHeader("allow", allow)
                .end("{\"error\":\"Method not allowed\"}");
    }

    private String openApiJson() {
        String cached = cachedOpenApiJson;
        if (cached == null) {
            cached = openApiGenerator.generateJson(gateway.getRegistry());
            cachedOpenApiJson = cached;
        }
        return cached;
    }

    @Override
    public void close() {
        HttpServer server = httpServer.getAndSet(null);
        CountDownLatch latch = new CountDownLatch(1);
        Future<Void> stop = server == null ? Future.succeededFuture() : server.close();
        stop.onComplete(ar -> {
            if (ownsVertx) {
                vertx.close().onComplete(v -> latch.countDown());
            } else {
                latch.countDown();
            }
        });
        try {
            latch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        starting.set(false);
    }
}
