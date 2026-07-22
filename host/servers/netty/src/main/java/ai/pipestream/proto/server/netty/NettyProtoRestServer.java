package ai.pipestream.proto.server.netty;

import ai.pipestream.proto.openapi.ProtoOpenApiGenerator;
import ai.pipestream.proto.rest.MalformedRequestException;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.server.ProtoRestHttpSupport;
import ai.pipestream.proto.server.ProtoRestServerHost;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Netty HTTP host for protobuf JSON/REST. Binds the same three routes every {@code servers/*}
 * adapter serves — {@code POST {restPrefix}/{service}/{method}}, {@code GET /openapi.json},
 * {@code GET /health} — and delegates all logic to {@link ProtoRestGateway}; this class only wires
 * the HTTP pipeline (codec + aggregator + a single request handler) onto a Netty event loop.
 * Gateway invocations run on a virtual-thread-per-task executor so slow backends never stall
 * the I/O event loops; health and OpenAPI stay on the loop.
 */
public final class NettyProtoRestServer implements ProtoRestServerHost {

    public static final String ENGINE_ID = "netty";

    private static final Logger LOG = LoggerFactory.getLogger(NettyProtoRestServer.class);
    private static final String METHOD_NOT_ALLOWED = "{\"error\":\"Method not allowed\"}";
    private static final long CLOSE_TIMEOUT_MILLIS = 15_000;

    private final ProtoToolsServerConfig config;
    private final ProtoRestGateway gateway;
    private final ProtoOpenApiGenerator openApiGenerator;

    private volatile EventLoopGroup bossGroup;
    private volatile EventLoopGroup workerGroup;
    private volatile ExecutorService invokerPool;
    private volatile Channel serverChannel;
    private volatile int boundPort = -1;
    private volatile String cachedOpenApiJson;

    public NettyProtoRestServer(ProtoRestGateway gateway) {
        this(ProtoToolsServerConfig.defaults(), gateway);
    }

    public NettyProtoRestServer(ProtoToolsServerConfig config, ProtoRestGateway gateway) {
        this(config, gateway, new ProtoOpenApiGenerator(
                "Protobuf REST Gateway", "1.0.0", "/", config.restPathPrefix()));
    }

    public NettyProtoRestServer(
            ProtoToolsServerConfig config,
            ProtoRestGateway gateway,
            ProtoOpenApiGenerator openApiGenerator) {
        this.config = Objects.requireNonNull(config, "config");
        this.gateway = Objects.requireNonNull(gateway, "gateway");
        this.openApiGenerator = Objects.requireNonNull(openApiGenerator, "openApiGenerator");
    }

    @Override
    public String engineId() {
        return ENGINE_ID;
    }

    @Override
    public int start() {
        if (serverChannel != null) {
            throw new IllegalStateException("Server already started");
        }
        EventLoopGroup boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        EventLoopGroup workers = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new HttpServerCodec());
                            pipeline.addLast(new HttpObjectAggregator(config.maxRequestBytes()));
                            pipeline.addLast(new RestHandler());
                        }
                    });
            Channel channel = bootstrap.bind(config.host(), config.port()).sync().channel();
            this.bossGroup = boss;
            this.workerGroup = workers;
            this.invokerPool = pool;
            this.serverChannel = channel;
            this.boundPort = ((InetSocketAddress) channel.localAddress()).getPort();
            LOG.info("Proto REST Netty host on {}:{} ({} , {})",
                    config.host(), boundPort, config.restPathPrefix(), config.openApiPath());
            return boundPort;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pool.shutdownNow();
            shutdownGroups(boss, workers);
            throw new IllegalStateException("Interrupted while starting Netty host", e);
        } catch (Throwable t) {
            // bind().sync() rethrows checked failures (e.g. BindException) unchecked;
            // always release the event loop groups before surfacing the failure.
            pool.shutdownNow();
            shutdownGroups(boss, workers);
            throw new IllegalStateException("Failed to start Netty host", t);
        }
    }

    @Override
    public int actualPort() {
        if (serverChannel == null) {
            throw new IllegalStateException("Server not started");
        }
        return boundPort;
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

    private String openApiJson() {
        String cached = cachedOpenApiJson;
        if (cached == null) {
            cached = openApiGenerator.generateJson(gateway.getRegistry());
            cachedOpenApiJson = cached;
        }
        return cached;
    }

    private static Map<String, String> flattenHeaders(HttpHeaders headers) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, String> entry : headers) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            // Keep the first value for a repeated header, matching the other hosts.
            out.putIfAbsent(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
        }
        return out;
    }

    private static String rawQuery(String uri) {
        int q = uri.indexOf('?');
        return q < 0 ? null : uri.substring(q + 1);
    }

    private static String decodePath(String uri) {
        try {
            return new QueryStringDecoder(uri).path();
        } catch (IllegalArgumentException e) {
            throw new MalformedRequestException("Malformed percent-encoding in request path", e);
        }
    }

    private static void shutdownGroups(EventLoopGroup boss, EventLoopGroup workers) {
        io.netty.util.concurrent.Future<?> bossDone = boss == null ? null : boss.shutdownGracefully();
        io.netty.util.concurrent.Future<?> workersDone = workers == null ? null : workers.shutdownGracefully();
        if (bossDone != null) {
            bossDone.awaitUninterruptibly(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }
        if (workersDone != null) {
            workersDone.awaitUninterruptibly(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        Channel channel = serverChannel;
        serverChannel = null;
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        ExecutorService pool = invokerPool;
        invokerPool = null;
        if (pool != null) {
            pool.shutdown();
        }
        shutdownGroups(bossGroup, workerGroup);
        bossGroup = null;
        workerGroup = null;
        boundPort = -1;
    }

    /** Routes an aggregated request to health / OpenAPI / the gateway, mirroring the JDK host. */
    private final class RestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            try {
                dispatch(ctx, keepAlive, request);
            } catch (Throwable err) {
                ProtoRestHttpSupport.logIfServerError(LOG, err);
                respond(ctx, keepAlive, ProtoRestHttpSupport.statusFor(err),
                        ProtoRestHttpSupport.errorJson(err),
                        ProtoRestHttpSupport.allowHeaderFor(err).orElse(null));
            }
        }

        private void dispatch(ChannelHandlerContext ctx, boolean keepAlive, FullHttpRequest request) {
            String method = request.method().name();
            String path = decodePath(request.uri());
            if (path.equals(config.healthPath())) {
                if ("GET".equalsIgnoreCase(method)) {
                    respond(ctx, keepAlive, 200, "{\"status\":\"UP\"}", null);
                } else {
                    respond(ctx, keepAlive, 405, METHOD_NOT_ALLOWED, "GET");
                }
                return;
            }
            if (path.equals(config.openApiPath())) {
                if ("GET".equalsIgnoreCase(method)) {
                    respond(ctx, keepAlive, 200, openApiJson(), null);
                } else {
                    respond(ctx, keepAlive, 405, METHOD_NOT_ALLOWED, "GET");
                }
                return;
            }
            if (!ProtoRestHttpSupport.isAllowedHttpMethod(method)) {
                respond(ctx, keepAlive, 405, METHOD_NOT_ALLOWED, ProtoRestHttpSupport.REST_ALLOW_HEADER);
                return;
            }
            Optional<String[]> route = ProtoRestHttpSupport.parseServiceMethod(path, config.restPathPrefix());
            if (route.isEmpty()) {
                respond(ctx, keepAlive, 404,
                        "{\"error\":\"Expected " + config.restPathPrefix() + "/{service}/{method}\"}", null);
                return;
            }
            // Extract everything on the event loop so only immutable data crosses to the
            // invoker thread; the request itself is released when channelRead0 returns.
            String[] parts = route.get();
            String body = ProtoRestHttpSupport.bodyOrEmptyJson(
                    request.content().toString(StandardCharsets.UTF_8));
            Map<String, String> headers = flattenHeaders(request.headers());
            Map<String, String> query = ProtoRestHttpSupport.parseQuery(rawQuery(request.uri()));
            ExecutorService pool = invokerPool;
            if (pool == null) {
                throw new IllegalStateException("Server not started");
            }
            pool.execute(() -> {
                try {
                    String json = gateway.invoke(method, parts[0], parts[1], body, headers, query);
                    respond(ctx, keepAlive, 200, json, null);
                } catch (Throwable err) {
                    ProtoRestHttpSupport.logIfServerError(LOG, err);
                    respond(ctx, keepAlive, ProtoRestHttpSupport.statusFor(err),
                            ProtoRestHttpSupport.errorJson(err),
                            ProtoRestHttpSupport.allowHeaderFor(err).orElse(null));
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.warn("Netty channel error", cause);
            ctx.close();
        }
    }

    private static void respond(
            ChannelHandlerContext ctx, boolean keepAlive, int status, String body, String allowHeader) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(status), Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        if (allowHeader != null) {
            response.headers().set(HttpHeaderNames.ALLOW, allowHeader);
        }
        if (keepAlive) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);
        } else {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
