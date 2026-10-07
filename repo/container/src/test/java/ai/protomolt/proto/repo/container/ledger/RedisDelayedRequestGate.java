package ai.protomolt.proto.repo.container.ledger;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Test-only unauthenticated DB 0 proxy. Retains one complete wire frame, at most 2 MiB. */
final class RedisDelayedRequestGate implements AutoCloseable {
    private final String host;
    private final int port;
    private final ServerSocket listener;
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(), armed = new AtomicBoolean();
    private final AtomicBoolean deliveryStarted = new AtomicBoolean();
    private final AtomicReference<byte[]> captured = new AtomicReference<>();
    private volatile byte[] expectedKey;
    private final CountDownLatch held = new CountDownLatch(1), clientDisconnected = new CountDownLatch(1);
    private final Thread acceptor;

    RedisDelayedRequestGate(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        listener = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
        acceptor = Thread.ofVirtual().start(() -> {
            try {
                while (!closed.get()) {
                    var client = listener.accept();
                    sockets.add(client);
                    workers.submit(() -> forward(client));
                }
            } catch (Throwable failure) { recordFailure(failure); }
        });
    }

    String uri() { return "redis://127.0.0.1:" + listener.getLocalPort(); }

    synchronized void arm(String physicalKey) {
        if (closed.get()) throw new IllegalStateException("Request gate is closed");
        Objects.requireNonNull(physicalKey);
        if (captured.get() != null || armed.get())
            throw new IllegalStateException("Request gate already armed or used");
        expectedKey = physicalKey.getBytes(StandardCharsets.UTF_8);
        armed.set(true);
    }

    boolean awaitCaptured(Duration timeout) throws InterruptedException {
        return held.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** Deliver the original queued frame once, including after the original client disconnects. */
    synchronized void deliver() throws IOException {
        if (closed.get()) throw new IllegalStateException("Request gate is closed");
        byte[] request = captured.get();
        if (request == null || !deliveryStarted.compareAndSet(false, true))
            throw new IllegalStateException("No undelivered request");
        try {
            if (!clientDisconnected.await(5, TimeUnit.SECONDS))
                throw new IOException("Original client has not disconnected");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for original client disconnect", interrupted);
        }
        try (var server = connect()) {
            server.getOutputStream().write(request);
            server.getOutputStream().flush();
            Frame response = frame(server.getInputStream(), 0);
            if (response == null || !Arrays.equals(response.wire(), ":1\r\n".getBytes(StandardCharsets.US_ASCII)))
                throw new IOException("Delayed Redis write did not return success");
        }
    }

    private Socket connect() throws IOException {
        var server = new Socket();
        try {
            server.connect(new InetSocketAddress(host, port), 5000);
            server.setSoTimeout(5000);
            return server;
        } catch (IOException failure) {
            try { server.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void forward(Socket client) {
        try (client; var server = connect()) {
            sockets.add(server);
            try {
                for (;;) {
                    Frame request = frame(client.getInputStream(), 0);
                    if (request == null) return;
                    if (!request.children().isEmpty()) {
                        String command = new String(request.children().getFirst().value(), StandardCharsets.US_ASCII);
                        if (Set.of("AUTH", "SELECT", "HELLO").contains(command.toUpperCase(Locale.ROOT)))
                            throw new IOException("Request gate requires unauthenticated RESP2 Redis DB 0");
                    }
                    if (request.conditionalWrite() && Arrays.equals(request.children().get(3).value(), expectedKey)
                            && armed.compareAndSet(true, false)) {
                        if (!captured.compareAndSet(null, request.wire())) throw new IOException("Multiple captured requests");
                        held.countDown();
                        // Keep the real caller waiting for its own timeout. The queued frame survives it.
                        client.setSoTimeout(10000);
                        try {
                            if (client.getInputStream().read() != -1)
                                throw new IOException("Client sent another command while its write was held");
                        } catch (SocketException expectedDisconnect) {
                            // A reset is allowed only after this complete request was captured.
                        }
                        clientDisconnected.countDown();
                        return;
                    }
                    server.getOutputStream().write(request.wire());
                    server.getOutputStream().flush();
                    Frame response = frame(server.getInputStream(), 0);
                    if (response == null) throw new EOFException("Redis closed before replying");
                    client.getOutputStream().write(response.wire());
                    client.getOutputStream().flush();
                }
            } finally { sockets.remove(server); }
        } catch (Throwable failure) {
            recordFailure(failure);
        } finally { sockets.remove(client); }
    }

    private void recordFailure(Throwable failure) {
        if (!(closed.get() && failure instanceof SocketException)) failures.add(failure);
    }

    private record Frame(byte[] wire, byte[] value, List<Frame> children) {
        boolean conditionalWrite() {
            return children.size() > 3 && "EVAL".equalsIgnoreCase(new String(children.get(0).value, StandardCharsets.US_ASCII))
                    && new String(children.get(1).value, StandardCharsets.UTF_8).contains("local expected=table.remove(ARGV)");
        }
    }

    private static Frame frame(InputStream in, int depth) throws IOException {
        if (depth > 16) throw new IOException("RESP nesting exceeds fixture bound");
        int kind = in.read(); if (kind == -1) return null;
        return frame(in, depth, kind);
    }

    private static Frame frame(InputStream in, int depth, int kind) throws IOException {
        var wire = new ByteArrayOutputStream(); wire.write(kind);
        var line = new ByteArrayOutputStream();
        for (;;) {
            int next = in.read(); if (next == -1) throw new EOFException("Partial RESP header");
            wire.write(next);
            if (next == '\r') {
                if (in.read() != '\n') throw new IOException("Malformed RESP line ending");
                wire.write('\n'); break;
            }
            line.write(next);
            if (line.size() > 8192) throw new IOException("RESP header exceeds fixture bound");
        }
        byte[] value = line.toByteArray(); var children = new ArrayList<Frame>();
        if (kind == '$' || kind == '*') {
            int count;
            try { count = Integer.parseInt(line.toString(StandardCharsets.US_ASCII)); }
            catch (NumberFormatException invalid) { throw new IOException("Invalid RESP length", invalid); }
            if (count < -1 || count > 1_048_576) throw new IOException("RESP value exceeds fixture bound");
            if (count >= 0 && kind == '$') {
                value = in.readNBytes(count);
                if (value.length != count || in.read() != '\r' || in.read() != '\n') throw new EOFException("Partial RESP bulk value");
                wire.write(value); wire.write('\r'); wire.write('\n');
            } else if (count >= 0) {
                if (count > 1024) throw new IOException("RESP array exceeds fixture bound");
                for (int i = 0; i < count; i++) {
                    var child = frame(in, depth + 1);
                    if (child == null) throw new EOFException("Partial RESP array");
                    children.add(child); wire.write(child.wire());
                    if (wire.size() > 2_097_152) throw new IOException("RESP frame exceeds fixture bound");
                }
            }
        } else if (kind != '+' && kind != '-' && kind != ':') throw new IOException("Unsupported fixture RESP type");
        return new Frame(wire.toByteArray(), value, List.copyOf(children));
    }


    @Override public synchronized void close() throws Exception {
        closed.set(true);
        try { listener.close(); } catch (IOException failure) { failures.add(failure); }
        for (var socket : sockets) {
            try { socket.close(); } catch (IOException failure) { failures.add(failure); }
        }
        workers.shutdown();
        acceptor.join(5000);
        if (acceptor.isAlive() || !workers.awaitTermination(5, TimeUnit.SECONDS))
            throw new IllegalStateException("Redis request proxy did not stop");
        if (!failures.isEmpty()) {
            var failure = new AssertionError("Redis request proxy failed");
            failures.forEach(failure::addSuppressed);
            throw failure;
        }
    }
}
