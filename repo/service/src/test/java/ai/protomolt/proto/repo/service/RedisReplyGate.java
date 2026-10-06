package ai.protomolt.proto.repo.service;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Test-only RESP2 pass-through: hold a real successful conditional-write reply, never fabricate one. */
final class RedisReplyGate implements AutoCloseable {
    private final ServerSocket listener = new ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"));
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(), armed = new AtomicBoolean();
    private final AtomicBoolean clientClosing = new AtomicBoolean();
    final CountDownLatch held = new CountDownLatch(1), release = new CountDownLatch(1);
    private final Thread acceptor;

    RedisReplyGate(String host, int port) throws IOException {
        acceptor = Thread.ofVirtual().start(() -> {
            try {
                while (!closed.get()) {
                    var client = listener.accept(); sockets.add(client);
                    workers.submit(() -> forward(client, host, port));
                }
            } catch (Throwable failure) { recordFailure(failure); }
        });
    }

    String uri() { return "redis://127.0.0.1:" + listener.getLocalPort(); }
    void arm() { if (!armed.compareAndSet(false, true)) throw new IllegalStateException("Reply gate already armed"); }
    void expectClientClose() { clientClosing.set(true); }

    private void forward(Socket client, String host, int port) {
        try (client; var server = new Socket(host, port)) {
            sockets.add(server);
            try {
                for (;;) {
                    var request = request(client.getInputStream());
                    if (request == null) return;
                    server.getOutputStream().write(request.wire()); server.getOutputStream().flush();
                    var response = frame(server.getInputStream(), 0);
                    if (response == null) throw new EOFException("Redis closed before replying");
                    if (request.conditionalWrite() && armed.compareAndSet(true, false)) {
                        if (!Arrays.equals(response.wire(), ":1\r\n".getBytes(StandardCharsets.US_ASCII)))
                            throw new IOException("Conditional Redis write did not return success");
                        held.countDown();
                        if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("Redis reply gate was not released");
                    }
                    client.getOutputStream().write(response.wire()); client.getOutputStream().flush();
                }
            } finally { sockets.remove(server); }
        } catch (Throwable failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            recordFailure(failure);
        } finally { sockets.remove(client); }
    }

    private Frame request(InputStream in) throws IOException {
        final int first;
        try { first = in.read(); }
        catch (SocketException reset) {
            // The test has completed its call and is explicitly closing the pool.
            // Only an idle connection boundary may end this way, never a partial frame.
            if (clientClosing.get()) return null;
            throw reset;
        }
        return first == -1 ? null : frame(in, 0, first);
    }

    private void recordFailure(Throwable failure) {
        if (!(closed.get() && failure instanceof SocketException)) failures.add(failure);
    }

    private record Frame(byte[] wire, byte[] value, List<Frame> children) {
        boolean conditionalWrite() {
            return children.size() > 1 && "EVAL".equalsIgnoreCase(new String(children.get(0).value, StandardCharsets.US_ASCII))
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

    @Override public void close() throws Exception {
        closed.set(true); release.countDown();
        try { listener.close(); } catch (IOException failure) { failures.add(failure); }
        for (var socket : sockets) {
            try { socket.close(); } catch (IOException failure) { failures.add(failure); }
        }
        workers.shutdown();
        acceptor.join(5000);
        if (acceptor.isAlive() || !workers.awaitTermination(5, TimeUnit.SECONDS))
            throw new IllegalStateException("Redis reply proxy did not stop");
        if (!failures.isEmpty()) {
            var failure = new AssertionError("Redis reply proxy failed");
            failures.forEach(failure::addSuppressed); throw failure;
        }
    }
}
