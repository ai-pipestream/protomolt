package ai.protomolt.proto.repo.blob.s3;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Raw TCP relay that sits between a real SDK client and a real container
 * endpoint, forwarding genuine traffic in both directions while being able to
 * hold one specific request's body mid-flight or cut one specific request's
 * response. It answers only the HTTP/1.1 100-continue prompt when an
 * intercepted client request explicitly asked for it; every other byte in
 * either direction comes from, and goes to, the real upstream.
 *
 * <p>One request/response pair is relayed at a time per connection, so
 * keep-alive ordering is preserved. Interception predicates match the raw
 * request line (e.g. {@code "DELETE /bucket/key?versionId=..."}).
 *
 * <p>All handler threads are daemon threads; {@link #close()} releases every
 * tracked socket so a failed test cannot wedge the suite.
 */
final class S3RequestRelay implements AutoCloseable {
    private final ServerSocket server;
    private final String upstreamHost;
    private final int upstreamPort;
    private final List<Intercept> intercepts = new CopyOnWriteArrayList<>();
    private final List<String> observedRequests = new CopyOnWriteArrayList<>();
    private final Set<Socket> live = ConcurrentHashMap.newKeySet();
    private final AtomicLong upstreamBodyBytes = new AtomicLong();
    private volatile boolean closed;

    private S3RequestRelay(ServerSocket server, String upstreamHost, int upstreamPort) {
        this.server = server;
        this.upstreamHost = upstreamHost;
        this.upstreamPort = upstreamPort;
    }

    /** Starts a relay on a loopback port forwarding to the given upstream. */
    static S3RequestRelay to(String host, int port) throws IOException {
        var server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress("127.0.0.1", 0), 50);
        var relay = new S3RequestRelay(server, host, port);
        var accept = new Thread(relay::acceptLoop);
        accept.setDaemon(true);
        accept.start();
        return relay;
    }

    /** Loopback HTTP endpoint clients should point at. */
    URI endpoint() { return URI.create("http://127.0.0.1:" + server.getLocalPort()); }

    /** Request lines observed so far, in arrival order. */
    List<String> observedRequests() { return List.copyOf(observedRequests); }

    /** Body bytes forwarded upstream before the current hold; zero when no hold is active. */
    long upstreamBodyBytes() { return upstreamBodyBytes.get(); }

    /**
     * Holds a request whose request line matches: its head and up to
     * {@code leadBytes} of its body are forwarded upstream immediately, and
     * the remainder waits for {@code release}. The returned latch counts down
     * once the request has matched and its body is held upstream.
     */
    CountDownLatch holdRequestBody(Predicate<String> requestLine, long leadBytes, CountDownLatch release) {
        var intercepted = new CountDownLatch(1);
        intercepts.add(new Intercept(requestLine, Mode.HOLD_REQUEST_BODY, leadBytes, release, intercepted));
        return intercepted;
    }

    /**
     * Forwards a request whose request line matches upstream in full, then
     * resets the client connection without relaying the response. When
     * {@code holdUntil} is non-null the reset waits for that latch, holding
     * the response until released. The returned latch counts down once the
     * full request has been forwarded upstream.
     */
    CountDownLatch cutResponse(Predicate<String> requestLine, CountDownLatch holdUntil) {
        var intercepted = new CountDownLatch(1);
        intercepts.add(new Intercept(requestLine, Mode.CUT_RESPONSE, 0, holdUntil, intercepted));
        return intercepted;
    }

    @Override public void close() throws IOException {
        closed = true;
        server.close();
        for (Socket socket : live) {
            try { socket.close(); } catch (IOException ignored) { /* releasing */ }
        }
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = server.accept();
                live.add(client);
                var handler = new Thread(() -> handle(client));
                handler.setDaemon(true);
                handler.start();
            } catch (IOException stopped) {
                return; // closed
            }
        }
    }

    private void handle(Socket client) {
        try (client) {
            Socket upstream = new Socket();
            upstream.connect(new InetSocketAddress(upstreamHost, upstreamPort), 10_000);
            live.add(upstream);
            try (upstream) {
                relay(client, upstream);
            }
        } catch (IOException | RuntimeException done) {
            // Client or upstream went away; nothing further to relay on this connection.
        } finally {
            live.remove(client);
        }
    }

    private void relay(Socket client, Socket upstream) throws IOException {
        InputStream clientIn = client.getInputStream();
        OutputStream clientOut = client.getOutputStream();
        InputStream upstreamIn = upstream.getInputStream();
        OutputStream upstreamOut = upstream.getOutputStream();
        while (!closed) {
            String requestLine;
            try { requestLine = readLine(clientIn); }
            catch (EOFException idle) { return; } // client closed the connection
            if (requestLine.isEmpty()) continue; // stray CRLF between keep-alive requests
            List<String> headers = readHeaders(clientIn);
            observedRequests.add(requestLine);
            Body body = Body.of(headers, false);
            Intercept intercept = find(requestLine);
            if (intercept == null) {
                writeHead(upstreamOut, requestLine, headers);
                forwardBody(clientIn, upstreamOut, body);
                if (!relayResponse(clientOut, upstreamIn, requestLine)) return;
                continue;
            }
            switch (intercept.mode) {
                case HOLD_REQUEST_BODY -> {
                    writeHead(upstreamOut, requestLine, headers);
                    if (body.contentLength >= 0) {
                        long lead = Math.min(intercept.leadBytes, body.contentLength);
                        var forwarded = new AtomicLong();
                        if (hasExpectContinue(headers)) {
                            // A well-behaved client holds its body until the 100: answer first, then copy.
                            clientOut.write("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
                            clientOut.flush();
                        }
                        copy(clientIn, upstreamOut, lead, forwarded);
                        upstreamBodyBytes.set(forwarded.get());
                        intercept.intercepted.countDown();
                        await(intercept.release);
                        copy(clientIn, upstreamOut, body.contentLength - lead, null);
                    } else {
                        // No declared midpoint to hold at: forward fully, then hold before responding.
                        forwardBody(clientIn, upstreamOut, body);
                        intercept.intercepted.countDown();
                        await(intercept.release);
                    }
                    if (!relayResponse(clientOut, upstreamIn, requestLine)) return;
                }
                case CUT_RESPONSE -> {
                    writeHead(upstreamOut, requestLine, headers);
                    forwardBody(clientIn, upstreamOut, body);
                    upstreamOut.flush();
                    intercept.intercepted.countDown();
                    await(intercept.release);
                    reset(client); // the provider answer, if any, never reaches the client
                    return;
                }
            }
        }
    }

    private Intercept find(String requestLine) {
        for (Intercept intercept : intercepts) {
            if (intercept.requestLine.test(requestLine)) return intercept;
        }
        return null;
    }

    /** Waits for the release latch (or close/timeout); never blocks forever. */
    private void await(CountDownLatch latch) {
        if (latch == null) return;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (latch.getCount() > 0 && !closed && System.nanoTime() < deadline) {
            try { Thread.sleep(2); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
        }
    }

    /** Abrupt close with zero linger so the client sees a TCP reset, not an orderly EOF. */
    private static void reset(Socket socket) throws IOException {
        try { socket.setSoLinger(true, 0); } catch (IOException ignored) { /* already closing */ }
        socket.close();
    }

    /**
     * Relays one response. Any 1xx status is forwarded immediately and the
     * final status line is read next. Returns false when either side
     * signalled connection close.
     */
    private boolean relayResponse(OutputStream clientOut, InputStream upstreamIn, String requestLine) throws IOException {
        boolean head = requestLine.startsWith("HEAD ");
        while (true) {
            String statusLine = readLine(upstreamIn);
            List<String> headers = readHeaders(upstreamIn);
            writeHead(clientOut, statusLine, headers);
            int status = statusLine.length() < 12 ? 0 : Integer.parseInt(statusLine.substring(9, 12).trim());
            if (status >= 100 && status < 200) continue;
            if (!head && status != 204 && status != 304) {
                forwardBody(upstreamIn, clientOut, Body.of(headers, true));
            }
            boolean close = hasToken(headers, "connection", "close")
                    || statusLine.endsWith("HTTP/1.0") && !hasToken(headers, "connection", "keep-alive");
            return !close;
        }
    }

    private enum Mode { HOLD_REQUEST_BODY, CUT_RESPONSE }

    private record Intercept(Predicate<String> requestLine, Mode mode, long leadBytes,
            CountDownLatch release, CountDownLatch intercepted) {}

    /** Framing of one HTTP message body as declared by its headers. */
    private record Body(long contentLength, boolean chunked, boolean untilEnd) {
        static Body of(List<String> headers, boolean response) {
            if (hasToken(headers, "transfer-encoding", "chunked")) return new Body(-1, true, false);
            for (String header : headers) {
                int colon = header.indexOf(':');
                if (colon > 0 && header.substring(0, colon).trim().equalsIgnoreCase("Content-Length")) {
                    return new Body(Long.parseLong(header.substring(colon + 1).trim()), false, false);
                }
            }
            return response ? new Body(-1, false, true) : new Body(-1, false, false);
        }
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder line = new StringBuilder();
        int read;
        while ((read = in.read()) != -1) {
            if (read == '\n') {
                if (line.length() > 0 && line.charAt(line.length() - 1) == '\r') line.setLength(line.length() - 1);
                return line.toString();
            }
            line.append((char) read);
            if (line.length() > 16384) throw new IOException("header line too long");
        }
        throw new EOFException("connection closed mid-message");
    }

    private static List<String> readHeaders(InputStream in) throws IOException {
        var headers = new ArrayList<String>();
        String line;
        while (!(line = readLine(in)).isEmpty()) headers.add(line);
        return headers;
    }

    private static void writeHead(OutputStream out, String line, List<String> headers) throws IOException {
        out.write((line + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
        for (String header : headers) out.write((header + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
        out.write("\r\n".getBytes(StandardCharsets.ISO_8859_1));
        out.flush();
    }

    private static void forwardBody(InputStream in, OutputStream out, Body body) throws IOException {
        if (body.chunked) {
            forwardChunked(in, out);
        } else if (body.contentLength >= 0) {
            copy(in, out, body.contentLength, null);
        } else if (body.untilEnd) {
            copy(in, out, Long.MAX_VALUE, null);
        }
    }

    private static void forwardChunked(InputStream in, OutputStream out) throws IOException {
        while (true) {
            String sizeLine = readLine(in);
            out.write((sizeLine + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
            int semicolon = sizeLine.indexOf(';');
            long size = Long.parseLong((semicolon < 0 ? sizeLine : sizeLine.substring(0, semicolon)).trim(), 16);
            if (size == 0) {
                String trailer;
                while (!(trailer = readLine(in)).isEmpty()) out.write((trailer + "\r\n").getBytes(StandardCharsets.ISO_8859_1));
                out.write("\r\n".getBytes(StandardCharsets.ISO_8859_1));
                out.flush();
                return;
            }
            copy(in, out, size, null);
            byte[] crlf = new byte[2];
            if (readExact(in, crlf) != 2) throw new EOFException("chunk ended early");
            out.write(crlf);
        }
    }

    private static void copy(InputStream in, OutputStream out, long bytes, AtomicLong counted) throws IOException {
        byte[] buffer = new byte[16384];
        long remaining = bytes;
        while (remaining > 0) {
            int read = in.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (read < 0) throw new EOFException("body ended early");
            out.write(buffer, 0, read);
            if (counted != null) counted.addAndGet(read);
            remaining -= read;
        }
        out.flush();
    }

    private static int readExact(InputStream in, byte[] target) throws IOException {
        int received = 0;
        while (received < target.length) {
            int read = in.read(target, received, target.length - received);
            if (read < 0) break;
            received += read;
        }
        return received;
    }

    private static boolean hasExpectContinue(List<String> headers) {
        return hasToken(headers, "expect", "100-continue");
    }

    private static boolean hasToken(List<String> headers, String name, String token) {
        for (String header : headers) {
            int colon = header.indexOf(':');
            if (colon <= 0 || !header.substring(0, colon).trim().equalsIgnoreCase(name)) continue;
            for (String part : header.substring(colon + 1).split(",")) {
                if (part.trim().equalsIgnoreCase(token)) return true;
            }
        }
        return false;
    }
}
