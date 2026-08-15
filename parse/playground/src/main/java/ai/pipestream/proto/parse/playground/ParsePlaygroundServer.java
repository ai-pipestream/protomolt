package ai.pipestream.proto.parse.playground;

import ai.pipestream.proto.parse.plugin.v1.ParseOptions;
import ai.pipestream.proto.parse.plugin.v1.ParseRequest;
import ai.pipestream.proto.parse.plugin.v1.ParseResponse;
import ai.pipestream.proto.parse.plugin.v1.ParserPluginServiceGrpc;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The parser playground: the streaming front end of the parse contract.
 *
 * <p>Thesis: a parse is watchable. The page uploads a document and renders
 * the parser's typed event stream AS IT HAPPENS — progress, document
 * claims, pages, and the final document materialize into the page the
 * moment the parser emits them, instead of hiding behind a spinner until
 * everything is done.
 *
 * <p>Transport: {@code POST /parse} pipes the upload into
 * {@code ParserPluginService.Parse} and answers with newline-delimited
 * proto3-JSON {@link ParseResponse} events, flushed per event; the page
 * consumes the stream incrementally with {@code fetch}. Pure JDK on both
 * sides: {@link HttpServer} here, {@code ReadableStream} in the browser.
 */
public final class ParsePlaygroundServer implements AutoCloseable {

    /** Prefix selecting an in-process parser channel: {@code inprocess:<name>}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    private final HttpServer http;
    private final ManagedChannel parserChannel;
    private final JsonFormat.Printer printer;

    /**
     * @param port HTTP port to bind; {@code 0} picks a free port
     * @param parserTarget the parser endpoint — {@code host:port} or
     *        {@code inprocess:<name>}
     * @param typeRegistry resolves the {@code Any} payloads parse events
     *        carry (the fleet document model at minimum)
     */
    public ParsePlaygroundServer(int port, String parserTarget, JsonFormat.TypeRegistry typeRegistry)
            throws IOException {
        if (parserTarget == null || parserTarget.isBlank()) {
            throw new IllegalArgumentException("parserTarget must not be blank");
        }
        this.parserChannel = openChannel(parserTarget);
        this.printer =
                JsonFormat.printer().usingTypeRegistry(typeRegistry).omittingInsignificantWhitespace();
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/", this::servePage);
        http.createContext("/parse", this::serveParse);
        http.start();
    }

    /** The bound HTTP port. */
    public int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
        parserChannel.shutdownNow();
        try {
            parserChannel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------

    private void servePage(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        byte[] page = PAGE.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(200, page.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(page);
        }
    }

    private void serveParse(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        String query = exchange.getRequestURI().getRawQuery();
        String filename = queryParam(query, "filename");
        String contentType = queryParam(query, "content_type");
        byte[] payload;
        try (InputStream in = exchange.getRequestBody()) {
            payload = in.readAllBytes();
        }

        exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream out = exchange.getResponseBody()) {
            streamParse(payload, filename, contentType, out);
        }
    }

    /** Drives one Parse call, writing each event as one flushed JSON line. */
    private void streamParse(byte[] payload, String filename, String contentType, OutputStream out)
            throws IOException {
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<ParseRequest> requests =
                ParserPluginServiceGrpc.newStub(parserChannel)
                        .parse(new StreamObserver<>() {
                            @Override
                            public void onNext(ParseResponse event) {
                                try {
                                    out.write(printer.print(event).getBytes(StandardCharsets.UTF_8));
                                    out.write('\n');
                                    out.flush();
                                } catch (IOException e) {
                                    // The browser went away; the parse stream
                                    // ends with the exchange.
                                    done.countDown();
                                }
                            }

                            @Override
                            public void onError(Throwable t) {
                                try {
                                    out.write(("{\"error\":\""
                                            + t.getMessage().replace('"', '\'')
                                            + "\"}\n").getBytes(StandardCharsets.UTF_8));
                                    out.flush();
                                } catch (IOException ignored) {
                                    // Nothing left to tell the browser.
                                }
                                done.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                done.countDown();
                            }
                        });
        requests.onNext(ParseRequest.newBuilder()
                .setOptions(ParseOptions.newBuilder()
                        .setDocumentId("playground")
                        .setFilename(filename)
                        .setContentType(contentType)
                        .setEmitPages(true)
                        .setEmitPreviews(true))
                .build());
        int chunk = 1024 * 1024;
        for (int offset = 0; offset < payload.length; offset += chunk) {
            requests.onNext(ParseRequest.newBuilder()
                    .setData(ByteString.copyFrom(
                            payload, offset, Math.min(chunk, payload.length - offset)))
                    .build());
        }
        requests.onCompleted();
        try {
            done.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String queryParam(String rawQuery, String name) {
        if (rawQuery == null) {
            return "";
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && pair.substring(0, eq).equals(name)) {
                return URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return "";
    }

    private static ManagedChannel openChannel(String target) {
        if (target.startsWith(INPROCESS_TARGET_PREFIX)) {
            return InProcessChannelBuilder.forName(
                    target.substring(INPROCESS_TARGET_PREFIX.length())).build();
        }
        return NettyChannelBuilder.forTarget(target).usePlaintext().build();
    }

    /** The page: no build step, no framework, no floating bar. */
    static final String PAGE = """
            <!doctype html>
            <meta charset="utf-8">
            <title>Parser Playground</title>
            <style>
              :root { color-scheme: light dark; }
              body { font: 15px/1.5 system-ui, sans-serif; max-width: 52rem;
                     margin: 2rem auto; padding: 0 1rem; }
              h1 { font-size: 1.3rem; }
              #drop { border: 2px dashed #888; border-radius: 8px; padding: 1.2rem;
                      text-align: center; cursor: pointer; }
              #status { color: #888; margin: .8rem 0; min-height: 1.5em; }
              #doc h2 { font-size: 1.15rem; margin-bottom: .2rem; }
              #doc p, #doc pre { margin: .45rem 0; animation: arrive .35s ease-out; }
              #doc img { max-width: 100%; border: 1px solid #ccc; animation: arrive .35s; }
              @keyframes arrive { from { opacity: 0; transform: translateY(4px); }
                                  to { opacity: 1; } }
              progress { width: 100%; }
            </style>
            <h1>Parser Playground</h1>
            <p>Drop a document. The page renders the parse's typed event stream
            live: every claim, page, and text block appears the moment the
            parser emits it.</p>
            <div id="drop">drop a file here, or click to choose
              <input id="file" type="file" hidden></div>
            <div id="status"></div>
            <progress id="bar" max="1" value="0" hidden></progress>
            <div id="doc"></div>
            <script>
            const drop = document.getElementById('drop');
            const input = document.getElementById('file');
            const status = document.getElementById('status');
            const bar = document.getElementById('bar');
            const doc = document.getElementById('doc');
            drop.onclick = () => input.click();
            drop.ondragover = e => e.preventDefault();
            drop.ondrop = e => { e.preventDefault(); parse(e.dataTransfer.files[0]); };
            input.onchange = () => parse(input.files[0]);
            let pagesRendered = false;
            async function parse(file) {
              if (!file) return;
              pagesRendered = false;
              doc.innerHTML = ''; bar.hidden = false; bar.value = 0;
              status.textContent = 'streaming…';
              const resp = await fetch('/parse?filename=' + encodeURIComponent(file.name)
                  + '&content_type=' + encodeURIComponent(file.type || ''), {
                  method: 'POST', body: file });
              const reader = resp.body.getReader();
              const decoder = new TextDecoder();
              let buffer = '';
              for (;;) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                let nl;
                while ((nl = buffer.indexOf('\\n')) >= 0) {
                  const line = buffer.slice(0, nl); buffer = buffer.slice(nl + 1);
                  if (line.trim()) render(JSON.parse(line));
                }
              }
              bar.hidden = true;
              status.textContent = 'done';
            }
            function render(event) {
              if (event.error) { status.textContent = 'error: ' + event.error; return; }
              if (event.progress) {
                status.textContent = event.progress.message || event.progress.phase || '';
                if (event.progress.progress) bar.value = event.progress.progress;
              }
              if (event.claims && event.claims.claims && event.claims.claims.title) {
                let h = doc.querySelector('h2') || doc.insertBefore(
                    document.createElement('h2'), doc.firstChild);
                h.textContent = event.claims.claims.title;
              }
              if (event.preview && event.preview.image) {
                const img = document.createElement('img');
                img.src = 'data:' + (event.preview.mimeType || 'image/png')
                    + ';base64,' + event.preview.image;
                doc.appendChild(img);
              }
              if (event.page && event.page.text) {
                pagesRendered = true;
                const p = document.createElement('p');
                p.textContent = event.page.text;
                doc.appendChild(p);
              }
              if (event.document && !pagesRendered) {
                // A page-streaming parser already materialized the content;
                // only non-streaming parsers render from the final document.
                const shape = event.document.document && event.document.document.shape;
                if (shape && shape.texts) {
                  for (const item of shape.texts) {
                    const base = item.title ? item.title.base
                        : item.sectionHeader ? item.sectionHeader.base
                        : item.text ? item.text.base
                        : item.listItem ? item.listItem.base
                        : item.code ? item.code : null;
                    if (!base || !base.text) continue;
                    if (item.title) {
                      let h = doc.querySelector('h2') || doc.insertBefore(
                          document.createElement('h2'), doc.firstChild);
                      h.textContent = base.text;
                    } else if (item.code) {
                      const pre = document.createElement('pre');
                      pre.textContent = base.text;
                      doc.appendChild(pre);
                    } else {
                      const p = document.createElement('p');
                      p.textContent = base.text;
                      doc.appendChild(p);
                    }
                  }
                }
              }
            }
            </script>
            """;
}
