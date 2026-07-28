package ai.pipestream.proto.acp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Speaks raw newline-delimited JSON over a pair of streams: each line written by the peer
 * lands in a queue, so a test can assert the exact bytes on the wire against the golden
 * transcript. Times out rather than hanging if the peer goes silent.
 */
final class JsonPipe implements AutoCloseable {

    private static final long TAKE_TIMEOUT_SECONDS = 180;

    private final OutputStream out;
    private final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
    private final InputStream in;

    private JsonPipe(InputStream in, OutputStream out) {
        this.in = in;
        this.out = out;
    }

    static JsonPipe over(InputStream in, OutputStream out) {
        JsonPipe pipe = new JsonPipe(in, out);
        Thread.ofVirtual().name("json-pipe-reader").start(() -> {
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(pipe.in, StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    pipe.lines.add(line);
                }
            } catch (IOException e) {
                // Stream closed; take() will time out if a test still waits.
            }
        });
        return pipe;
    }

    /** Writes one JSON message plus its newline delimiter. */
    void send(String jsonLine) throws IOException {
        synchronized (out) {
            out.write(jsonLine.getBytes(StandardCharsets.UTF_8));
            out.write('\n');
            out.flush();
        }
    }

    /** The next line the peer wrote; fails the test rather than hanging if none arrives. */
    String take() throws InterruptedException {
        String line = lines.poll(TAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (line == null) {
            throw new AssertionError("the agent wrote nothing within " + TAKE_TIMEOUT_SECONDS + "s");
        }
        return line;
    }

    @Override
    public void close() throws IOException {
        in.close();
        out.close();
    }
}
