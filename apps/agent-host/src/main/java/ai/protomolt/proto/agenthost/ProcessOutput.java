package ai.protomolt.proto.agenthost;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Bounded reads of a child process's output streams for the per-turn providers. */
final class ProcessOutput {

    /** The most a provider keeps of one stream; the rest is drained and dropped. */
    static final int MAX_BYTES = 4 * 1024 * 1024;

    private ProcessOutput() {
    }

    /** Reads a stream to its end, keeping at most {@link #MAX_BYTES} of it. */
    static byte[] readBounded(InputStream stream) throws IOException {
        byte[] kept = new byte[0];
        byte[] buffer = new byte[64 * 1024];
        int total = 0;
        try (stream) {
            int read;
            while ((read = stream.read(buffer)) >= 0) {
                if (total < MAX_BYTES) {
                    int keep = Math.min(read, MAX_BYTES - total);
                    byte[] grown = new byte[total + keep];
                    System.arraycopy(kept, 0, grown, 0, total);
                    System.arraycopy(buffer, 0, grown, total, keep);
                    kept = grown;
                    total += keep;
                }
            }
        }
        return kept;
    }

    /** The bytes a reader future produced, with the failure named after the stream. */
    static byte[] get(Future<byte[]> reader, String stream) {
        try {
            return reader.get();
        } catch (ExecutionException e) {
            throw new AgentHostException("could not read the child process " + stream, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AgentHostException("interrupted reading the child process " + stream, e);
        }
    }

    /** The tail of a stream as a one-line message, bounded so an error stays readable. */
    static String tail(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8).strip();
        if (text.length() > 1_024) {
            text = text.substring(text.length() - 1_024);
        }
        return text.replace('\n', ' ');
    }
}
