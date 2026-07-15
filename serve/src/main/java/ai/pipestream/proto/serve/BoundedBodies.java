package ai.pipestream.proto.serve;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/** One bounded request-body reader for the hand-rolled handlers in this module. */
final class BoundedBodies {

    private BoundedBodies() {
    }

    /**
     * Reads at most {@code maxBytes}; returns {@code null} when the body is larger, so the
     * caller answers 413 instead of buffering an unbounded stream into the heap.
     */
    static byte[] read(InputStream in, int maxBytes) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            if (out.size() + read > maxBytes) {
                return null;
            }
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }
}
