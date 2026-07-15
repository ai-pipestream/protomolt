package ai.pipestream.proto.emit;

import java.io.IOException;

/**
 * Delivers a {@link Bundle} somewhere — a directory, a git commit, an object store. Sinks
 * are always constructed with an explicit destination by the caller; nothing in the emit
 * pipeline ever picks a location on its own, matching the toolkit's disk-footprint policy.
 */
public interface BundleSink {

    /**
     * Writes every file in the bundle.
     *
     * @return a short receipt naming where the bundle went (an absolute directory, a commit
     *         SHA) — for logs and verb responses, not for parsing
     */
    String write(Bundle bundle) throws IOException;
}
