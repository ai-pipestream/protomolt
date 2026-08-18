package ai.pipestream.proto.search.door;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Commit-point snapshots of a subject's index to a {@link SnapshotStore}:
 * the repository stays the source of truth and a snapshot is a cache, so
 * losing the bucket loses time, never data — {@code replay-documents}
 * rebuilds any subject from the repo.
 *
 * <p>A commit is an immutable set of segment files. Snapshotting holds the
 * commit point open ({@link SnapshotDeletionPolicy}), uploads the files the
 * store does not already have, and writes the commit's {@code segments_N}
 * last as the atomic marker that the snapshot exists; segment immutability
 * makes uploads incremental for free. After the marker lands, blobs the
 * commit no longer references are pruned.</p>
 *
 * <p>Identity keys a snapshot to what produced it:
 * {@code {subject}/{mapping-digest}/{policy-digest}}. Change the mapping or
 * the chunking policy and the old snapshot is not yours to restore — the
 * mount starts empty and replay re-derives, never serving a stale shape.
 * A restore that fails verification is wiped and treated the same way.</p>
 */
public final class IndexSnapshots {

    private static final Logger LOG = LoggerFactory.getLogger(IndexSnapshots.class);
    private static final String MARKER_PREFIX = "segments_";

    private final SnapshotStore store;

    public IndexSnapshots(SnapshotStore store) {
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        this.store = store;
    }

    /** The snapshot key prefix identifying one subject's produced shape. */
    static String identityPrefix(String subjectName, ServedMapping served) {
        String policy = served.chunkLane() == null
                ? "none"
                : served.chunkLane().policy().digest().substring(0, 12);
        return subjectName + "/" + mappingDigest(served) + "/" + policy;
    }

    /**
     * A digest of the index mapping's full shape: message type plus every
     * indexable field's path, engine name, and resolved hint. Any change
     * produces a new identity, so an old snapshot is never restored into a
     * reshaped mapping.
     */
    static String mappingDigest(ServedMapping served) {
        StringBuilder canonical = new StringBuilder(served.mapping().messageFullName());
        for (var field : served.mapping().fields()) {
            canonical.append('|').append(field.path())
                    .append('=').append(field.fieldName())
                    .append(':').append(field.hint())
                    .append(':').append(field.repeated());
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 6);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Restores the latest snapshot into an empty index directory before the
     * writer opens. A directory that already holds segments is newer than
     * any snapshot and is left alone. Every failure path ends with an empty
     * directory and a warning: the mount proceeds, and replay re-derives.
     *
     * @param subjectDir the subject's index directory
     * @param subjectName the subject, for the identity key and logs
     * @param served the served mapping the identity derives from
     */
    void restoreInto(Path subjectDir, String subjectName, ServedMapping served) {
        try {
            if (hasSegments(subjectDir)) {
                return;
            }
            String prefix = identityPrefix(subjectName, served);
            List<String> keys = store.list(prefix + "/");
            if (keys.stream().noneMatch(key -> fileName(key).startsWith(MARKER_PREFIX))) {
                return;
            }
            Files.createDirectories(subjectDir);
            for (String key : keys) {
                store.download(key, subjectDir.resolve(fileName(key)));
            }
            verify(subjectDir);
            LOG.info("restored snapshot of '{}' ({} files)", subjectName, keys.size());
        } catch (IOException | RuntimeException e) {
            LOG.warn("cannot restore the snapshot of '{}'; starting empty, replay re-derives",
                    subjectName, e);
            wipe(subjectDir);
        }
    }

    /**
     * Snapshots the writer's latest commit: holds it open, uploads what the
     * store is missing, writes the {@code segments_N} marker last, then
     * prunes blobs the commit no longer references.
     *
     * @param subjectName the subject, for the identity key and logs
     * @param served the served mapping the identity derives from
     * @param policy the writer's snapshot deletion policy
     * @param subjectDir the subject's index directory the files read from
     */
    void snapshot(String subjectName, ServedMapping served,
            SnapshotDeletionPolicy policy, Path subjectDir) {
        String prefix = identityPrefix(subjectName, served) + "/";
        IndexCommit commit;
        try {
            commit = policy.snapshot();
        } catch (IOException | IllegalStateException e) {
            // No commit yet: nothing to snapshot.
            return;
        }
        try {
            Collection<String> files = commit.getFileNames();
            String marker = commit.getSegmentsFileName();
            Set<String> existing = new HashSet<>();
            for (String key : store.list(prefix)) {
                existing.add(fileName(key));
            }
            for (String file : files) {
                if (!file.equals(marker) && !existing.contains(file)) {
                    store.put(prefix + file, subjectDir.resolve(file));
                }
            }
            if (!existing.contains(marker)) {
                store.put(prefix + marker, subjectDir.resolve(marker));
            }
            for (String stale : existing) {
                if (!files.contains(stale)) {
                    store.delete(prefix + stale);
                }
            }
        } catch (IOException | RuntimeException e) {
            // The next commit retries; a snapshot the marker never reached
            // does not exist, so a half-upload is invisible to restores.
            LOG.warn("cannot snapshot '{}'; the next commit retries", subjectName, e);
        } finally {
            try {
                policy.release(commit);
            } catch (IOException e) {
                LOG.warn("cannot release the snapshot commit of '{}'", subjectName, e);
            }
        }
    }

    private static boolean hasSegments(Path subjectDir) throws IOException {
        if (!Files.isDirectory(subjectDir)) {
            return false;
        }
        try (DirectoryStream<Path> entries =
                Files.newDirectoryStream(subjectDir, MARKER_PREFIX + "*")) {
            return entries.iterator().hasNext();
        }
    }

    /** Opens the restored commit once; an unopenable restore is no restore. */
    private static void verify(Path subjectDir) throws IOException {
        try (var directory = FSDirectory.open(subjectDir)) {
            DirectoryReader.open(directory).close();
        }
    }

    private static void wipe(Path subjectDir) {
        if (!Files.isDirectory(subjectDir)) {
            return;
        }
        List<Path> entries = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(subjectDir)) {
            stream.forEach(entries::add);
            for (Path entry : entries) {
                Files.deleteIfExists(entry);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "cannot clear the failed restore under " + subjectDir, e);
        }
    }

    private static String fileName(String key) {
        int slash = key.lastIndexOf('/');
        return slash < 0 ? key : key.substring(slash + 1);
    }
}
