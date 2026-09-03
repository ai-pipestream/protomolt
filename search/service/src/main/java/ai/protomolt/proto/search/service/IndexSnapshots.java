package ai.protomolt.proto.search.service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import org.apache.lucene.index.SegmentInfos;
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
    private final boolean readOnly;

    public IndexSnapshots(SnapshotStore store) {
        this(store, false);
    }

    /**
     * @param store the blob store snapshots live in
     * @param readOnly restore-only: {@link #snapshot} becomes a no-op, so a
     *        reader node never uploads a commit or prunes the writer's blobs
     */
    public IndexSnapshots(SnapshotStore store, boolean readOnly) {
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        this.store = store;
        this.readOnly = readOnly;
    }

    /** Whether this instance restores without ever writing. */
    public boolean readOnly() {
        return readOnly;
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
        if (readOnly) {
            // A reader restores; the writer's snapshots are not ours to
            // overwrite or prune.
            return;
        }
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

    /**
     * Pulls a newer commit into a live reader's directory, if the store has
     * one: segment immutability makes the pull additive — missing segment
     * files download first, the new {@code segments_N} marker lands last
     * through an atomic move, and the refreshed commit is verified before
     * this returns. On any failure the newly downloaded marker is removed
     * first, so the previous commit keeps serving untouched. A reader-side
     * pull only: nothing here uploads or deletes a store blob, so the
     * writer's snapshots are never at risk from a reader, stale or not.
     *
     * @param subjectDir the subject's live index directory
     * @param subjectName the subject, for the identity key and logs
     * @param served the served mapping the identity derives from
     * @return whether a newer commit landed and verified
     */
    boolean refreshInto(Path subjectDir, String subjectName, ServedMapping served) {
        List<Path> downloaded = new ArrayList<>();
        try {
            String prefix = identityPrefix(subjectName, served) + "/";
            List<String> keys = store.list(prefix);
            String marker = null;
            long remoteGeneration = -1;
            for (String key : keys) {
                String name = fileName(key);
                if (name.startsWith(MARKER_PREFIX)) {
                    long generation = SegmentInfos.generationFromSegmentsFileName(name);
                    if (generation > remoteGeneration) {
                        remoteGeneration = generation;
                        marker = key;
                    }
                }
            }
            if (marker == null || remoteGeneration <= localGeneration(subjectDir)) {
                return false;
            }
            for (String key : keys) {
                String name = fileName(key);
                Path target = subjectDir.resolve(name);
                if (!name.startsWith(MARKER_PREFIX) && !Files.exists(target)) {
                    store.download(key, target);
                    downloaded.add(target);
                }
            }
            Path markerFile = subjectDir.resolve(fileName(marker));
            Path staged = subjectDir.resolve(fileName(marker) + ".downloading");
            store.download(marker, staged);
            Files.move(staged, markerFile, StandardCopyOption.ATOMIC_MOVE);
            // The marker rolls back first on failure, so an unopenable
            // refresh never hides the commit that was serving.
            downloaded.add(0, markerFile);
            verify(subjectDir);
            LOG.info("refreshed '{}' to snapshot generation {}",
                    subjectName, remoteGeneration);
            return true;
        } catch (IOException | RuntimeException e) {
            LOG.warn("cannot refresh '{}'; the previous commit keeps serving",
                    subjectName, e);
            for (Path file : downloaded) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException cleanup) {
                    LOG.warn("cannot remove a failed refresh download {}", file, cleanup);
                }
            }
            return false;
        }
    }

    /** The directory's latest commit generation; {@code -1} for none. */
    private static long localGeneration(Path subjectDir) throws IOException {
        if (!Files.isDirectory(subjectDir)) {
            return -1;
        }
        List<String> names = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(subjectDir)) {
            entries.forEach(entry -> names.add(entry.getFileName().toString()));
        }
        return SegmentInfos.getLastCommitGeneration(names.toArray(String[]::new));
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
