package ai.pipestream.proto.search.service;

import java.nio.file.Path;
import java.util.List;

/**
 * Where index snapshots live: a flat blob namespace (S3, a shared
 * filesystem) addressed by string keys. The service never runs a live Lucene
 * {@code Directory} over object storage — that is refused as a design,
 * because S3 is not a filesystem and locking over it is unsafe — so this
 * seam only copies whole immutable files at commit points.
 */
public interface SnapshotStore {

    /** Every key under {@code prefix}, in any order. */
    List<String> list(String prefix);

    /** Uploads {@code file} under {@code key}, replacing any previous blob. */
    void put(String key, Path file);

    /** Downloads {@code key} into {@code target}, replacing it. */
    void download(String key, Path target);

    /** Deletes {@code key}; deleting an absent key succeeds. */
    void delete(String key);
}
