package ai.protomolt.proto.repo.blob.spi;

/** Physical deletion port for lifecycle owners that have fenced new references. */
@FunctionalInterface
public interface ObjectReclaimer {
    /**
     * One bounded cleanup pass over an exact key, including all versions and
     * delete markers. True means absence was observed after deletion; false
     * requires another pass. Exceptions leave the outcome uncertain. Retain and
     * reconcile tombstones because a late writer can recreate bytes afterwards.
     */
    boolean reclaim(String bucket, String key);
}
