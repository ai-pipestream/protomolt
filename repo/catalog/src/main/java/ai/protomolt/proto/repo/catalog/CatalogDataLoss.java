package ai.protomolt.proto.repo.catalog;

/** A catalog record fails its digest or does not decode. Never treated as absent or valid. */
public final class CatalogDataLoss extends IllegalStateException {
    /**
     * Constructs a catalog data loss exception with the specified detail message.
     *
     * @param message the detail message describing the data corruption or decode failure
     */
    public CatalogDataLoss(String message) { super(message); }

    /**
     * Constructs a catalog data loss exception with the specified detail message and cause.
     *
     * @param message the detail message describing the data corruption or decode failure
     * @param cause the underlying cause of the failure
     */
    public CatalogDataLoss(String message, Throwable cause) { super(message, cause); }
}
