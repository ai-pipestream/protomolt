package ai.protomolt.proto.repo.catalog;

/** A catalog record fails its digest or does not decode. Never treated as absent or valid. */
public final class CatalogDataLoss extends IllegalStateException {
    public CatalogDataLoss(String message) { super(message); }
    public CatalogDataLoss(String message, Throwable cause) { super(message, cause); }
}
