package ai.protomolt.proto.repo.blob.spi;

/** Selected backend's administrative namespace creation/readiness operation. */
@FunctionalInterface
public interface NamespaceProvisioner {
    void ensureNamespace(String namespace);
}
