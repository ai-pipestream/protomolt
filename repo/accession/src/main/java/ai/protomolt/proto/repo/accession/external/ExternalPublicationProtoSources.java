package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.registry.ConfigSupport;
import ai.protomolt.proto.registry.SchemaReference;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * The proto sources a config-lane store needs before it can gate an
 * {@link ExternalSystemBinding} envelope, and their registration in dependency order.
 */
public final class ExternalPublicationProtoSources {

    /** Classpath resource path for house validation rules proto. */
    public static final String VALIDATE_PROTO_RESOURCE =
            "ai/protomolt/proto/validate/v1/validate.proto";

    /** Classpath resource path for portable Buf protovalidate rules proto. */
    public static final String BUF_VALIDATE_PROTO_RESOURCE =
            "buf/validate/validate.proto";

    /** Classpath resource path for repository NodeAddress proto. */
    public static final String ADDRESS_PROTO_RESOURCE =
            "ai/protomolt/proto/repo/v1/address.proto";

    /** Classpath resource path for repository manifest proto. */
    public static final String MANIFEST_PROTO_RESOURCE =
            "ai/protomolt/proto/repo/v1/manifest.proto";

    /** Classpath resource path for external publication contract proto. */
    public static final String EXTERNAL_PUBLICATION_PROTO_RESOURCE =
            "ai/protomolt/proto/repo/external/v1/external_publication.proto";

    private ExternalPublicationProtoSources() {
    }

    /**
     * Registers {@code external_publication.proto} and its transitive imports into
     * {@code registryStore} so {@link ConfigSupport#gate} can resolve and validate
     * {@link ExternalSystemBinding} config-lane envelopes.
     *
     * @param registryStore target schema registry / config store
     */
    public static void registerInConfigStore(SchemaRegistryStore registryStore) {
        Objects.requireNonNull(registryStore, "registryStore");
        int validateVer = registryStore
                .register(
                        VALIDATE_PROTO_RESOURCE,
                        readClasspathResource(VALIDATE_PROTO_RESOURCE),
                        List.of())
                .version();
        int bufValidateVer = registryStore
                .register(
                        BUF_VALIDATE_PROTO_RESOURCE,
                        readClasspathResource(BUF_VALIDATE_PROTO_RESOURCE),
                        List.of())
                .version();
        int addressVer = registryStore
                .register(
                        ADDRESS_PROTO_RESOURCE,
                        readClasspathResource(ADDRESS_PROTO_RESOURCE),
                        List.of())
                .version();
        int manifestVer = registryStore
                .register(
                        MANIFEST_PROTO_RESOURCE,
                        readClasspathResource(MANIFEST_PROTO_RESOURCE),
                        List.of(new SchemaReference(
                                ADDRESS_PROTO_RESOURCE, ADDRESS_PROTO_RESOURCE, addressVer)))
                .version();
        registryStore.register(
                EXTERNAL_PUBLICATION_PROTO_RESOURCE,
                readClasspathResource(EXTERNAL_PUBLICATION_PROTO_RESOURCE),
                List.of(
                        new SchemaReference(
                                ADDRESS_PROTO_RESOURCE, ADDRESS_PROTO_RESOURCE, addressVer),
                        new SchemaReference(
                                MANIFEST_PROTO_RESOURCE, MANIFEST_PROTO_RESOURCE, manifestVer),
                        new SchemaReference(
                                VALIDATE_PROTO_RESOURCE, VALIDATE_PROTO_RESOURCE, validateVer),
                        new SchemaReference(
                                BUF_VALIDATE_PROTO_RESOURCE,
                                BUF_VALIDATE_PROTO_RESOURCE,
                                bufValidateVer)));
    }

    private static String readClasspathResource(String resourcePath) {
        try (InputStream stream =
                ExternalSystemBinding.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (stream == null) {
                throw new IllegalStateException("Missing classpath proto resource: " + resourcePath);
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ioError) {
            throw new IllegalStateException(
                    "Failed reading classpath proto resource: " + resourcePath, ioError);
        }
    }
}
