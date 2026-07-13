package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.microsoft.kiota.ApiException;
import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.resolver.SchemaParser;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.models.ArtifactReference;
import io.apicurio.registry.rest.client.models.ArtifactSearchResults;
import io.apicurio.registry.rest.client.models.SearchedArtifact;
import io.apicurio.registry.serde.protobuf.ProtobufSchemaParser;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Loads Protocol Buffer descriptors from Apicurio Schema Registry v3.
 *
 * <p>Artifacts whose .proto imports other registered artifacts are supported when the import
 * is recorded as a first-class registry reference: references are resolved recursively (see
 * {@link ApicurioReferenceResolver}) before parsing. An artifact with an unresolvable
 * (dangling) reference is skipped with a warning naming the unresolved import, never failing
 * the whole bulk load.</p>
 *
 * <p>Usable as plain Java (no CDI required) via constructors / {@link Builder}. When only a
 * registry URL is supplied, the builder constructs a {@link RegistryClient} for it.</p>
 *
 * <p>Resolved descriptors (and, bounded, names that resolved to nothing) are cached for the
 * lifetime of the loader; the caches never expire except via {@link #clearCache()}.</p>
 */
public class ApicurioDescriptorLoader implements DescriptorLoader {

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioDescriptorLoader.class);

    /** Bound for the negative (not-found) lookup cache. */
    private static final int MAX_NEGATIVE_CACHE_SIZE = 1024;

    private final RegistryClient client;
    private final String groupId;
    private final SchemaParser<ProtobufSchema, ?> schemaParser;
    private final ApicurioReferenceResolver referenceResolver;
    private final ConcurrentHashMap<String, FileDescriptor> cache = new ConcurrentHashMap<>();
    private final Set<String> negativeCache = ConcurrentHashMap.newKeySet();

    public ApicurioDescriptorLoader(RegistryClient client, String groupId) {
        this.client = client;
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.schemaParser = new ProtobufSchemaParser<>();
        this.referenceResolver = new ApicurioReferenceResolver(new ClientArtifactSource(), schemaParser);
    }

    /**
     * @param registryUrl informational when the client is supplied externally
     * @param groupId artifact group ID
     * @param registryClient pre-built client, or {@code null} (loader reports unavailable)
     */
    public ApicurioDescriptorLoader(String registryUrl, String groupId, RegistryClient registryClient) {
        this(registryClient, groupId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public List<FileDescriptor> loadDescriptors() throws DescriptorLoadException {
        if (!isAvailable()) {
            throw new DescriptorLoadException("Apicurio registry client is not available");
        }
        List<FileDescriptor> results = new ArrayList<>();
        int offset = 0;
        int limit = 100;

        try {
            while (true) {
                int currentOffset = offset;
                ArtifactSearchResults searchResults = client.search().artifacts().get(config -> {
                    config.queryParameters.groupId = groupId;
                    config.queryParameters.artifactType = "PROTOBUF";
                    config.queryParameters.offset = currentOffset;
                    config.queryParameters.limit = limit;
                });

                if (searchResults == null || searchResults.getArtifacts() == null) {
                    break;
                }

                List<SearchedArtifact> artifacts = searchResults.getArtifacts();
                for (SearchedArtifact artifact : artifacts) {
                    String artifactId = artifact.getArtifactId();
                    String artifactGroup = artifact.getGroupId() != null ? artifact.getGroupId() : groupId;
                    try {
                        FileDescriptor fd = fetchAndParse(artifactGroup, artifactId);
                        if (fd != null) {
                            results.add(fd);
                            cache.put(artifactId, fd);
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to load descriptor for artifact {}/{}: {}",
                                artifactGroup, artifactId, e.getMessage());
                    }
                }

                offset += limit;
                // getCount() is a nullable Integer; when absent, fall back to page-size
                // detection so a missing total cannot NPE or loop forever.
                Integer total = searchResults.getCount();
                if (total != null ? offset >= total : artifacts.size() < limit) {
                    break;
                }
            }
        } catch (Exception e) {
            throw new DescriptorLoadException("Failed to search Apicurio for PROTOBUF artifacts", e);
        }

        LOG.info("Loaded {} protobuf descriptors from Apicurio group '{}'", results.size(), groupId);
        return results;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Resolution runs outside any cache lock (concurrent lookups of different names never
     * block each other) and negative results are cached (bounded) so a repeatedly requested
     * unknown name does not hit the registry every time. Only a genuine not-found (HTTP 404)
     * falls through the lookup heuristics; any other registry failure (auth, server error,
     * connection refused) surfaces as a {@link DescriptorLoadException}.</p>
     */
    @Override
    public FileDescriptor loadDescriptor(String name) throws DescriptorLoadException {
        if (!isAvailable()) {
            throw new DescriptorLoadException("Apicurio registry client is not available");
        }
        FileDescriptor cached = cache.get(name);
        if (cached != null) {
            return cached;
        }
        if (negativeCache.contains(name)) {
            return null;
        }

        FileDescriptor resolved = resolve(name);
        if (resolved == null) {
            if (negativeCache.size() >= MAX_NEGATIVE_CACHE_SIZE) {
                negativeCache.clear();
            }
            negativeCache.add(name);
            return null;
        }
        FileDescriptor previous = cache.putIfAbsent(name, resolved);
        return previous != null ? previous : resolved;
    }

    private FileDescriptor resolve(String name) throws DescriptorLoadException {
        LOG.debug("Heuristic 1: Fetching artifactId={} from group={}", name, groupId);
        FileDescriptor fd = fetchIfPresent(groupId, name);
        if (fd != null) {
            return fd;
        }

        if (name.contains(".")) {
            int lastDot = name.lastIndexOf('.');
            String g = name.substring(0, lastDot);
            String a = name.substring(lastDot + 1);
            LOG.debug("Heuristic 2: Fetching artifactId={} from group={}", a, g);
            fd = fetchIfPresent(g, a);
            if (fd != null) {
                return fd;
            }
        }

        if (!"default".equals(groupId)) {
            LOG.debug("Heuristic 3: Fetching artifactId={} from group=default", name);
            fd = fetchIfPresent("default", name);
            if (fd != null) {
                return fd;
            }
        }

        return null;
    }

    /**
     * Fetches and parses one artifact, returning {@code null} only when the artifact is
     * genuinely unavailable: the registry reports it as not found (HTTP 404) or it exists but
     * cannot be resolved because of a dangling/cyclic reference (logged, mirroring the bulk
     * load's skip behaviour). Every other failure, notably auth errors, server errors and
     * connectivity problems, is wrapped in a {@link DescriptorLoadException} so outages are
     * not mistaken for missing artifacts.
     */
    private FileDescriptor fetchIfPresent(String gid, String aid) throws DescriptorLoadException {
        try {
            return fetchAndParse(gid, aid);
        } catch (ApicurioReferenceResolver.ArtifactNotFoundException e) {
            return null;
        } catch (IllegalStateException e) {
            // Reference resolution failed on an existing artifact. A registry outage while
            // fetching a reference must still surface; a genuinely missing reference degrades
            // to "cannot provide this descriptor".
            ApiException api = findApiException(e);
            if (api != null && api.getResponseStatusCode() != 404) {
                throw new DescriptorLoadException("Apicurio registry request failed while resolving "
                        + "references of artifact " + gid + "/" + aid
                        + " (HTTP " + api.getResponseStatusCode() + ")", e);
            }
            LOG.warn("Artifact {}/{} exists but could not be resolved: {}", gid, aid, e.getMessage());
            return null;
        } catch (ApiException e) {
            if (e.getResponseStatusCode() == 404) {
                return null;
            }
            throw new DescriptorLoadException("Apicurio registry request failed for artifact "
                    + gid + "/" + aid + " (HTTP " + e.getResponseStatusCode() + ")", e);
        } catch (Exception e) {
            throw new DescriptorLoadException(
                    "Failed to load descriptor for artifact " + gid + "/" + aid, e);
        }
    }

    private static ApiException findApiException(Throwable t) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (current instanceof ApiException apiException) {
                return apiException;
            }
        }
        return null;
    }

    private FileDescriptor fetchAndParse(String gid, String aid) throws Exception {
        ProtobufSchema parsed = referenceResolver.resolveAndParse(
                gid, aid, ApicurioReferenceResolver.LATEST_VERSION_EXPRESSION);
        return parsed.getFileDescriptor();
    }

    /** {@link ApicurioReferenceResolver.ArtifactSource} backed by the registry SDK client. */
    private final class ClientArtifactSource implements ApicurioReferenceResolver.ArtifactSource {

        @Override
        public byte[] content(String gid, String aid, String versionExpression) throws Exception {
            InputStream inputStream = client.groups().byGroupId(gid).artifacts().byArtifactId(aid)
                    .versions().byVersionExpression(versionExpression).content().get();
            if (inputStream == null) {
                return null;
            }
            try (inputStream) {
                return inputStream.readAllBytes();
            }
        }

        @Override
        public List<ApicurioReferenceResolver.Reference> references(
                String gid, String aid, String versionExpression) {
            List<ArtifactReference> references = client.groups().byGroupId(gid)
                    .artifacts().byArtifactId(aid)
                    .versions().byVersionExpression(versionExpression).references().get();
            if (references == null) {
                return List.of();
            }
            return references.stream()
                    .map(ref -> new ApicurioReferenceResolver.Reference(
                            ref.getName(), ref.getGroupId(), ref.getArtifactId(), ref.getVersion()))
                    .toList();
        }
    }

    /**
     * Clears the on-demand resolution caches (positive and negative). The caches have no
     * time-based expiry; this is the only way stale entries are dropped.
     */
    public void clearCache() {
        cache.clear();
        negativeCache.clear();
    }

    public String getGroupId() {
        return groupId;
    }

    @Override
    public boolean isAvailable() {
        return client != null;
    }

    @Override
    public String getLoaderType() {
        return "Apicurio Schema Registry";
    }

    public static final class Builder {
        private String registryUrl;
        private String groupId = "default";
        private RegistryClient registryClient;
        private boolean registryClientSet;

        public Builder registryUrl(String registryUrl) {
            this.registryUrl = registryUrl;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        /**
         * Supplies a pre-built client. Passing {@code null} explicitly opts out of client
         * creation and yields an unavailable loader; leaving the client unset lets
         * {@link #build()} construct one from {@link #registryUrl(String)}.
         */
        public Builder registryClient(RegistryClient registryClient) {
            this.registryClient = registryClient;
            this.registryClientSet = true;
            return this;
        }

        public ApicurioDescriptorLoader build() {
            if (registryUrl == null || registryUrl.isBlank()) {
                throw new IllegalArgumentException("Registry URL is required");
            }
            if (groupId == null || groupId.isBlank()) {
                throw new IllegalArgumentException("Group ID is required");
            }
            RegistryClient client = registryClient;
            if (client == null && !registryClientSet) {
                client = RegistryClientFactory.create(RegistryClientOptions.create(registryUrl));
            }
            return new ApicurioDescriptorLoader(registryUrl, groupId, client);
        }
    }
}
