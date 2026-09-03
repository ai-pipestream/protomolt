package ai.protomolt.proto.schema.apicurio;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.microsoft.kiota.ApiException;
import com.microsoft.kiota.RequestAdapter;
import com.microsoft.kiota.RequestInformation;
import com.microsoft.kiota.serialization.Parsable;
import com.microsoft.kiota.serialization.ParsableFactory;
import com.microsoft.kiota.serialization.SerializationWriterFactory;
import com.microsoft.kiota.serialization.ValuedEnumParser;
import com.microsoft.kiota.store.BackingStoreFactory;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.models.ArtifactReference;
import io.apicurio.registry.rest.client.models.ArtifactSearchResults;
import io.apicurio.registry.rest.client.models.SearchedArtifact;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Success-path tests for {@link ApicurioDescriptorLoader} over a fake {@link RequestAdapter}
 * that serves the v3 SDK's search/content/references requests from memory (no live registry),
 * complementing {@link ApicurioDescriptorLoaderTest}'s failure-path coverage. Covers bulk-load
 * pagination and per-artifact skipping, the lookup heuristics, positive caching and reference
 * resolution through registry references.
 */
class ApicurioDescriptorLoaderBulkTest {

    private static final String GROUP = "g";

    @Test
    void bulkLoadPagesThroughAllArtifacts() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        for (int i = 0; i < 150; i++) {
            adapter.artifact(GROUP, "f" + i + ".proto", proto("f" + i + ".proto", "M" + i));
        }
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        List<FileDescriptor> descriptors = loader.loadDescriptors();

        assertThat(descriptors).hasSize(150);
        // Page size is 100: exactly two search requests (offsets 0 and 100).
        assertThat(adapter.searchOffsets).containsExactly(0, 100);
        assertThat(adapter.searchGroupIds).containsOnly(GROUP);
    }

    @Test
    void bulkLoadTerminatesOnShortPageWhenCountIsMissing() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        adapter.omitCount = true;
        adapter.artifact(GROUP, "a.proto", proto("a.proto", "A"));
        adapter.artifact(GROUP, "b.proto", proto("b.proto", "B"));
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        List<FileDescriptor> descriptors = loader.loadDescriptors();

        assertThat(descriptors).hasSize(2);
        assertThat(adapter.searchOffsets)
                .as("a null count must fall back to page-size detection, not loop or NPE")
                .containsExactly(0);
    }

    @Test
    void bulkLoadSkipsArtifactsWhoseContentCannotBeFetched() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        adapter.artifact(GROUP, "good.proto", proto("good.proto", "Good"));
        adapter.artifact(GROUP, "gone.proto", null); // listed by search, 404 on content
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        List<FileDescriptor> descriptors = loader.loadDescriptors();

        assertThat(descriptors).singleElement()
                .satisfies(fd -> assertThat(fd.findMessageTypeByName("Good")).isNotNull());
    }

    @Test
    void bulkLoadPopulatesTheLookupCache() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        adapter.artifact(GROUP, "cached.proto", proto("cached.proto", "Cached"));
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);
        loader.loadDescriptors();
        int fetchesAfterBulk = adapter.contentFetches.size();

        FileDescriptor fd = loader.loadDescriptor("cached.proto");

        assertThat(fd).isNotNull();
        assertThat(fd.findMessageTypeByName("Cached")).isNotNull();
        assertThat(adapter.contentFetches)
                .as("bulk-loaded descriptors are cached by artifactId; no re-fetch on lookup")
                .hasSize(fetchesAfterBulk);
    }

    @Test
    void lookupFindsArtifactDirectlyInConfiguredGroup() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        adapter.artifact(GROUP, "direct.proto", proto("direct.proto", "Direct"));
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        FileDescriptor fd = loader.loadDescriptor("direct.proto");

        assertThat(fd).isNotNull();
        assertThat(fd.findMessageTypeByName("Direct")).isNotNull();
        assertThat(adapter.contentFetches).containsExactly(GROUP + "/direct.proto");
    }

    @Test
    void lookupSplitsDottedNameIntoGroupAndArtifact() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        // Only reachable via heuristic 2: "my.pkg.Types" -> group "my.pkg", artifact "Types".
        adapter.artifact("my.pkg", "Types", proto("types.proto", "Types"));
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        FileDescriptor fd = loader.loadDescriptor("my.pkg.Types");

        assertThat(fd).isNotNull();
        assertThat(adapter.contentFetches).contains("my.pkg/Types");
    }

    @Test
    void lookupFallsBackToDefaultGroup() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        adapter.artifact("default", "shared.proto", proto("shared.proto", "Shared"));
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        FileDescriptor fd = loader.loadDescriptor("shared.proto");

        assertThat(fd).isNotNull();
        assertThat(adapter.contentFetches).contains("default/shared.proto");
    }

    @Test
    void successfulLookupIsCachedUntilCleared() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        adapter.artifact(GROUP, "once.proto", proto("once.proto", "Once"));
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        assertThat(loader.loadDescriptor("once.proto")).isNotNull();
        assertThat(loader.loadDescriptor("once.proto")).isNotNull();
        assertThat(adapter.contentFetches.stream().filter((GROUP + "/once.proto")::equals))
                .as("second lookup must be served from the positive cache")
                .hasSize(1);

        loader.clearCache();
        assertThat(loader.loadDescriptor("once.proto")).isNotNull();
        assertThat(adapter.contentFetches.stream().filter((GROUP + "/once.proto")::equals))
                .hasSize(2);
    }

    @Test
    void resolvesReferencesDeclaredOnTheArtifactVersion() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        adapter.artifact(GROUP, "common.proto", """
                syntax = "proto3";
                package unit.v1;
                message Common { string id = 1; }
                """);
        ArtifactReference reference = new ArtifactReference();
        reference.setName("common.proto");
        reference.setGroupId(GROUP);
        reference.setArtifactId("common.proto");
        reference.setVersion("1");
        adapter.artifact(GROUP, "app.proto", """
                syntax = "proto3";
                package unit.v1;
                import "common.proto";
                message App { unit.v1.Common common = 1; }
                """, reference);
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        FileDescriptor fd = loader.loadDescriptor("app.proto");

        assertThat(fd.getDependencies()).extracting(FileDescriptor::getName).contains("common.proto");
        assertThat(fd.findMessageTypeByName("App").findFieldByName("common").getMessageType()
                .getFullName()).isEqualTo("unit.v1.Common");
    }

    @Test
    void artifactListedWithoutGroupIsFetchedFromTheConfiguredGroup() throws Exception {
        FakeRegistryAdapter adapter = new FakeRegistryAdapter();
        adapter.artifactWithoutGroup(GROUP, "nogroup.proto", proto("nogroup.proto", "NoGroup"));
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(new RegistryClient(adapter), GROUP);

        List<FileDescriptor> descriptors = loader.loadDescriptors();

        assertThat(descriptors).singleElement()
                .satisfies(fd -> assertThat(fd.findMessageTypeByName("NoGroup")).isNotNull());
        assertThat(adapter.contentFetches).containsExactly(GROUP + "/nogroup.proto");
    }

    private static String proto(String fileName, String messageName) {
        // A unique package per file keeps independently parsed descriptors collision-free.
        String pkg = fileName.replace(".proto", "").replaceAll("[^A-Za-z0-9]", "").toLowerCase();
        return "syntax = \"proto3\";\npackage bulk." + pkg + ";\nmessage " + messageName
                + " { string id = 1; }\n";
    }

    private static ApiException apiException(int status) {
        return new ApiException("HTTP " + status) {
            {
                setResponseStatusCode(status);
            }
        };
    }

    // ---------------------------------------------------------------- fake adapter

    /**
     * Serves the three SDK calls the loader makes — artifact search ({@code send}), version
     * content ({@code sendPrimitive} with {@code InputStream.class}) and version references
     * ({@code sendCollection}) — from in-memory maps keyed {@code group/artifactId}.
     */
    private static final class FakeRegistryAdapter implements RequestAdapter {

        /** group/artifactId -> proto text; a null value means the content request 404s. */
        final Map<String, String> contents = new LinkedHashMap<>();
        /** group/artifactId -> outbound references of the fetched version. */
        final Map<String, List<ArtifactReference>> references = new HashMap<>();
        /** artifactIds whose search hit carries no groupId (registry omits it for the default). */
        final List<String> noGroupArtifacts = new ArrayList<>();
        final List<String> contentFetches = new ArrayList<>();
        final List<Integer> searchOffsets = new ArrayList<>();
        final List<String> searchGroupIds = new ArrayList<>();
        boolean omitCount;

        void artifact(String group, String artifactId, String proto, ArtifactReference... refs) {
            contents.put(group + "/" + artifactId, proto);
            references.put(group + "/" + artifactId, List.of(refs));
        }

        /** Content lives under the group, but the search hit carries no groupId. */
        void artifactWithoutGroup(String group, String artifactId, String proto) {
            contents.put(group + "/" + artifactId, proto);
            noGroupArtifacts.add(artifactId);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <ModelType extends Parsable> ModelType send(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ParsableFactory<ModelType> factory) {
            Map<String, Object> query = requestInfo.getQueryParameters();
            String groupId = (String) query.get("groupId");
            int offset = ((Number) query.getOrDefault("offset", 0)).intValue();
            int limit = ((Number) query.getOrDefault("limit", 100)).intValue();
            searchOffsets.add(offset);
            searchGroupIds.add(groupId);

            List<SearchedArtifact> all = new ArrayList<>();
            contents.keySet().forEach(key -> {
                String[] parts = key.split("/", 2);
                if (!parts[0].equals(groupId) || noGroupArtifacts.contains(parts[1])) {
                    return;
                }
                SearchedArtifact artifact = new SearchedArtifact();
                artifact.setGroupId(parts[0]);
                artifact.setArtifactId(parts[1]);
                all.add(artifact);
            });
            noGroupArtifacts.forEach(artifactId -> {
                SearchedArtifact artifact = new SearchedArtifact();
                artifact.setArtifactId(artifactId);
                all.add(artifact);
            });
            List<SearchedArtifact> page = all.stream().skip(offset).limit(limit).toList();
            ArtifactSearchResults results = new ArtifactSearchResults();
            results.setArtifacts(new ArrayList<>(page));
            if (!omitCount) {
                results.setCount(all.size());
            }
            return (ModelType) results;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <ModelType> ModelType sendPrimitive(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                Class<ModelType> targetClass) {
            String groupId = (String) requestInfo.pathParameters.get("groupId");
            String artifactId = (String) requestInfo.pathParameters.get("artifactId");
            contentFetches.add(groupId + "/" + artifactId);
            String proto = contents.get(groupId + "/" + artifactId);
            if (proto == null) {
                throw apiException(404);
            }
            return (ModelType) new ByteArrayInputStream(proto.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        @SuppressWarnings("unchecked")
        public <ModelType extends Parsable> List<ModelType> sendCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ParsableFactory<ModelType> factory) {
            String groupId = (String) requestInfo.pathParameters.get("groupId");
            String artifactId = (String) requestInfo.pathParameters.get("artifactId");
            return (List<ModelType>) references.getOrDefault(groupId + "/" + artifactId, List.of());
        }

        @Override
        public void enableBackingStore(BackingStoreFactory backingStoreFactory) {
        }

        @Override
        public SerializationWriterFactory getSerializationWriterFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <ModelType> List<ModelType> sendPrimitiveCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                Class<ModelType> targetClass) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <ModelType extends Enum<ModelType>> ModelType sendEnum(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ValuedEnumParser<ModelType> enumParser) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <ModelType extends Enum<ModelType>> List<ModelType> sendEnumCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ValuedEnumParser<ModelType> enumParser) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBaseUrl(String baseUrl) {
        }

        @Override
        public String getBaseUrl() {
            return "http://fake";
        }

        @Override
        public <T> T convertToNativeRequest(RequestInformation requestInfo) {
            throw new UnsupportedOperationException();
        }
    }
}
