package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.sources.ProtoImports;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.sources.publish.PublishResult;
import ai.pipestream.proto.sources.publish.PublishResult.Action;
import ai.pipestream.proto.sources.publish.PublishResult.FileOutcome;
import ai.pipestream.proto.sources.publish.SchemaPublishException;
import ai.pipestream.proto.sources.publish.SchemaPublisher;
import ai.pipestream.proto.sources.publish.SubjectNamingStrategy;
import com.microsoft.kiota.ApiException;
import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.models.ArtifactReference;
import io.apicurio.registry.rest.client.models.CreateArtifact;
import io.apicurio.registry.rest.client.models.CreateArtifactResponse;
import io.apicurio.registry.rest.client.models.CreateVersion;
import io.apicurio.registry.rest.client.models.IfArtifactExists;
import io.apicurio.registry.rest.client.models.ProblemDetails;
import io.apicurio.registry.rest.client.models.RuleViolationProblemDetails;
import io.apicurio.registry.rest.client.models.VersionContent;
import io.apicurio.registry.rest.client.models.VersionMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Publishes a {@link ProtoSourceSet} to an Apicurio Registry 3.x over the v3 SDK
 * {@link RegistryClient} — the write-side counterpart of {@link ApicurioDescriptorLoader}.
 *
 * <p>Files are registered in reverse-topological import order
 * ({@link ProtoSourceSet#topologicalOrder()}), so every artifact reference exists before the
 * artifact that declares the import. Each file's {@code import} statements become first-class
 * registry references ({@code name} = import path, {@code artifactId} from the
 * {@link SubjectNamingStrategy}, {@code version} = the version registered for that import
 * earlier in the run or the registry's current latest); {@code google/protobuf/*} well-known
 * imports are skipped, matching the read-side convention.</p>
 *
 * <p>Idempotency uses the v3 create-artifact API with
 * {@code ifExists=FIND_OR_CREATE_VERSION}: a returned version that already existed before the
 * call is reported {@link Action#UNCHANGED}, a new version on an existing artifact
 * {@link Action#UPDATED}, and a brand-new artifact {@link Action#CREATED}. In
 * {@link PublishOptions#dryRun() dry-run} mode only reads are performed (existence + latest
 * content comparison) and writes are reported as {@link Action#WOULD_WRITE}.</p>
 *
 * <p>Per-artifact rule rejections (validity/compatibility rule violations) become
 * {@link Action#FAILED} outcomes and the remaining files are still attempted; registry-level
 * failures (auth, server errors, connectivity) abort the whole publish with a
 * {@link SchemaPublishException}.</p>
 *
 * <p>The {@link ArtifactStore} seam exists so ordering, reference building and outcome
 * classification are unit-testable without a live registry, mirroring
 * {@link ApicurioReferenceResolver}'s {@code ArtifactSource}.</p>
 */
public class ApicurioSchemaPublisher implements SchemaPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(ApicurioSchemaPublisher.class);

    private static final String WELL_KNOWN_PREFIX = "google/protobuf/";
    private static final String LATEST_VERSION_EXPRESSION = "branch=latest";
    private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";

    /** Minimal write-side view of the registry; seam for unit tests. */
    interface ArtifactStore {

        /** Latest version of the artifact, or {@code null} when the artifact does not exist. */
        VersionInfo latestVersion(String artifactId) throws Exception;

        /** Content of the artifact's latest version, or {@code null} when it does not exist. */
        String latestContent(String artifactId) throws Exception;

        /**
         * Registers content with {@code ifExists=FIND_OR_CREATE_VERSION} semantics: returns an
         * existing version holding identical content when there is one, otherwise creates (and
         * returns) a new version, creating the artifact itself when needed.
         */
        VersionInfo createOrFindVersion(String artifactId, String content, List<Reference> references)
                throws Exception;
    }

    /** A version's coordinates; {@code globalId} orders versions across the registry. */
    record VersionInfo(String version, Long globalId) {
    }

    /** An artifact reference; {@code name} is the import path used by the referencing file. */
    record Reference(String name, String groupId, String artifactId, String version) {
    }

    private final String groupId;
    private final ArtifactStore store;
    private final String target;

    /**
     * Creates a publisher over a pre-built v3 SDK client.
     *
     * @param client registry client
     * @param groupId group the artifacts are registered in
     */
    public ApicurioSchemaPublisher(RegistryClient client, String groupId) {
        this(client, groupId, "apicurio:group=" + Objects.requireNonNull(groupId, "groupId"));
    }

    private ApicurioSchemaPublisher(RegistryClient client, String groupId, String target) {
        this(new ClientArtifactStore(Objects.requireNonNull(client, "client"), groupId),
                groupId, target);
    }

    /** Test seam: publish through an arbitrary {@link ArtifactStore}. */
    ApicurioSchemaPublisher(ArtifactStore store, String groupId, String target) {
        this.store = Objects.requireNonNull(store, "store");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.target = Objects.requireNonNull(target, "target");
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public PublishResult publish(ProtoSourceSet sources, PublishOptions options)
            throws SchemaPublishException {
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(options, "options");
        PublishRun run = new PublishRun(options);
        List<FileOutcome> outcomes = new ArrayList<>(sources.size());
        for (String path : sources.topologicalOrder()) {
            String content = sources.get(path).orElseThrow().content();
            FileOutcome outcome = run.publishFile(path, content);
            if (outcome.action() == Action.FAILED) {
                LOG.warn("Publishing {} to group {} failed: {}", path, groupId, outcome.detail());
            }
            outcomes.add(outcome);
        }
        return new PublishResult(outcomes);
    }

    @Override
    public String target() {
        return target;
    }

    public String getGroupId() {
        return groupId;
    }

    // ---------------------------------------------------------------- publish state machine

    /** State shared across one {@link #publish} invocation. */
    private final class PublishRun {

        private final PublishOptions options;
        /** import path -> version registered (or found) for it during this run. */
        private final Map<String, String> versions = new HashMap<>();
        /** Dry run only: paths that would have been written (their versions are unknown). */
        private final Set<String> pendingPaths = new HashSet<>();
        /** Paths whose publish failed; files importing them fail too. */
        private final Set<String> failedPaths = new HashSet<>();

        PublishRun(PublishOptions options) {
            this.options = options;
        }

        FileOutcome publishFile(String path, String content) throws SchemaPublishException {
            String artifactId = options.naming().subjectFor(path);

            List<Reference> references = new ArrayList<>();
            boolean pendingReference = false;
            for (String importPath : ProtoImports.of(content)) {
                if (importPath.startsWith(WELL_KNOWN_PREFIX)) {
                    continue; // well-known types are compiled in, never registered
                }
                if (failedPaths.contains(importPath)) {
                    failedPaths.add(path);
                    return new FileOutcome(path, artifactId, Action.FAILED,
                            "import " + importPath + " failed to publish earlier in this run");
                }
                if (pendingPaths.contains(importPath)) {
                    pendingReference = true; // dry run: the import has no registered version yet
                    continue;
                }
                String referenceArtifactId = options.naming().subjectFor(importPath);
                String version = versions.get(importPath);
                if (version == null) {
                    VersionInfo latest = read(() -> store.latestVersion(referenceArtifactId),
                            "reading latest version of artifact " + referenceArtifactId);
                    if (latest == null) {
                        failedPaths.add(path);
                        return new FileOutcome(path, artifactId, Action.FAILED,
                                "import " + importPath + " is neither in the source set nor "
                                        + "registered as artifact " + groupId + "/" + referenceArtifactId);
                    }
                    version = latest.version();
                    versions.put(importPath, version);
                }
                references.add(new Reference(importPath, groupId, referenceArtifactId, version));
            }

            if (options.dryRun()) {
                return dryRunOutcome(path, artifactId, content, pendingReference);
            }

            VersionInfo before = read(() -> store.latestVersion(artifactId),
                    "reading latest version of artifact " + artifactId);
            VersionInfo result;
            try {
                result = store.createOrFindVersion(artifactId, content, references);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SchemaPublishException(
                        "Interrupted while publishing artifact " + groupId + "/" + artifactId, e);
            } catch (Exception e) {
                ApiException api = findApiException(e);
                if (api != null && isPerArtifactRejection(api)) {
                    failedPaths.add(path);
                    return new FileOutcome(path, artifactId, Action.FAILED, rejectionMessage(api));
                }
                throw new SchemaPublishException(
                        "Failed to publish artifact " + groupId + "/" + artifactId, e);
            }
            versions.put(path, result.version());
            return new FileOutcome(path, artifactId, classify(before, result),
                    "version " + result.version());
        }

        /**
         * Dry run performs only reads: artifact existence plus a latest-content comparison.
         * (Unlike the live path's FIND_OR_CREATE_VERSION, only the latest version is compared,
         * so content matching an older version reports WOULD_WRITE rather than UNCHANGED.)
         */
        private FileOutcome dryRunOutcome(String path, String artifactId, String content,
                                          boolean pendingReference) throws SchemaPublishException {
            if (pendingReference) {
                pendingPaths.add(path);
                return new FileOutcome(path, artifactId, Action.WOULD_WRITE,
                        "depends on files that would be written in this run");
            }
            VersionInfo latest = read(() -> store.latestVersion(artifactId),
                    "reading latest version of artifact " + artifactId);
            if (latest == null) {
                pendingPaths.add(path);
                return new FileOutcome(path, artifactId, Action.WOULD_WRITE, "artifact would be created");
            }
            String latestContent = read(() -> store.latestContent(artifactId),
                    "reading latest content of artifact " + artifactId);
            if (content.equals(latestContent)) {
                versions.put(path, latest.version());
                return new FileOutcome(path, artifactId, Action.UNCHANGED, "version " + latest.version());
            }
            pendingPaths.add(path);
            return new FileOutcome(path, artifactId, Action.WOULD_WRITE, "new version would be registered");
        }
    }

    /**
     * A found existing version carries a global ID no greater than the pre-call latest;
     * a freshly created version always gets a larger one.
     */
    private static Action classify(VersionInfo before, VersionInfo result) {
        if (before == null) {
            return Action.CREATED;
        }
        if (result.globalId() != null && before.globalId() != null) {
            return result.globalId() > before.globalId() ? Action.UPDATED : Action.UNCHANGED;
        }
        // Global IDs unavailable: fall back to comparing version identifiers.
        return Objects.equals(result.version(), before.version()) ? Action.UNCHANGED : Action.UPDATED;
    }

    // ---------------------------------------------------------------- failure classification

    @FunctionalInterface
    private interface StoreRead<T> {
        T get() throws Exception;
    }

    /** Reads never fail per-file: any failure is registry-level and aborts the publish. */
    private <T> T read(StoreRead<T> call, String description) throws SchemaPublishException {
        try {
            return call.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaPublishException("Interrupted while " + description, e);
        } catch (Exception e) {
            throw new SchemaPublishException("Apicurio registry request failed while " + description, e);
        }
    }

    /**
     * Content-specific rejections (rule violations, conflicts, unprocessable content) fail the
     * file; anything else (auth, group missing, server errors) is registry-level.
     */
    private static boolean isPerArtifactRejection(ApiException e) {
        if (e instanceof RuleViolationProblemDetails) {
            return true;
        }
        int status = statusOf(e);
        return status == 409 || status == 422;
    }

    private static String rejectionMessage(ApiException e) {
        String title = null;
        String detail = null;
        if (e instanceof RuleViolationProblemDetails problem) {
            title = problem.getTitle();
            detail = problem.getDetail();
        } else if (e instanceof ProblemDetails problem) {
            title = problem.getTitle();
            detail = problem.getDetail();
        }
        StringBuilder message = new StringBuilder("registry rejected content (HTTP ")
                .append(statusOf(e)).append(")");
        if (title != null && !title.isBlank()) {
            message.append(": ").append(title);
        }
        if (detail != null && !detail.isBlank()) {
            message.append(" - ").append(detail);
        }
        if (title == null && detail == null && e.getMessage() != null) {
            message.append(": ").append(e.getMessage());
        }
        return message.toString();
    }

    /**
     * HTTP status of an SDK failure: the problem document's {@code status} field when present
     * (settable in tests), otherwise the transport-level response status.
     */
    private static int statusOf(ApiException e) {
        Integer status = null;
        if (e instanceof RuleViolationProblemDetails problem) {
            status = problem.getStatus();
        } else if (e instanceof ProblemDetails problem) {
            status = problem.getStatus();
        }
        return status != null && status != 0 ? status : e.getResponseStatusCode();
    }

    private static ApiException findApiException(Throwable t) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (current instanceof ApiException apiException) {
                return apiException;
            }
        }
        return null;
    }

    // ---------------------------------------------------------------- SDK-backed store

    /** {@link ArtifactStore} backed by the registry SDK client. */
    private static final class ClientArtifactStore implements ArtifactStore {

        private final RegistryClient client;
        private final String groupId;

        ClientArtifactStore(RegistryClient client, String groupId) {
            this.client = client;
            this.groupId = groupId;
        }

        @Override
        public VersionInfo latestVersion(String artifactId) throws Exception {
            try {
                VersionMetaData meta = client.groups().byGroupId(groupId)
                        .artifacts().byArtifactId(artifactId)
                        .versions().byVersionExpression(LATEST_VERSION_EXPRESSION).get();
                return meta == null ? null : new VersionInfo(meta.getVersion(), meta.getGlobalId());
            } catch (ApiException e) {
                if (statusOf(e) == 404) {
                    return null;
                }
                throw e;
            }
        }

        @Override
        public String latestContent(String artifactId) throws Exception {
            try {
                InputStream inputStream = client.groups().byGroupId(groupId)
                        .artifacts().byArtifactId(artifactId)
                        .versions().byVersionExpression(LATEST_VERSION_EXPRESSION).content().get();
                if (inputStream == null) {
                    return null;
                }
                try (inputStream) {
                    return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                }
            } catch (ApiException e) {
                if (statusOf(e) == 404) {
                    return null;
                }
                throw e;
            }
        }

        @Override
        public VersionInfo createOrFindVersion(String artifactId, String content,
                                               List<Reference> references) throws Exception {
            CreateArtifact createArtifact = new CreateArtifact();
            createArtifact.setArtifactId(artifactId);
            createArtifact.setArtifactType("PROTOBUF");
            VersionContent versionContent = new VersionContent();
            versionContent.setContent(content);
            versionContent.setContentType(PROTOBUF_CONTENT_TYPE);
            if (!references.isEmpty()) {
                versionContent.setReferences(references.stream().map(reference -> {
                    ArtifactReference artifactReference = new ArtifactReference();
                    artifactReference.setName(reference.name());
                    artifactReference.setGroupId(reference.groupId());
                    artifactReference.setArtifactId(reference.artifactId());
                    artifactReference.setVersion(reference.version());
                    return artifactReference;
                }).toList());
            }
            CreateVersion createVersion = new CreateVersion();
            createVersion.setContent(versionContent);
            createArtifact.setFirstVersion(createVersion);

            CreateArtifactResponse response = client.groups().byGroupId(groupId).artifacts()
                    .post(createArtifact, config ->
                            config.queryParameters.ifExists = IfArtifactExists.FIND_OR_CREATE_VERSION);
            VersionMetaData meta = response == null ? null : response.getVersion();
            if (meta == null) {
                throw new IllegalStateException(
                        "Registry returned no version metadata for artifact " + groupId + "/" + artifactId);
            }
            return new VersionInfo(meta.getVersion(), meta.getGlobalId());
        }
    }

    // ---------------------------------------------------------------- builder

    public static final class Builder {

        private String registryUrl;
        private String groupId = "default";
        private RegistryClient registryClient;

        /** Base registry URL; the SDK appends {@code /apis/registry/v3} when missing. */
        public Builder registryUrl(String registryUrl) {
            this.registryUrl = registryUrl;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        /**
         * Supplies a pre-built client; when unset, {@link #build()} constructs one from
         * {@link #registryUrl(String)}.
         */
        public Builder registryClient(RegistryClient registryClient) {
            this.registryClient = registryClient;
            return this;
        }

        public ApicurioSchemaPublisher build() {
            if (registryUrl == null || registryUrl.isBlank()) {
                throw new IllegalArgumentException("Registry URL is required");
            }
            if (groupId == null || groupId.isBlank()) {
                throw new IllegalArgumentException("Group ID is required");
            }
            RegistryClient client = registryClient;
            if (client == null) {
                client = RegistryClientFactory.create(RegistryClientOptions.create(registryUrl));
            }
            return new ApicurioSchemaPublisher(client, groupId,
                    "apicurio:" + registryUrl + " group=" + groupId);
        }
    }
}
