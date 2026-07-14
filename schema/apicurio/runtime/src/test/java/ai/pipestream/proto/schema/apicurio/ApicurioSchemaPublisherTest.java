package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.schema.apicurio.ApicurioSchemaPublisher.ArtifactStore;
import ai.pipestream.proto.schema.apicurio.ApicurioSchemaPublisher.Reference;
import ai.pipestream.proto.schema.apicurio.ApicurioSchemaPublisher.VersionInfo;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.sources.publish.PublishResult;
import ai.pipestream.proto.sources.publish.PublishResult.Action;
import ai.pipestream.proto.sources.publish.PublishResult.FileOutcome;
import ai.pipestream.proto.sources.publish.SchemaPublishException;
import ai.pipestream.proto.sources.publish.SubjectNamingStrategy;
import com.microsoft.kiota.ApiException;
import io.apicurio.registry.rest.client.models.ProblemDetails;
import io.apicurio.registry.rest.client.models.RuleViolationProblemDetails;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Unit tests for {@link ApicurioSchemaPublisher} using an in-memory {@link ArtifactStore} fake
 * (no live registry required), following the {@link ApicurioReferenceResolverTest} seam
 * pattern. Covers registration ordering, reference building, outcome classification, per-file
 * failure isolation and dry-run behaviour.
 */
class ApicurioSchemaPublisherTest {

    private static final String GROUP = "unit-group";

    private static final String CORE_PATH = "common/v1/core.proto";
    private static final String USER_PATH = "common/v1/user.proto";
    private static final String APP_PATH = "app/v1/app.proto";

    private static final String CORE_PROTO = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
            }
            """;

    private static final String USER_PROTO = """
            syntax = "proto3";
            package common.v1;
            import "common/v1/core.proto";
            message User {
              Core core = 1;
            }
            """;

    private static final String APP_PROTO = """
            syntax = "proto3";
            package app.v1;
            import "common/v1/user.proto";
            import "google/protobuf/timestamp.proto";
            message App {
              common.v1.User owner = 1;
              google.protobuf.Timestamp created = 2;
            }
            """;

    private static final String STANDALONE_PROTO = """
            syntax = "proto3";
            package other.v1;
            message Standalone {
              string name = 1;
            }
            """;

    private final FakeStore store = new FakeStore();
    private final ApicurioSchemaPublisher publisher =
            new ApicurioSchemaPublisher(store, GROUP, "apicurio:test group=" + GROUP);

    private static ProtoSourceSet chainSet() {
        // Inserted in NON-topological order (root first) to prove the publisher reorders.
        return ProtoSourceSet.builder()
                .add(APP_PATH, APP_PROTO, "test")
                .add(USER_PATH, USER_PROTO, "test")
                .add(CORE_PATH, CORE_PROTO, "test")
                .build();
    }

    // ---------------------------------------------------------------- fresh publish

    @Test
    void publishesImportsFirstWithExactReferences() throws Exception {
        PublishResult result = publisher.publish(chainSet(), PublishOptions.defaults());

        assertThat(result.outcomes())
                .extracting(FileOutcome::path, FileOutcome::subject, FileOutcome::action)
                .containsExactly(
                        tuple(CORE_PATH, CORE_PATH, Action.CREATED),
                        tuple(USER_PATH, USER_PATH, Action.CREATED),
                        tuple(APP_PATH, APP_PATH, Action.CREATED));
        assertThat(result.outcomes()).allSatisfy(o -> assertThat(o.detail()).isEqualTo("version 1"));

        // Registration order is imports-first.
        assertThat(store.createCalls).containsExactly(CORE_PATH, USER_PATH, APP_PATH);

        // Reference lists carry exact {name, groupId, artifactId, version};
        // google/protobuf/* is skipped.
        assertThat(store.latest(CORE_PATH).references()).isEmpty();
        assertThat(store.latest(USER_PATH).references())
                .containsExactly(new Reference(CORE_PATH, GROUP, CORE_PATH, "1"));
        assertThat(store.latest(APP_PATH).references())
                .containsExactly(new Reference(USER_PATH, GROUP, USER_PATH, "1"));
    }

    @Test
    void namingStrategyAppliesToArtifactIdsButNotReferenceNames() throws Exception {
        PublishOptions options = PublishOptions.defaults()
                .withNaming(SubjectNamingStrategy.prefixed("schemas/"));

        PublishResult result = publisher.publish(chainSet(), options);

        assertThat(result.outcomes()).extracting(FileOutcome::subject)
                .containsExactly("schemas/" + CORE_PATH, "schemas/" + USER_PATH, "schemas/" + APP_PATH);
        // The reference name stays the raw import path; only the artifactId is renamed.
        assertThat(store.latest("schemas/" + USER_PATH).references())
                .containsExactly(new Reference(CORE_PATH, GROUP, "schemas/" + CORE_PATH, "1"));
    }

    // ---------------------------------------------------------------- idempotency

    @Test
    void republishingIdenticalContentIsUnchangedWithoutNewVersions() throws Exception {
        publisher.publish(chainSet(), PublishOptions.defaults());
        store.createCalls.clear();

        PublishResult second = publisher.publish(chainSet(), PublishOptions.defaults());

        assertThat(second.outcomes()).extracting(FileOutcome::action).containsOnly(Action.UNCHANGED);
        assertThat(second.outcomes()).allSatisfy(o -> assertThat(o.detail()).isEqualTo("version 1"));
        // FIND_OR_CREATE_VERSION is still invoked, but no new version materializes.
        assertThat(store.versionCount(CORE_PATH)).isEqualTo(1);
        assertThat(store.versionCount(USER_PATH)).isEqualTo(1);
        assertThat(store.versionCount(APP_PATH)).isEqualTo(1);
    }

    @Test
    void changedFileIsUpdatedOnlyForThatFile() throws Exception {
        publisher.publish(chainSet(), PublishOptions.defaults());

        String changedApp = APP_PROTO.replace("created = 2;", "created = 2;\n  string note = 3;");
        ProtoSourceSet changed = ProtoSourceSet.builder()
                .add(CORE_PATH, CORE_PROTO, "test")
                .add(USER_PATH, USER_PROTO, "test")
                .add(APP_PATH, changedApp, "test")
                .build();

        PublishResult result = publisher.publish(changed, PublishOptions.defaults());

        assertThat(result.outcomes())
                .extracting(FileOutcome::path, FileOutcome::action)
                .containsExactly(
                        tuple(CORE_PATH, Action.UNCHANGED),
                        tuple(USER_PATH, Action.UNCHANGED),
                        tuple(APP_PATH, Action.UPDATED));
        assertThat(outcomeFor(result, APP_PATH).detail()).isEqualTo("version 2");
        assertThat(store.versionCount(APP_PATH)).isEqualTo(2);
    }

    @Test
    void artifactCreatedOutsideTheRunIsUpdatedNotCreated() throws Exception {
        store.preload(CORE_PATH, "// some earlier revision\n" + CORE_PROTO, List.of());

        PublishResult result = publisher.publish(
                ProtoSourceSet.builder().add(CORE_PATH, CORE_PROTO, "test").build(),
                PublishOptions.defaults());

        assertThat(outcomeFor(result, CORE_PATH).action()).isEqualTo(Action.UPDATED);
        assertThat(outcomeFor(result, CORE_PATH).detail()).isEqualTo("version 2");
    }

    @Test
    void importAlreadyInRegistryResolvesToItsLatestVersion() throws Exception {
        store.preload(CORE_PATH, "// v1\n" + CORE_PROTO, List.of());
        store.preload(CORE_PATH, CORE_PROTO, List.of());

        PublishResult result = publisher.publish(
                ProtoSourceSet.builder().add(USER_PATH, USER_PROTO, "test").build(),
                PublishOptions.defaults());

        assertThat(outcomeFor(result, USER_PATH).action()).isEqualTo(Action.CREATED);
        assertThat(store.latest(USER_PATH).references())
                .containsExactly(new Reference(CORE_PATH, GROUP, CORE_PATH, "2"));
    }

    // ---------------------------------------------------------------- per-file failures

    @Test
    void ruleViolationFailsThatFileAndItsDependentsButContinues() throws Exception {
        RuleViolationProblemDetails violation = new RuleViolationProblemDetails();
        violation.setTitle("Incompatible content");
        violation.setDetail("field id removed without reservation");
        violation.setStatus(409);
        store.failOnCreate.put(USER_PATH, violation);

        ProtoSourceSet set = chainSet().merge(
                ProtoSourceSet.builder().add("other/v1/standalone.proto", STANDALONE_PROTO, "test").build());
        PublishResult result = publisher.publish(set, PublishOptions.defaults());

        assertThat(outcomeFor(result, CORE_PATH).action()).isEqualTo(Action.CREATED);
        FileOutcome user = outcomeFor(result, USER_PATH);
        assertThat(user.action()).isEqualTo(Action.FAILED);
        assertThat(user.detail()).contains("Incompatible content").contains("409");
        FileOutcome app = outcomeFor(result, APP_PATH);
        assertThat(app.action()).isEqualTo(Action.FAILED);
        assertThat(app.detail()).contains(USER_PATH);
        // Remaining independent files are still attempted.
        assertThat(outcomeFor(result, "other/v1/standalone.proto").action()).isEqualTo(Action.CREATED);

        assertThatThrownBy(result::throwIfFailed)
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining("2 of 4")
                .hasMessageContaining(USER_PATH)
                .hasMessageContaining(APP_PATH);
    }

    @Test
    void missingImportFailsWithImportNameAndOthersContinue() throws Exception {
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(USER_PATH, USER_PROTO, "test") // imports core, which is nowhere
                .add("other/v1/standalone.proto", STANDALONE_PROTO, "test")
                .build();

        PublishResult result = publisher.publish(set, PublishOptions.defaults());

        FileOutcome user = outcomeFor(result, USER_PATH);
        assertThat(user.action()).isEqualTo(Action.FAILED);
        assertThat(user.detail()).contains(CORE_PATH).contains("neither in the source set");
        assertThat(outcomeFor(result, "other/v1/standalone.proto").action()).isEqualTo(Action.CREATED);
    }

    // ---------------------------------------------------------------- dry run

    @Test
    void dryRunReportsWouldWriteAndPerformsNoWrites() throws Exception {
        PublishResult result = publisher.publish(chainSet(), PublishOptions.dryRunDefaults());

        assertThat(result.outcomes()).extracting(FileOutcome::action).containsOnly(Action.WOULD_WRITE);
        assertThat(store.createCalls).isEmpty();
        assertThat(store.artifacts).isEmpty();
    }

    @Test
    void dryRunAfterPublishReportsUnchangedAndChangedFileAsWouldWrite() throws Exception {
        publisher.publish(chainSet(), PublishOptions.defaults());
        store.createCalls.clear();

        PublishResult identical = publisher.publish(chainSet(), PublishOptions.dryRunDefaults());
        assertThat(identical.outcomes()).extracting(FileOutcome::action).containsOnly(Action.UNCHANGED);

        String changedApp = APP_PROTO.replace("created = 2;", "created = 2;\n  string note = 3;");
        ProtoSourceSet changed = ProtoSourceSet.builder()
                .add(CORE_PATH, CORE_PROTO, "test")
                .add(USER_PATH, USER_PROTO, "test")
                .add(APP_PATH, changedApp, "test")
                .build();
        PublishResult result = publisher.publish(changed, PublishOptions.dryRunDefaults());

        assertThat(result.outcomes())
                .extracting(FileOutcome::path, FileOutcome::action)
                .containsExactly(
                        tuple(CORE_PATH, Action.UNCHANGED),
                        tuple(USER_PATH, Action.UNCHANGED),
                        tuple(APP_PATH, Action.WOULD_WRITE));
        assertThat(store.createCalls).isEmpty();
        assertThat(store.versionCount(APP_PATH)).isEqualTo(1);
    }

    // ---------------------------------------------------------------- registry-level failures

    @Test
    void authFailureThrowsSchemaPublishException() {
        ProblemDetails unauthorized = new ProblemDetails();
        unauthorized.setTitle("Unauthorized");
        unauthorized.setStatus(401);
        store.failAllReads = unauthorized;

        assertThatThrownBy(() -> publisher.publish(chainSet(), PublishOptions.defaults()))
                .isInstanceOf(SchemaPublishException.class);
    }

    @Test
    void serverErrorOnWriteThrowsSchemaPublishException() {
        ProblemDetails serverError = new ProblemDetails();
        serverError.setTitle("Internal server error");
        serverError.setStatus(500);
        store.failOnCreate.put(CORE_PATH, serverError);

        assertThatThrownBy(() -> publisher.publish(chainSet(), PublishOptions.defaults()))
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining(CORE_PATH);
    }

    @Test
    void targetDescribesRegistry() {
        assertThat(publisher.target()).isEqualTo("apicurio:test group=" + GROUP);
        assertThat(publisher.getGroupId()).isEqualTo(GROUP);
    }

    @Test
    void builderRequiresRegistryUrlAndGroupId() {
        assertThatThrownBy(() -> ApicurioSchemaPublisher.builder().build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Registry URL");
        assertThatThrownBy(() -> ApicurioSchemaPublisher.builder()
                .registryUrl("http://localhost:1").groupId(" ").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Group ID");
    }

    private static FileOutcome outcomeFor(PublishResult result, String path) {
        return result.outcomes().stream()
                .filter(o -> o.path().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No outcome for " + path));
    }

    // ---------------------------------------------------------------- fake store

    /** In-memory registry with FIND_OR_CREATE_VERSION semantics and scriptable failures. */
    private static final class FakeStore implements ArtifactStore {

        record StoredVersion(String version, long globalId, String content, List<Reference> references) {
        }

        final Map<String, List<StoredVersion>> artifacts = new LinkedHashMap<>();
        final List<String> createCalls = new ArrayList<>();
        final Map<String, ApiException> failOnCreate = new LinkedHashMap<>();
        ApiException failAllReads;
        private long nextGlobalId = 1;

        StoredVersion latest(String artifactId) {
            List<StoredVersion> versions = artifacts.get(artifactId);
            return versions == null || versions.isEmpty() ? null : versions.getLast();
        }

        int versionCount(String artifactId) {
            List<StoredVersion> versions = artifacts.get(artifactId);
            return versions == null ? 0 : versions.size();
        }

        void preload(String artifactId, String content, List<Reference> references) {
            List<StoredVersion> versions = artifacts.computeIfAbsent(artifactId, a -> new ArrayList<>());
            versions.add(new StoredVersion(
                    String.valueOf(versions.size() + 1), nextGlobalId++, content, List.copyOf(references)));
        }

        @Override
        public VersionInfo latestVersion(String artifactId) throws Exception {
            if (failAllReads != null) {
                throw failAllReads;
            }
            StoredVersion latest = latest(artifactId);
            return latest == null ? null : new VersionInfo(latest.version(), latest.globalId());
        }

        @Override
        public String latestContent(String artifactId) throws Exception {
            if (failAllReads != null) {
                throw failAllReads;
            }
            StoredVersion latest = latest(artifactId);
            return latest == null ? null : latest.content();
        }

        @Override
        public VersionInfo createOrFindVersion(String artifactId, String content,
                                               List<Reference> references) throws Exception {
            createCalls.add(artifactId);
            ApiException failure = failOnCreate.get(artifactId);
            if (failure != null) {
                throw failure;
            }
            List<StoredVersion> versions = artifacts.computeIfAbsent(artifactId, a -> new ArrayList<>());
            for (StoredVersion stored : versions) {
                if (stored.content().equals(content) && stored.references().equals(references)) {
                    return new VersionInfo(stored.version(), stored.globalId());
                }
            }
            StoredVersion created = new StoredVersion(
                    String.valueOf(versions.size() + 1), nextGlobalId++, content, List.copyOf(references));
            versions.add(created);
            return new VersionInfo(created.version(), created.globalId());
        }
    }
}
