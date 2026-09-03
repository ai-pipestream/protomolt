package ai.protomolt.proto.schema.apicurio;

import ai.protomolt.proto.schema.apicurio.ApicurioSchemaPublisher.ArtifactStore;
import ai.protomolt.proto.schema.apicurio.ApicurioSchemaPublisher.Reference;
import ai.protomolt.proto.schema.apicurio.ApicurioSchemaPublisher.VersionInfo;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.sources.publish.PublishOptions;
import ai.protomolt.proto.sources.publish.PublishResult;
import ai.protomolt.proto.sources.publish.PublishResult.Action;
import ai.protomolt.proto.sources.publish.PublishResult.FileOutcome;
import ai.protomolt.proto.sources.publish.SchemaPublishException;
import com.microsoft.kiota.ApiException;
import io.apicurio.registry.rest.client.models.ProblemDetails;
import io.apicurio.registry.rest.client.models.RuleViolationProblemDetails;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Additional {@link ApicurioSchemaPublisher} tests focused on outcome classification and
 * failure taxonomy, over a scriptable in-memory {@link ArtifactStore} (same seam as
 * {@link ApicurioSchemaPublisherTest}).
 */
class ApicurioSchemaPublisherClassificationTest {

    private static final String GROUP = "unit-group";

    private static final String PATH = "common/v1/core.proto";
    private static final String PROTO = """
            syntax = "proto3";
            package common.v1;
            message Core {
              string id = 1;
            }
            """;

    private final ScriptableStore store = new ScriptableStore();
    private final ApicurioSchemaPublisher publisher =
            new ApicurioSchemaPublisher(store, GROUP, "apicurio:test group=" + GROUP);

    // ---------------------------------------------------------------- classify fallbacks

    @Test
    void equalVersionsWithMissingGlobalIdsClassifyAsUnchanged() throws Exception {
        // Global IDs unavailable: classification falls back to version comparison.
        store.latest = new VersionInfo("3", null);
        store.created = new VersionInfo("3", null);

        PublishResult result = publishSingleFile();

        assertThat(result.outcomes()).extracting(FileOutcome::action).containsExactly(Action.UNCHANGED);
    }

    @Test
    void differentVersionsWithMissingGlobalIdsClassifyAsUpdated() throws Exception {
        store.latest = new VersionInfo("3", null);
        store.created = new VersionInfo("4", null);

        PublishResult result = publishSingleFile();

        assertThat(result.outcomes()).extracting(FileOutcome::action).containsExactly(Action.UPDATED);
    }

    @Test
    void foundVersionWithLowerGlobalIdClassifiesAsUnchanged() throws Exception {
        store.latest = new VersionInfo("3", 30L);
        store.created = new VersionInfo("2", 20L); // FIND_OR_CREATE found an older version

        PublishResult result = publishSingleFile();

        assertThat(result.outcomes()).extracting(FileOutcome::action).containsExactly(Action.UNCHANGED);
        assertThat(result.outcomes().getFirst().detail()).isEqualTo("version 2");
    }

    @Test
    void brandNewArtifactClassifiesAsCreated() throws Exception {
        store.latest = null;
        store.created = new VersionInfo("1", 1L);

        PublishResult result = publishSingleFile();

        assertThat(result.outcomes()).extracting(FileOutcome::action).containsExactly(Action.CREATED);
    }

    // ---------------------------------------------------------------- interruption

    @Test
    void interruptedReadAbortsAndRestoresTheInterruptFlag() {
        store.readFailure = new InterruptedException("stop");
        try {
            assertThatThrownBy(this::publishSingleFile)
                    .isInstanceOf(SchemaPublishException.class)
                    .hasMessageContaining("Interrupted");
            assertThat(Thread.currentThread().isInterrupted())
                    .as("the interrupt flag must be restored")
                    .isTrue();
        } finally {
            Thread.interrupted(); // do not leak the flag into other tests
        }
    }

    @Test
    void interruptedCreateAbortsAndRestoresTheInterruptFlag() {
        store.latest = null;
        store.createFailure = new InterruptedException("stop");
        try {
            assertThatThrownBy(this::publishSingleFile)
                    .isInstanceOf(SchemaPublishException.class)
                    .hasMessageContaining("Interrupted while publishing artifact");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    // ---------------------------------------------------------------- rejection taxonomy

    @Test
    void conflictProblemDetailsFailsOnlyThatFile() throws Exception {
        ProblemDetails conflict = new ProblemDetails();
        conflict.setStatus(409);
        conflict.setTitle("Already exists under different content");
        store.latest = null;
        store.createFailure = conflict;

        PublishResult result = publishSingleFile();

        FileOutcome outcome = result.outcomes().getFirst();
        assertThat(outcome.action()).isEqualTo(Action.FAILED);
        assertThat(outcome.detail()).contains("409").contains("Already exists under different content");
    }

    @Test
    void problemDetailsWithZeroStatusFallsBackToTransportStatus() throws Exception {
        // statusOf prefers the problem document's status but treats 0/absent as "use the
        // transport status": a 0-status 409 problem is still a per-artifact rejection.
        StatusProblemDetails conflict = new StatusProblemDetails();
        conflict.setStatus(0);
        conflict.responseStatusCode(409);
        store.latest = null;
        store.createFailure = conflict;

        PublishResult result = publishSingleFile();

        assertThat(result.outcomes()).extracting(FileOutcome::action).containsExactly(Action.FAILED);
        assertThat(result.outcomes().getFirst().detail()).contains("409");
    }

    @Test
    void zeroStatusProblemWithServerErrorTransportStatusAbortsThePublish() {
        StatusProblemDetails serverError = new StatusProblemDetails();
        serverError.setStatus(0);
        serverError.responseStatusCode(500);
        store.latest = null;
        store.createFailure = serverError;

        assertThatThrownBy(this::publishSingleFile)
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining(PATH);
    }

    @Test
    void ruleViolationIsPerArtifactEvenWithoutAnyStatus() throws Exception {
        RuleViolationProblemDetails violation = new RuleViolationProblemDetails();
        violation.setTitle("Invalid content");
        // No status anywhere: instanceof alone makes it a per-artifact rejection.
        store.latest = null;
        store.createFailure = violation;

        PublishResult result = publishSingleFile();

        FileOutcome outcome = result.outcomes().getFirst();
        assertThat(outcome.action()).isEqualTo(Action.FAILED);
        assertThat(outcome.detail()).contains("Invalid content");
    }

    @Test
    void nonApiExceptionWriteFailureAbortsThePublish() {
        store.latest = null;
        store.createFailure = new IllegalStateException("connection reset");

        assertThatThrownBy(this::publishSingleFile)
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining(PATH)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void readFailureAbortsTheWholePublish() {
        store.readFailure = new ApiException("HTTP 503") {
            {
                setResponseStatusCode(503);
            }
        };

        assertThatThrownBy(this::publishSingleFile)
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining("reading latest version");
    }

    // ---------------------------------------------------------------- dry run details

    @Test
    void dryRunChainReportsPreciseReasonsPerFile() throws Exception {
        String userPath = "common/v1/user.proto";
        String userProto = """
                syntax = "proto3";
                package common.v1;
                import "common/v1/core.proto";
                message User {
                  Core core = 1;
                }
                """;
        ProtoSourceSet set = ProtoSourceSet.builder()
                .add(PATH, PROTO, "test")
                .add(userPath, userProto, "test")
                .build();

        PublishResult result = publisher.publish(set, PublishOptions.dryRunDefaults());

        FileOutcome core = result.outcomes().get(0);
        assertThat(core.action()).isEqualTo(Action.WOULD_WRITE);
        assertThat(core.detail()).isEqualTo("artifact would be created");
        FileOutcome user = result.outcomes().get(1);
        assertThat(user.action()).isEqualTo(Action.WOULD_WRITE);
        assertThat(user.detail()).isEqualTo("depends on files that would be written in this run");
    }

    // ---------------------------------------------------------------- argument validation

    @Test
    void nullArgumentsAreRejected() {
        ProtoSourceSet set = ProtoSourceSet.builder().add(PATH, PROTO, "test").build();
        assertThatThrownBy(() -> publisher.publish(null, PublishOptions.defaults()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> publisher.publish(set, null))
                .isInstanceOf(NullPointerException.class);
    }

    private PublishResult publishSingleFile() throws SchemaPublishException {
        return publisher.publish(
                ProtoSourceSet.builder().add(PATH, PROTO, "test").build(), PublishOptions.defaults());
    }

    /** Exposes the transport status setter so classification tests can model SDK errors. */
    private static final class StatusProblemDetails extends ProblemDetails {

        void responseStatusCode(int status) {
            setResponseStatusCode(status);
        }
    }

    // ---------------------------------------------------------------- scriptable store

    /** Single-artifact store: every call hits the scripted latest/created/failure fields. */
    private static final class ScriptableStore implements ArtifactStore {

        VersionInfo latest;
        VersionInfo created = new VersionInfo("1", 1L);
        Exception readFailure;
        Exception createFailure;

        @Override
        public VersionInfo latestVersion(String artifactId) throws Exception {
            if (readFailure != null) {
                throw readFailure;
            }
            return latest;
        }

        @Override
        public String latestContent(String artifactId) throws Exception {
            if (readFailure != null) {
                throw readFailure;
            }
            return null;
        }

        @Override
        public VersionInfo createOrFindVersion(String artifactId, String content,
                                               List<Reference> references) throws Exception {
            if (createFailure != null) {
                throw createFailure;
            }
            return created;
        }
    }
}
