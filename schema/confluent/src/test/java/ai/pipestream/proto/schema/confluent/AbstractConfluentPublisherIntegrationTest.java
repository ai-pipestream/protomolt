package ai.pipestream.proto.schema.confluent;

import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.sources.publish.PublishResult;
import ai.pipestream.proto.sources.publish.PublishResult.Action;
import ai.pipestream.proto.sources.publish.PublishResult.FileOutcome;
import ai.pipestream.proto.sources.publish.SubjectNamingStrategy;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for {@link ConfluentSchemaPublisher} against a live Confluent-compatible
 * registry, run once per endpoint via the concrete subclasses (Apicurio ccompat facade and
 * real Confluent Schema Registry), mirroring {@link AbstractConfluentCompatIntegrationTest}.
 *
 * <p>Start the registries with {@code docker compose -f docker-compose.integration.yml up -d}
 * (repo root). When the registry is not reachable these tests skip via JUnit assumptions, so
 * {@code ./gradlew build} stays green without containers.</p>
 *
 * <p>Subjects are registered under unique per-run prefixes so reruns never collide; a
 * best-effort cleanup deletes them afterwards.</p>
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractConfluentPublisherIntegrationTest {

    private static final String PERSON_PROTO = """
            syntax = "proto3";
            package pipestream.it.pub.v1;
            message Person {
              string name = 1;
              int32 id = 2;
            }
            """;

    private static final String TEAM_PROTO = """
            syntax = "proto3";
            package pipestream.it.pub.v1;
            import "person.proto";
            import "google/protobuf/timestamp.proto";
            message Team {
              string name = 1;
              repeated Person members = 2;
              google.protobuf.Timestamp created = 3;
            }
            """;

    private static final String INCOMPAT_V1_PROTO = """
            syntax = "proto3";
            package pipestream.it.pub.v1;
            message Audit {
              string actor = 1;
            }
            """;

    /** Field 1 changes type (string -> int32): backward-incompatible for both registries. */
    private static final String INCOMPAT_V2_PROTO = """
            syntax = "proto3";
            package pipestream.it.pub.v1;
            message Audit {
              int32 actor = 1;
            }
            """;

    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private final String prefix = "pipestream-it-pub-" + runId + "-";
    private final SubjectNamingStrategy naming = SubjectNamingStrategy.prefixed(prefix);
    private HttpClient http;

    /** Base URL of the Confluent-compatible API (no trailing slash). */
    abstract String registryBaseUrl();

    @BeforeAll
    void setUp() {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        assumeTrue(registryReachable(),
                "Schema registry not reachable at " + registryBaseUrl() + " - skipping integration tests");
    }

    @AfterAll
    void cleanUp() {
        if (http == null) {
            return;
        }
        // Referencing subject first, so the referenced one is free to delete.
        for (String subject : List.of(prefix + "team.proto", prefix + "person.proto",
                prefix + "audit.proto")) {
            deleteSubject(subject, false);
            deleteSubject(subject, true);
        }
    }

    private ProtoSourceSet personTeamSet() {
        return ProtoSourceSet.builder()
                .add("team.proto", TEAM_PROTO, "it")
                .add("person.proto", PERSON_PROTO, "it")
                .build();
    }

    /**
     * End-to-end write path: publish a set with a schema reference and a well-known import,
     * round-trip it through {@link ConfluentSchemaRegistryLoader} (the root type must resolve
     * with its imported types), then re-publish and observe UNCHANGED idempotency.
     */
    @Test
    void publishesRoundTripsAndRepublishesUnchanged() throws Exception {
        try (ConfluentSchemaPublisher publisher =
                     new ConfluentSchemaPublisher(URI.create(registryBaseUrl()))) {
            PublishResult first = publisher.publish(personTeamSet(),
                    new PublishOptions(naming, false));

            first.throwIfFailed();
            assertThat(first.outcomes())
                    .extracting(FileOutcome::path, FileOutcome::action)
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("person.proto", Action.CREATED),
                            org.assertj.core.groups.Tuple.tuple("team.proto", Action.CREATED));

            // Round-trip: the loader compiles the published subjects back into descriptors.
            try (ConfluentSchemaRegistryLoader loader =
                         new ConfluentSchemaRegistryLoader(URI.create(registryBaseUrl()))) {
                List<FileDescriptor> descriptors = loader.loadDescriptors();
                Descriptor team = descriptors.stream()
                        .map(fd -> fd.findMessageTypeByName("Team"))
                        .filter(java.util.Objects::nonNull)
                        .filter(d -> d.getFullName().equals("pipestream.it.pub.v1.Team"))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError(
                                "Published Team schema not loaded back from " + registryBaseUrl()));
                FieldDescriptor members = team.findFieldByName("members");
                assertThat(members.isRepeated()).isTrue();
                assertThat(members.getMessageType().getFullName())
                        .isEqualTo("pipestream.it.pub.v1.Person");
                assertThat(members.getMessageType().findFieldByName("id").getNumber()).isEqualTo(2);
                assertThat(team.findFieldByName("created").getMessageType().getFullName())
                        .isEqualTo("google.protobuf.Timestamp");
            }

            // Idempotency: identical content re-publishes as UNCHANGED, creating no versions.
            PublishResult second = publisher.publish(personTeamSet(),
                    new PublishOptions(naming, false));
            assertThat(second.outcomes()).extracting(FileOutcome::action)
                    .containsOnly(Action.UNCHANGED);
        }
    }

    /**
     * A change the registry's BACKWARD compatibility policy rejects surfaces as a FAILED
     * outcome carrying the registry's message, not as a thrown exception. Compatibility is set
     * explicitly on the subject: the ccompat facade does not default to BACKWARD the way a
     * stock Confluent Schema Registry does.
     */
    @Test
    void incompatibleChangeYieldsFailedOutcomeWithRegistryMessage() throws Exception {
        String subject = prefix + "audit.proto";
        try (ConfluentSchemaPublisher publisher =
                     new ConfluentSchemaPublisher(URI.create(registryBaseUrl()))) {
            PublishResult first = publisher.publish(
                    ProtoSourceSet.builder().add("audit.proto", INCOMPAT_V1_PROTO, "it").build(),
                    new PublishOptions(naming, false));
            first.throwIfFailed();

            setCompatibility(subject, "BACKWARD");

            PublishResult second = publisher.publish(
                    ProtoSourceSet.builder().add("audit.proto", INCOMPAT_V2_PROTO, "it").build(),
                    new PublishOptions(naming, false));

            assertThat(second.outcomes()).hasSize(1);
            FileOutcome outcome = second.outcomes().getFirst();
            assertThat(outcome.action()).isEqualTo(Action.FAILED);
            assertThat(outcome.subject()).isEqualTo(subject);
            assertThat(outcome.detail()).contains("409");
        }
    }

    // ---------------------------------------------------------------- registry plumbing

    private boolean registryReachable() {
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(registryBaseUrl() + "/subjects"))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            return http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void setCompatibility(String subject, String level) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(registryBaseUrl() + "/config/" + subject))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .PUT(HttpRequest.BodyPublishers.ofString("{\"compatibility\":\"" + level + "\"}"))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("Failed to set compatibility on " + subject
                    + ": HTTP " + response.statusCode() + " " + response.body());
        }
    }

    private void deleteSubject(String subjectName, boolean permanent) {
        try {
            String suffix = permanent ? "?permanent=true" : "";
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create(registryBaseUrl() + "/subjects/" + subjectName + suffix))
                    .timeout(Duration.ofSeconds(5))
                    .DELETE()
                    .build();
            http.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException ignored) {
            // best effort
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
