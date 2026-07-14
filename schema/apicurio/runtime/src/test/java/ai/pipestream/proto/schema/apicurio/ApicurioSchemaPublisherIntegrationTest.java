package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.sources.publish.PublishResult;
import ai.pipestream.proto.sources.publish.PublishResult.Action;
import ai.pipestream.proto.sources.publish.PublishResult.FileOutcome;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.models.ArtifactSearchResults;
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
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for {@link ApicurioSchemaPublisher} against a live Apicurio Registry 3.x
 * (native v3 API), mirroring {@link ApicurioDescriptorLoaderIntegrationTest}.
 *
 * <p>Start the registry with {@code docker compose -f docker-compose.integration.yml up -d}
 * (repo root). When the registry is not reachable these tests skip via JUnit assumptions, so
 * {@code ./gradlew build} stays green without containers.</p>
 *
 * <p>Endpoint override: {@code -Dpipestream.it.apicurio.url=...} or env
 * {@code PIPESTREAM_IT_APICURIO_URL} (default {@code http://localhost:18780}).</p>
 *
 * <p>Artifacts are registered under unique per-run groups so reruns never collide. The shared
 * registry has deletion disabled (HTTP 405), so no cleanup is attempted.</p>
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApicurioSchemaPublisherIntegrationTest {

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package pipestream.it.pub.v1;
            message Address {
              string street = 1;
              string city = 2;
            }
            """;

    private static final String EMPLOYEE_PROTO = """
            syntax = "proto3";
            package pipestream.it.pub.v1;
            import "common.proto";
            message Employee {
              string name = 1;
              pipestream.it.pub.v1.Address address = 2;
            }
            """;

    private static final String COMPANY_PROTO = """
            syntax = "proto3";
            package pipestream.it.pub.v1;
            import "employee.proto";
            import "google/protobuf/timestamp.proto";
            message Company {
              string name = 1;
              repeated pipestream.it.pub.v1.Employee employees = 2;
              google.protobuf.Timestamp founded = 3;
            }
            """;

    private final String registryUrl = ApicurioDescriptorLoaderIntegrationTest.configuredRegistryUrl();
    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private final String groupId = "pipestream-it-pub-" + runId;
    private final String dryRunGroupId = "pipestream-it-pub-dry-" + runId;

    private HttpClient http;
    private RegistryClient registryClient;

    @BeforeAll
    void setUp() {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        assumeTrue(registryReachable(),
                "Apicurio Registry not reachable at " + registryUrl + " - skipping integration tests");
        registryClient = RegistryClientFactory.create(RegistryClientOptions.create(registryUrl));
    }

    private boolean registryReachable() {
        try {
            HttpRequest request = HttpRequest.newBuilder(
                            URI.create(registryUrl + "/apis/registry/v3/system/info"))
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

    private ApicurioSchemaPublisher publisherFor(String group) {
        return ApicurioSchemaPublisher.builder()
                .registryUrl(registryUrl)
                .groupId(group)
                .registryClient(registryClient)
                .build();
    }

    private static ProtoSourceSet companySet() {
        // Inserted in NON-topological order (root first) to prove the publisher reorders.
        return ProtoSourceSet.builder()
                .add("company.proto", COMPANY_PROTO, "it")
                .add("employee.proto", EMPLOYEE_PROTO, "it")
                .add("common.proto", COMMON_PROTO, "it")
                .build();
    }

    /**
     * End-to-end write path: publish a transitive reference chain
     * ({@code company -> employee -> common}, plus a well-known import), round-trip it
     * through {@link ApicurioDescriptorLoader} (the root type must resolve with its imported
     * types), then re-publish and observe UNCHANGED idempotency.
     */
    @Test
    void publishesRoundTripsAndRepublishesUnchanged() throws Exception {
        ApicurioSchemaPublisher publisher = publisherFor(groupId);
        PublishResult first = publisher.publish(companySet(), PublishOptions.defaults());

        first.throwIfFailed();
        assertThat(first.outcomes())
                .extracting(FileOutcome::path, FileOutcome::action)
                .containsExactly(
                        tuple("common.proto", Action.CREATED),
                        tuple("employee.proto", Action.CREATED),
                        tuple("company.proto", Action.CREATED));

        // Round-trip: the loader resolves the registered references back into descriptors.
        ApicurioDescriptorLoader loader = ApicurioDescriptorLoader.builder()
                .registryUrl(registryUrl)
                .groupId(groupId)
                .registryClient(registryClient)
                .build();
        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors).hasSize(3);

        Descriptor company = descriptors.stream()
                .map(fd -> fd.findMessageTypeByName("Company"))
                .filter(java.util.Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Company not loaded back from " + groupId));
        FieldDescriptor employees = company.findFieldByName("employees");
        assertThat(employees.isRepeated()).isTrue();
        Descriptor employee = employees.getMessageType();
        assertThat(employee.getFullName()).isEqualTo("pipestream.it.pub.v1.Employee");
        assertThat(employee.findFieldByName("address").getMessageType().getFullName())
                .isEqualTo("pipestream.it.pub.v1.Address");
        assertThat(company.findFieldByName("founded").getMessageType().getFullName())
                .isEqualTo("google.protobuf.Timestamp");

        // Idempotency: identical content re-publishes as UNCHANGED, creating no versions.
        PublishResult second = publisher.publish(companySet(), PublishOptions.defaults());
        assertThat(second.outcomes()).extracting(FileOutcome::action).containsOnly(Action.UNCHANGED);
        assertThat(second.outcomes()).allSatisfy(o -> assertThat(o.detail()).isEqualTo("version 1"));
    }

    /** Dry run reports WOULD_WRITE for everything and leaves the group empty. */
    @Test
    void dryRunWritesNothing() throws Exception {
        ApicurioSchemaPublisher publisher = publisherFor(dryRunGroupId);
        PublishResult result = publisher.publish(companySet(), PublishOptions.dryRunDefaults());

        assertThat(result.outcomes()).extracting(FileOutcome::action).containsOnly(Action.WOULD_WRITE);
        assertThat(artifactCount(dryRunGroupId)).isZero();
    }

    private long artifactCount(String group) {
        ArtifactSearchResults results = registryClient.search().artifacts().get(config -> {
            config.queryParameters.groupId = group;
            config.queryParameters.limit = 100;
        });
        Integer count = results == null ? null : results.getCount();
        return count == null ? 0 : count;
    }
}
