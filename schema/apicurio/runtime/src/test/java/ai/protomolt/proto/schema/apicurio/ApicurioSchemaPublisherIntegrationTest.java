package ai.protomolt.proto.schema.apicurio;

import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.sources.publish.PublishOptions;
import ai.protomolt.proto.sources.publish.PublishResult;
import ai.protomolt.proto.sources.publish.PublishResult.Action;
import ai.protomolt.proto.sources.publish.PublishResult.FileOutcome;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
 * <p>The suite provisions its own registry, a Testcontainers Apicurio Registry (see
 * {@link ApicurioRegistryContainer}), and skips when Docker is unavailable, so
 * {@code ./gradlew build} stays green without containers.</p>
 *
 * <p>To run against an external registry instead (for example the compose stack's):
 * {@code -Dpipestream.it.apicurio.url=...} or env {@code PROTOMOLT_IT_APICURIO_URL}.
 * An unreachable override endpoint still skips via a JUnit assumption.</p>
 *
 * <p>Artifacts are registered under unique per-run groups so reruns never collide. The shared
 * registry has deletion disabled (HTTP 405), so no cleanup is attempted.</p>
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApicurioSchemaPublisherIntegrationTest {

    @Container
    static final ApicurioRegistryContainer REGISTRY = new ApicurioRegistryContainer();

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package protomolt.it.pub.v1;
            message Address {
              string street = 1;
              string city = 2;
            }
            """;

    private static final String EMPLOYEE_PROTO = """
            syntax = "proto3";
            package protomolt.it.pub.v1;
            import "common.proto";
            message Employee {
              string name = 1;
              protomolt.it.pub.v1.Address address = 2;
            }
            """;

    private static final String COMPANY_PROTO = """
            syntax = "proto3";
            package protomolt.it.pub.v1;
            import "employee.proto";
            import "google/protobuf/timestamp.proto";
            message Company {
              string name = 1;
              repeated protomolt.it.pub.v1.Employee employees = 2;
              google.protobuf.Timestamp founded = 3;
            }
            """;

    // Resolved in setUp: the container's mapped port only exists once it has started.
    private String registryUrl;
    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private final String groupId = "protomolt-it-pub-" + runId;
    private final String dryRunGroupId = "protomolt-it-pub-dry-" + runId;

    private HttpClient http;
    private RegistryClient registryClient;

    @BeforeAll
    void setUp() {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        registryUrl = ApicurioDescriptorLoaderIntegrationTest.configuredRegistryUrl(REGISTRY.getUrl());
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
        assertThat(employee.getFullName()).isEqualTo("protomolt.it.pub.v1.Employee");
        assertThat(employee.findFieldByName("address").getMessageType().getFullName())
                .isEqualTo("protomolt.it.pub.v1.Address");
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
