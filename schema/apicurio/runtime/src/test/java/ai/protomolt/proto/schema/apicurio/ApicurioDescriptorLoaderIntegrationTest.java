package ai.protomolt.proto.schema.apicurio;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.apicurio.registry.client.RegistryClientFactory;
import io.apicurio.registry.client.common.RegistryClientOptions;
import io.apicurio.registry.rest.client.RegistryClient;
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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for {@link ApicurioDescriptorLoader} against a live Apicurio Registry 3.x
 * (native v3 API).
 *
 * <p>The suite provisions its own registry, a Testcontainers Apicurio Registry (see
 * {@link ApicurioRegistryContainer}), and skips when Docker is unavailable, so
 * {@code ./gradlew build} stays green without containers.</p>
 *
 * <p>To run against an external registry instead (for example the compose stack's):
 * {@code -Dpipestream.it.apicurio.url=...} or env {@code PIPESTREAM_IT_APICURIO_URL}.
 * An unreachable override endpoint still skips via a JUnit assumption.</p>
 *
 * <p>Artifacts are registered under unique per-run groups so reruns never collide. The shared
 * registry has deletion disabled (HTTP 405), so no cleanup is attempted.</p>
 */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ApicurioDescriptorLoaderIntegrationTest {

    @Container
    static final ApicurioRegistryContainer REGISTRY = new ApicurioRegistryContainer();

    private static final String PERSON_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            message Person {
              string name = 1;
              int32 id = 2;
              repeated string emails = 3;
            }
            """;

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            message Order {
              string order_id = 1;
              int64 total_cents = 2;
            }
            """;

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            message Address {
              string street = 1;
              string city = 2;
            }
            """;

    private static final String EMPLOYEE_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            import "common.proto";
            message Employee {
              string name = 1;
              pipestream.it.v1.Address address = 2;
            }
            """;

    private static final String COMPANY_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            import "employee.proto";
            message Company {
              string name = 1;
              repeated pipestream.it.v1.Employee employees = 2;
            }
            """;

    private static final String BASE_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            message Base {
              string id = 1;
            }
            """;

    private static final String LEFT_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            import "base.proto";
            message Left {
              pipestream.it.v1.Base base = 1;
            }
            """;

    private static final String RIGHT_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            import "base.proto";
            message Right {
              pipestream.it.v1.Base base = 1;
            }
            """;

    private static final String TOP_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            import "left.proto";
            import "right.proto";
            message Top {
              pipestream.it.v1.Left left = 1;
              pipestream.it.v1.Right right = 2;
            }
            """;

    private static final String DANGLING_PROTO = """
            syntax = "proto3";
            package pipestream.it.v1;
            import "missing.proto";
            message Dangling {
              pipestream.it.v1.Missing missing = 1;
            }
            """;

    // Resolved in setUp: the container's mapped port only exists once it has started.
    private String registryUrl;
    private final String runId = UUID.randomUUID().toString().substring(0, 8);
    private final String groupId = "pipestream-it-" + runId;
    private final String refGroupId = "pipestream-it-refs-" + runId;
    private final String transitiveGroupId = "pipestream-it-trans-" + runId;
    private final String diamondGroupId = "pipestream-it-diamond-" + runId;
    private final String danglingGroupId = "pipestream-it-dangling-" + runId;

    private HttpClient http;
    private RegistryClient registryClient;

    static String configuredRegistryUrl(String defaultUrl) {
        String url = System.getProperty("pipestream.it.apicurio.url");
        if (url == null || url.isBlank()) {
            url = System.getenv("PIPESTREAM_IT_APICURIO_URL");
        }
        if (url == null || url.isBlank()) {
            url = defaultUrl;
        }
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    @BeforeAll
    void setUp() throws Exception {
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        registryUrl = configuredRegistryUrl(REGISTRY.getUrl());
        assumeTrue(registryReachable(),
                "Apicurio Registry not reachable at " + registryUrl + " - skipping integration tests");

        // Same client construction path as ApicurioDescriptorLoaderProducer; the SDK
        // normalizes the base URL by appending /apis/registry/v3 when missing.
        registryClient = RegistryClientFactory.create(RegistryClientOptions.create(registryUrl));

        registerProto(groupId, "person.proto", PERSON_PROTO, null);
        registerProto(groupId, "order.proto", ORDER_PROTO, null);

        registerProto(refGroupId, "common.proto", COMMON_PROTO, null);
        registerProto(refGroupId, "employee.proto", EMPLOYEE_PROTO,
                "[" + referenceJson("common.proto", refGroupId) + "]");

        // Transitive chain: company.proto -> employee.proto -> common.proto
        registerProto(transitiveGroupId, "common.proto", COMMON_PROTO, null);
        registerProto(transitiveGroupId, "employee.proto", EMPLOYEE_PROTO,
                "[" + referenceJson("common.proto", transitiveGroupId) + "]");
        registerProto(transitiveGroupId, "company.proto", COMPANY_PROTO,
                "[" + referenceJson("employee.proto", transitiveGroupId) + "]");

        // Diamond: top.proto -> {left.proto, right.proto} -> base.proto
        registerProto(diamondGroupId, "base.proto", BASE_PROTO, null);
        registerProto(diamondGroupId, "left.proto", LEFT_PROTO,
                "[" + referenceJson("base.proto", diamondGroupId) + "]");
        registerProto(diamondGroupId, "right.proto", RIGHT_PROTO,
                "[" + referenceJson("base.proto", diamondGroupId) + "]");
        registerProto(diamondGroupId, "top.proto", TOP_PROTO,
                "[" + referenceJson("left.proto", diamondGroupId) + ","
                        + referenceJson("right.proto", diamondGroupId) + "]");

        // Dangling: references an artifact that was never registered (integrity rule is off,
        // so the registry accepts it; the loader must skip it gracefully).
        registerProto(danglingGroupId, "dangling.proto", DANGLING_PROTO,
                "[" + referenceJson("missing.proto", danglingGroupId) + "]");
    }

    /** Registry reference JSON: {@code name} is the import path, artifactId matches it. */
    private static String referenceJson(String importPath, String group) {
        return "{\"name\":\"" + importPath + "\",\"groupId\":\"" + group
                + "\",\"artifactId\":\"" + importPath + "\",\"version\":\"1\"}";
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

    private void registerProto(String group, String artifactId, String protoText, String referencesJson)
            throws IOException, InterruptedException {
        String references = referencesJson == null ? "" : ",\"references\":" + referencesJson;
        String body = "{\"artifactId\":\"" + artifactId + "\",\"artifactType\":\"PROTOBUF\","
                + "\"firstVersion\":{\"content\":{"
                + "\"content\":\"" + jsonEscape(protoText) + "\","
                + "\"contentType\":\"application/x-protobuf\""
                + references + "}}}";
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create(registryUrl + "/apis/registry/v3/groups/" + group + "/artifacts"))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IllegalStateException("Failed to register " + group + "/" + artifactId
                    + ": HTTP " + response.statusCode() + " " + response.body());
        }
    }

    private static String jsonEscape(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    private ApicurioDescriptorLoader loaderFor(String group) {
        return ApicurioDescriptorLoader.builder()
                .registryUrl(registryUrl)
                .groupId(group)
                .registryClient(registryClient)
                .build();
    }

    @Test
    void loadDescriptorsReturnsAllProtobufArtifactsInGroup() throws Exception {
        ApicurioDescriptorLoader loader = loaderFor(groupId);
        assertThat(loader.isAvailable()).isTrue();

        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors).hasSize(2);

        Descriptor person = findMessage(descriptors, "Person");
        assertThat(person.getFile().getPackage()).isEqualTo("pipestream.it.v1");
        assertThat(person.getFields()).hasSize(3);
        FieldDescriptor name = person.findFieldByName("name");
        assertThat(name.getNumber()).isEqualTo(1);
        assertThat(name.getType()).isEqualTo(FieldDescriptor.Type.STRING);
        FieldDescriptor id = person.findFieldByName("id");
        assertThat(id.getNumber()).isEqualTo(2);
        assertThat(id.getType()).isEqualTo(FieldDescriptor.Type.INT32);
        FieldDescriptor emails = person.findFieldByName("emails");
        assertThat(emails.getNumber()).isEqualTo(3);
        assertThat(emails.isRepeated()).isTrue();

        Descriptor order = findMessage(descriptors, "Order");
        assertThat(order.findFieldByName("order_id").getType()).isEqualTo(FieldDescriptor.Type.STRING);
        assertThat(order.findFieldByName("total_cents").getType()).isEqualTo(FieldDescriptor.Type.INT64);
    }

    @Test
    void loadDescriptorResolvesSingleArtifactOnDemand() throws Exception {
        ApicurioDescriptorLoader loader = loaderFor(groupId);

        FileDescriptor descriptor = loader.loadDescriptor("person.proto");
        assertThat(descriptor).isNotNull();
        assertThat(descriptor.findMessageTypeByName("Person")).isNotNull();

        // Heuristic 2 in the loader: "group.artifactId" style names split on the last dot,
        // so a plain unknown name resolves to null rather than throwing.
        assertThat(loader.loadDescriptor("no-such-artifact")).isNull();
    }

    @Test
    void resolvesArtifactWithRegistryReference() throws Exception {
        ApicurioDescriptorLoader loader = loaderFor(refGroupId);

        // employee.proto imports common.proto via a first-class registry reference; the loader
        // resolves the reference so BOTH artifacts survive the bulk load.
        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors).hasSize(2);

        Descriptor employee = findMessage(descriptors, "Employee");
        assertThat(employee.getFile().getDependencies())
                .extracting(FileDescriptor::getName)
                .contains("common.proto");
        FieldDescriptor address = employee.findFieldByName("address");
        assertThat(address.getType()).isEqualTo(FieldDescriptor.Type.MESSAGE);
        assertThat(address.getMessageType().getFullName()).isEqualTo("pipestream.it.v1.Address");
        assertThat(address.getMessageType().findFieldByName("city").getType())
                .isEqualTo(FieldDescriptor.Type.STRING);
    }

    @Test
    void resolvesArtifactWithRegistryReferenceOnDemand() throws Exception {
        ApicurioDescriptorLoader loader = loaderFor(refGroupId);

        FileDescriptor employeeFile = loader.loadDescriptor("employee.proto");
        assertThat(employeeFile).isNotNull();
        Descriptor employee = employeeFile.findMessageTypeByName("Employee");
        assertThat(employee.findFieldByName("address").getMessageType().getFullName())
                .isEqualTo("pipestream.it.v1.Address");
    }

    @Test
    void resolvesTransitiveRegistryReferences() throws Exception {
        ApicurioDescriptorLoader loader = loaderFor(transitiveGroupId);

        // company.proto -> employee.proto -> common.proto
        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors).hasSize(3);

        Descriptor company = findMessage(descriptors, "Company");
        FieldDescriptor employees = company.findFieldByName("employees");
        assertThat(employees.isRepeated()).isTrue();
        Descriptor employee = employees.getMessageType();
        assertThat(employee.getFullName()).isEqualTo("pipestream.it.v1.Employee");
        assertThat(employee.findFieldByName("address").getMessageType().getFullName())
                .isEqualTo("pipestream.it.v1.Address");
    }

    @Test
    void resolvesDiamondRegistryReferences() throws Exception {
        ApicurioDescriptorLoader loader = loaderFor(diamondGroupId);

        // top.proto -> {left.proto, right.proto} -> base.proto
        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors).hasSize(4);

        Descriptor top = findMessage(descriptors, "Top");
        Descriptor left = top.findFieldByName("left").getMessageType();
        Descriptor right = top.findFieldByName("right").getMessageType();
        assertThat(left.findFieldByName("base").getMessageType().getFullName())
                .isEqualTo("pipestream.it.v1.Base");
        assertThat(right.findFieldByName("base").getMessageType().getFullName())
                .isEqualTo("pipestream.it.v1.Base");
    }

    @Test
    void artifactWithDanglingReferenceIsSkippedNotFailed() throws Exception {
        ApicurioDescriptorLoader loader = loaderFor(danglingGroupId);

        // dangling.proto references missing.proto, which was never registered. The bulk load
        // skips it with a warning naming the unresolved import instead of failing.
        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertThat(descriptors).isEmpty();

        // On-demand resolution degrades to null, not an exception.
        assertThat(loader.loadDescriptor("dangling.proto")).isNull();
    }

    private static Descriptor findMessage(List<FileDescriptor> descriptors, String messageName) {
        return descriptors.stream()
                .flatMap(fd -> fd.getMessageTypes().stream())
                .filter(message -> message.getName().equals(messageName))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "Message " + messageName + " not found in loaded descriptors"));
    }
}
