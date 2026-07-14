package ai.pipestream.proto.registry.server;

import ai.pipestream.proto.registry.GitSchemaRegistryStore;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaPublisher;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaRegistryLoader;
import ai.pipestream.proto.sources.ProtoSourceSet;
import ai.pipestream.proto.sources.publish.PublishOptions;
import ai.pipestream.proto.sources.publish.PublishResult;
import ai.pipestream.proto.sources.publish.PublishResult.Action;
import ai.pipestream.proto.sources.publish.PublishResult.FileOutcome;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The acceptance test: this repo's own {@link ConfluentSchemaPublisher} and
 * {@link ConfluentSchemaRegistryLoader} round-trip through {@link SchemaRegistryServer} backed
 * by a {@link GitSchemaRegistryStore} — publish a three-file reference-linked source set,
 * republish idempotently, load and resolve the root type with its imports, then mutate one
 * file and republish. Whatever those two clients need is the protocol contract.
 */
class SchemaRegistryServerRoundTripTest {

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

    @TempDir
    Path tempDir;

    @Test
    void publisherAndLoaderRoundTripThroughTheGitBackedServer() throws Exception {
        try (GitSchemaRegistryStore store = GitSchemaRegistryStore.builder()
                .repositoryDir(tempDir.resolve("registry-repo"))
                .build();
                SchemaRegistryServer server = new SchemaRegistryServer(
                        SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0), store)) {
            URI base = URI.create("http://127.0.0.1:" + server.start());

            // Publish a three-file reference-linked set: everything is CREATED.
            try (ConfluentSchemaPublisher publisher = new ConfluentSchemaPublisher(base)) {
                PublishResult first = publisher.publish(sourceSet(APP_PROTO), PublishOptions.defaults());
                first.throwIfFailed();
                assertThat(first.outcomes())
                        .extracting(FileOutcome::path, FileOutcome::action)
                        .containsExactly(
                                org.assertj.core.groups.Tuple.tuple(CORE_PATH, Action.CREATED),
                                org.assertj.core.groups.Tuple.tuple(USER_PATH, Action.CREATED),
                                org.assertj.core.groups.Tuple.tuple(APP_PATH, Action.CREATED));
                assertThat(first.outcomes())
                        .allSatisfy(outcome -> assertThat(outcome.detail()).isEqualTo("version 1"));

                // Republishing identical content writes nothing: everything is UNCHANGED.
                PublishResult second = publisher.publish(sourceSet(APP_PROTO), PublishOptions.defaults());
                second.throwIfFailed();
                assertThat(second.outcomes()).extracting(FileOutcome::action)
                        .containsOnly(Action.UNCHANGED);
            }
            assertThat(store.subjects()).containsExactly(APP_PATH, CORE_PATH, USER_PATH);

            // Load everything back and resolve the root type across its references.
            try (ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(base)) {
                List<FileDescriptor> descriptors = loader.loadDescriptors();
                assertThat(descriptors).hasSize(3);
                assertThat(loader.lastSkippedSubjectCount()).isZero();

                FileDescriptor appFile = loader.loadDescriptor("app.v1.App");
                assertThat(appFile).isNotNull();
                Descriptor app = appFile.findMessageTypeByName("App");
                FieldDescriptor owner = app.findFieldByName("owner");
                assertThat(owner.getMessageType().getFullName()).isEqualTo("common.v1.User");
                assertThat(owner.getMessageType().findFieldByName("core").getMessageType().getFullName())
                        .isEqualTo("common.v1.Core");
                assertThat(app.findFieldByName("created").getMessageType().getFullName())
                        .isEqualTo("google.protobuf.Timestamp");
            }

            // Mutate one file: only it is UPDATED, its imports stay UNCHANGED.
            String changedApp = APP_PROTO.replace("created = 2;", "created = 2;\n  string note = 3;");
            try (ConfluentSchemaPublisher publisher = new ConfluentSchemaPublisher(base)) {
                PublishResult third = publisher.publish(sourceSet(changedApp), PublishOptions.defaults());
                third.throwIfFailed();
                assertThat(third.outcomes())
                        .extracting(FileOutcome::path, FileOutcome::action)
                        .containsExactly(
                                org.assertj.core.groups.Tuple.tuple(CORE_PATH, Action.UNCHANGED),
                                org.assertj.core.groups.Tuple.tuple(USER_PATH, Action.UNCHANGED),
                                org.assertj.core.groups.Tuple.tuple(APP_PATH, Action.UPDATED));
                assertThat(outcomeFor(third, APP_PATH).detail()).isEqualTo("version 2");
            }
            assertThat(store.versions(APP_PATH)).containsExactly(1, 2);
            assertThat(store.versions(CORE_PATH)).containsExactly(1);

            // A fresh loader sees the mutated schema.
            try (ConfluentSchemaRegistryLoader loader = new ConfluentSchemaRegistryLoader(base)) {
                FileDescriptor appFile = loader.loadDescriptor("app.v1.App");
                assertThat(appFile.findMessageTypeByName("App").findFieldByName("note")).isNotNull();
            }
        }
    }

    private static ProtoSourceSet sourceSet(String appProto) {
        // Inserted in non-topological order (root first): the publisher reorders imports-first.
        return ProtoSourceSet.builder()
                .add(APP_PATH, appProto, "test")
                .add(USER_PATH, USER_PROTO, "test")
                .add(CORE_PATH, CORE_PROTO, "test")
                .build();
    }

    private static FileOutcome outcomeFor(PublishResult result, String path) {
        return result.outcomes().stream()
                .filter(outcome -> outcome.path().equals(path))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No outcome for " + path));
    }
}
