package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.StepStatus;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The recorder removes {@code pii} and {@code secret} fields before anything persists: the
 * run input, every step request and response, and the evidence itself. This is the
 * plain-build counterpart of the redaction checks in the live structured-inference
 * acceptance suite, which needs a real model endpoint.
 */
class WorkflowRunRecorderRedactionTest {

    private static final String METADATA = "ai/protomolt/proto/meta/v1/metadata.proto";

    private static final String PROTO = """
            syntax = "proto3";
            package workflow.redaction.test;
            import "ai/protomolt/proto/meta/v1/metadata.proto";
            message Request {
              string doc_id = 1 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "public"}];
              string notes = 2 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "pii"}];
              string api_key = 3 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "secret"}];
            }
            message Result {
              string doc_id = 1 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "public"}];
              string notes = 2 [(ai.protomolt.proto.meta.v1.field) = {sensitivity: "pii"}];
            }
            service Lookup { rpc Fetch(Request) returns (Result); }
            """;

    private static final String NOTES = "sentinel-notes-never-persisted";
    private static final String KEY = "sentinel-key-never-persisted";
    private static final String RESULT_NOTES = "sentinel-result-notes-never-persisted";

    private static FileDescriptor file;
    private static Descriptor request;
    private static Descriptor result;
    private static Server server;
    private static String serverName;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(METADATA, resource(METADATA), "test")
                .add("workflow/redaction/test/redaction.proto", PROTO, "test").build());
        file = compiled.descriptorFor("workflow/redaction/test/redaction.proto").orElseThrow();
        request = file.findMessageTypeByName("Request");
        result = file.findMessageTypeByName("Result");
        ServiceDescriptor lookup = file.findServiceByName("Lookup");
        var fetch = DynamicGrpcCalls.methodDescriptor(lookup.findMethodByName("Fetch"));
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(lookup.getFullName())
                                .addMethod(fetch).build())
                        .addMethod(fetch, ServerCalls.asyncUnaryCall((in, out) -> {
                            DynamicMessage message = (DynamicMessage) in;
                            // The live call receives the unredacted request.
                            assertThat(message.getField(request.findFieldByName("notes")))
                                    .isEqualTo(NOTES);
                            out.onNext(DynamicMessage.newBuilder(result)
                                    .setField(result.findFieldByName("doc_id"),
                                            message.getField(request.findFieldByName("doc_id")))
                                    .setField(result.findFieldByName("notes"), RESULT_NOTES)
                                    .build());
                            out.onCompleted();
                        }))
                        .build())
                .build()
                .start();
    }

    @AfterAll
    static void stop() {
        server.shutdownNow();
    }

    private static String resource(String name) {
        try (InputStream in = WorkflowRunRecorderRedactionTest.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void sensitiveFieldsNeverReachPersistedArtifactsOrEvidence(@TempDir Path dir) throws Exception {
        Path artifactDir = dir.resolve("a");
        Path runDir = dir.resolve("r");
        ArtifactRepository artifacts = new FileSystemArtifactRepository(artifactDir);
        CompiledWorkflow definition = new CompiledWorkflow("redacted-lookup", List.of(file),
                request, 15_000,
                List.of(CompiledWorkflow.Step.grpc("fetch", "in-process", false,
                        CompiledWorkflow.resolveMethod(List.of(file),
                                "workflow.redaction.test.Lookup/Fetch"),
                        null, List.of("doc_id = input.doc_id", "notes = input.notes",
                                "api_key = input.api_key"),
                        List.of(), false, 0, "")),
                null);
        DynamicMessage input = DynamicMessage.newBuilder(request)
                .setField(request.findFieldByName("doc_id"), "24-cv-00117")
                .setField(request.findFieldByName("notes"), NOTES)
                .setField(request.findFieldByName("api_key"), KEY)
                .build();

        RunEvidence evidence = new WorkflowRunRecorder(
                new WorkflowRunner(step -> InProcessChannelBuilder.forName(serverName).build()),
                artifacts, new FileSystemRunEvidenceRepository(runDir))
                .record("run-redaction-1", null, definition, input);

        assertThat(evidence.getSteps(0).getStatus()).isEqualTo(StepStatus.STEP_STATUS_SUCCEEDED);
        DynamicMessage storedInput = DynamicMessage.parseFrom(request,
                artifacts.find(evidence.getInputArtifact().getSha256()).orElseThrow().content());
        assertThat(storedInput.getField(request.findFieldByName("doc_id"))).isEqualTo("24-cv-00117");
        assertThat(storedInput.hasField(request.findFieldByName("notes"))).isFalse();
        assertThat(storedInput.hasField(request.findFieldByName("api_key"))).isFalse();
        DynamicMessage storedRequest = DynamicMessage.parseFrom(request,
                artifacts.find(evidence.getSteps(0).getRequestArtifact().getSha256())
                        .orElseThrow().content());
        assertThat(storedRequest.hasField(request.findFieldByName("notes"))).isFalse();
        assertThat(storedRequest.hasField(request.findFieldByName("api_key"))).isFalse();
        DynamicMessage storedResponse = DynamicMessage.parseFrom(result,
                artifacts.find(evidence.getSteps(0).getResponseArtifact().getSha256())
                        .orElseThrow().content());
        assertThat(storedResponse.getField(result.findFieldByName("doc_id"))).isEqualTo("24-cv-00117");
        assertThat(storedResponse.hasField(result.findFieldByName("notes"))).isFalse();

        // No persisted byte anywhere, artifact or evidence, carries a sentinel.
        assertThat(evidence.toString()).doesNotContain(NOTES, KEY, RESULT_NOTES);
        for (Path root : List.of(artifactDir, runDir)) {
            try (Stream<Path> files = Files.walk(root)) {
                for (Path persisted : files.filter(Files::isRegularFile).toList()) {
                    String content = new String(Files.readAllBytes(persisted), StandardCharsets.ISO_8859_1);
                    assertThat(content).as(persisted.toString()).doesNotContain(NOTES, KEY, RESULT_NOTES);
                }
            }
        }
    }
}
