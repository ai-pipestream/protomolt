package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Promotion retries preserve the immutable envelope selected by the first writer. */
class PromoteWorkflowActionTest {
    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String PROTO = """
            syntax = "proto3";
            package promote.test;
            message Input { string name = 1; string alternate = 2; }
            message Result { string text = 1; }
            service Echo { rpc Run(Input) returns (Result); }
            """;
    private static FileDescriptor file;
    private static Descriptor inputType;
    private static CompiledWorkflow workflow;
    private static CompiledWorkflow changedWorkflow;

    @BeforeAll static void compile() throws Exception {
        CompiledProtos protos = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("promote/test.proto", PROTO, "test").build());
        file = protos.descriptorFor("promote/test.proto").orElseThrow();
        inputType = file.findMessageTypeByName("Input");
        workflow = definition("stable", "name = input.name");
        changedWorkflow = definition("stable", "name = input.alternate");
    }

    private static CompiledWorkflow definition(String name, String rule) {
        List<FileDescriptor> files = List.of(file);
        return new CompiledWorkflow(name, files, inputType, 10_000,
                List.of(CompiledWorkflow.Step.grpc("echo", "localhost:9090", false,
                        CompiledWorkflow.resolveMethod(files, "promote.test.Echo/Run"), null,
                        List.of(rule), List.of(), false, 0, "")), null);
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = PromoteWorkflowActionTest.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) throw new IllegalStateException(path + " not on the test classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static PromoteWorkflowAction action(InMemoryVersions versions, long timestamp) {
        return new PromoteWorkflowAction(versions,
                Clock.fixed(Instant.ofEpochSecond(timestamp), ZoneOffset.UTC));
    }

    private static VersionedWorkflow promote(PromoteWorkflowAction action,
                                             CompiledWorkflow content, String version)
            throws Exception {
        Workflow durable = WorkflowCompiler.compile(content);
        Descriptor requestType = action.requestType();
        var workflowField = requestType.findFieldByName("workflow");
        DynamicMessage durableMessage = DynamicMessage.parseFrom(
                workflowField.getMessageType(), durable.toByteArray());
        Message request = DynamicMessage.newBuilder(requestType)
                .setField(workflowField, durableMessage)
                .setField(requestType.findFieldByName("version"), version)
                .build();
        Message response = action.execute(request, ActionContext.create());
        Object returned = response.getField(
                response.getDescriptorForType().findFieldByName("versioned_workflow"));
        assertThat(response.getDescriptorForType().findFieldByName("versioned_workflow")
                .getMessageType().getFullName()).isEqualTo(Struct.getDescriptor().getFullName());
        Message dynamicStruct = (Message) returned;
        assertThat(dynamicStruct.getDescriptorForType().getFullName())
                .isEqualTo(Struct.getDescriptor().getFullName());
        Struct struct = Struct.parseFrom(dynamicStruct.toByteArray());
        VersionedWorkflow.Builder promoted = VersionedWorkflow.newBuilder();
        JsonFormat.parser().merge(JsonFormat.printer().print(struct), promoted);
        return promoted.build();
    }

    @Test void identicalRetryWithANewerClockReturnsTheOriginalEnvelope() throws Exception {
        InMemoryVersions versions = new InMemoryVersions();

        VersionedWorkflow original = promote(action(versions, 100), workflow, "v1");
        VersionedWorkflow retried = promote(action(versions, 200), workflow, "v1");

        assertThat(original.getCreatedAt().getSeconds()).isEqualTo(100);
        assertThat(retried).isEqualTo(original);
        assertThat(versions.entries).containsOnlyKeys("stable/v1");
        assertThat(versions.entries.get("stable/v1")).isEqualTo(original);
    }

    @Test void changedContentAtTheSameIdentityConflictsWithoutMutation() throws Exception {
        InMemoryVersions versions = new InMemoryVersions();
        VersionedWorkflow original = promote(action(versions, 100), workflow, "v1");

        assertThatThrownBy(() -> promote(action(versions, 200), changedWorkflow, "v1"))
                .isInstanceOf(ActionException.class);

        assertThat(versions.entries).containsOnlyKeys("stable/v1");
        assertThat(versions.entries.get("stable/v1")).isEqualTo(original);
    }

    @Test void identicalRaceWinnerWithDifferentTimestampIsReturned() throws Exception {
        InMemoryVersions versions = new InMemoryVersions();
        versions.raceWith(versioned(workflow, "v1", 150));

        VersionedWorkflow promoted = promote(action(versions, 200), workflow, "v1");

        assertThat(promoted.getCreatedAt().getSeconds()).isEqualTo(150);
        assertThat(promoted).isEqualTo(versions.entries.get("stable/v1"));
    }

    @Test void differentRaceWinnerFailsAndRemainsUnchanged() throws Exception {
        InMemoryVersions versions = new InMemoryVersions();
        VersionedWorkflow winner = versioned(changedWorkflow, "v1", 150);
        versions.raceWith(winner);

        assertThatThrownBy(() -> promote(action(versions, 200), workflow, "v1"))
                .isInstanceOf(ActionException.class);

        assertThat(versions.entries).containsOnlyKeys("stable/v1");
        assertThat(versions.entries.get("stable/v1")).isEqualTo(winner);
    }

    @Test void aReadFailureWithoutAVerifiedMatchCannotReportSuccess() {
        InMemoryVersions versions = new InMemoryVersions();
        versions.findFailure = new IOException("read unavailable");

        assertThatThrownBy(() -> promote(action(versions, 100), workflow, "v1"))
                .isInstanceOf(ActionException.class);
        assertThat(versions.entries).isEmpty();
    }

    @Test void aSaveFailureWithoutAPersistedMatchCannotReportSuccess() {
        InMemoryVersions versions = new InMemoryVersions();
        versions.saveFailure = new IOException("write unavailable");

        assertThatThrownBy(() -> promote(action(versions, 100), workflow, "v1"))
                .isInstanceOf(ActionException.class);
        assertThat(versions.entries).isEmpty();
    }

    private static VersionedWorkflow versioned(CompiledWorkflow content, String version,
                                                long timestamp) {
        Workflow durable = WorkflowCompiler.compile(content);
        return VersionedWorkflow.newBuilder().setWorkflow(durable).setVersion(version)
                .setWorkflowFingerprint(ai.protomolt.proto.grpc.workflow.WorkflowValidation
                        .fingerprint(durable))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(timestamp)).build();
    }

    /** In-memory immutable repository: identity collisions compare the complete bytes. */
    private static final class InMemoryVersions implements WorkflowVersionRepository {
        private final Map<String, VersionedWorkflow> entries = new HashMap<>();
        private VersionedWorkflow raceWinner;
        private IOException findFailure;
        private IOException saveFailure;

        void raceWith(VersionedWorkflow winner) { raceWinner = winner; }

        @Override public Optional<VersionedWorkflow> find(String name, String version)
                throws IOException {
            if (findFailure != null) throw findFailure;
            return Optional.ofNullable(entries.get(name + "/" + version));
        }

        @Override public List<VersionedWorkflow> versions(String name) {
            return entries.values().stream()
                    .filter(value -> value.getWorkflow().getName().equals(name)).toList();
        }

        @Override public void save(VersionedWorkflow value) throws IOException {
            if (saveFailure != null) throw saveFailure;
            String key = value.getWorkflow().getName() + "/" + value.getVersion();
            if (raceWinner != null && !entries.containsKey(key)) {
                entries.put(key, raceWinner);
                raceWinner = null;
            }
            VersionedWorkflow existing = entries.get(key);
            if (existing == null) {
                entries.put(key, value);
            } else if (!Arrays.equals(existing.toByteArray(), value.toByteArray())) {
                throw new IOException("immutable version already exists with different bytes");
            }
        }
    }
}
