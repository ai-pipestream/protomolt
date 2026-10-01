package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Executes caller-pinned fixtures independently; worker check claims are not inputs. */
public final class WorkflowAuthoringFixtures {
    private WorkflowAuthoringFixtures() { }

    /** A named expectation from the trusted caller's policy. */
    public record Fixture(String name, ArtifactReference input, ArtifactReference expectedOutput) {
        public Fixture {
            WorkflowValidation.validateName(name, "fixture.name");
            Objects.requireNonNull(input, "input");
            Objects.requireNonNull(expectedOutput, "expectedOutput");
        }
    }

    /** Evidence of this invocation, not a worker-reported PASS. */
    public record Observation(String name, ArtifactReference input, ArtifactReference output) { }

    /** Fully checked fixture snapshot; only {@link #admit} can construct one. */
    public static final class Admitted {
        private final CompiledWorkflow workflow;
        private final List<Fixture> fixtures;
        private final List<DynamicMessage> inputs;
        private final List<DynamicMessage> expected;
        private final Descriptor outputType;

        private Admitted(CompiledWorkflow workflow, List<Fixture> fixtures,
                List<DynamicMessage> inputs, List<DynamicMessage> expected, Descriptor outputType) {
            this.workflow = workflow;
            this.fixtures = List.copyOf(fixtures);
            this.inputs = List.copyOf(inputs);
            this.expected = List.copyOf(expected);
            this.outputType = outputType;
        }
    }

    /**
     * Matches candidate fixtures against caller policy, admits all fixture bytes
     * before any call, then executes the already-preflighted workflow. Failure
     * propagates; no partial set of observations can be mistaken for acceptance.
     */
    public static List<Observation> execute(WorkflowAuthoringPreflight.Result admitted,
            List<Fixture> candidate, List<Fixture> callerPolicy,
            ArtifactRepository artifacts, WorkflowRunner runner)
            throws IOException, WorkflowRunner.WorkflowExecutionException {
        return execute(admit(admitted, candidate, callerPolicy, artifacts), artifacts, runner);
    }

    /** Resolves and validates every policy fixture before any external call. */
    public static Admitted admit(WorkflowAuthoringPreflight.Result admitted,
            List<Fixture> candidate, List<Fixture> callerPolicy, ArtifactRepository artifacts)
            throws IOException {
        Objects.requireNonNull(admitted, "admitted workflow");
        Objects.requireNonNull(artifacts, "artifacts");
        candidate = List.copyOf(candidate);
        callerPolicy = List.copyOf(callerPolicy);
        if (callerPolicy.isEmpty() || callerPolicy.size() > 32 || !candidate.equals(callerPolicy)) {
            throw new IllegalArgumentException("fixtures differ from caller policy or exceed bounds");
        }
        var names = new HashSet<String>();
        var inputs = new ArrayList<DynamicMessage>();
        var expected = new ArrayList<DynamicMessage>();
        var workflow = admitted.workflow();
        Descriptor outputType = workflow.output() != null ? workflow.output().type()
                : workflow.steps().getLast().method().getOutputType();
        long bytes = 0;
        for (Fixture fixture : callerPolicy) {
            if (!names.add(fixture.name())) throw new IllegalArgumentException("duplicate fixture name");
            for (ArtifactReference ref : List.of(fixture.input(), fixture.expectedOutput())) {
                WorkflowValidation.validate(ref);
                if (Long.compareUnsigned(ref.getSizeBytes(), 4 * 1024 * 1024) > 0) {
                    throw new IllegalArgumentException("fixture artifact exceeds 4 MiB");
                }
                bytes += ref.getSizeBytes();
                if (bytes > 64L * 1024 * 1024) throw new IllegalArgumentException("fixtures exceed 64 MiB");
            }
            inputs.add(load(fixture.input(), workflow.inputType(), artifacts));
            expected.add(load(fixture.expectedOutput(), outputType, artifacts));
        }
        return new Admitted(workflow, callerPolicy, inputs, expected, outputType);
    }

    /** Executes only the workflow and parsed bytes captured by admission. */
    public static List<Observation> execute(Admitted admitted,
            ArtifactRepository artifacts, WorkflowRunner runner)
            throws IOException, WorkflowRunner.WorkflowExecutionException {
        Objects.requireNonNull(admitted, "admitted fixtures");
        Objects.requireNonNull(artifacts, "artifacts");
        Objects.requireNonNull(runner, "runner");
        var observer = new WorkflowRunner.ExecutionObserver() {
            @Override public void stepStarted(CompiledWorkflow.Step step, DynamicMessage request,
                    Instant startedAt) { validate(request); }
            @Override public void stepCompleted(CompiledWorkflow.Step step, DynamicMessage request,
                    DynamicMessage response, boolean skipped, Instant start, Instant end) {
                if (!skipped) validate(response);
            }
        };
        var observations = new ArrayList<Observation>();
        for (int i = 0; i < admitted.fixtures.size(); i++) {
            Fixture fixture = admitted.fixtures.get(i);
            Message output = runner.run(admitted.workflow, admitted.inputs.get(i), observer).output();
            validate(output);
            if (!output.equals(admitted.expected.get(i))) {
                throw new IllegalArgumentException("fixture '" + fixture.name() + "' output differs from caller expectation");
            }
            ArtifactReference stored = artifacts.save(output.toByteArray(), "application/x-protobuf", false);
            // Confirm the repository actually retained the observed output.
            if (!load(stored, admitted.outputType, artifacts).equals(output)) {
                throw new IOException("stored fixture observation differs from executed output");
            }
            observations.add(new Observation(fixture.name(), fixture.input(), stored));
        }
        return List.copyOf(observations);
    }

    private static DynamicMessage load(ArtifactReference ref, Descriptor type,
            ArtifactRepository artifacts) throws IOException {
        WorkflowValidation.validate(ref);
        if (Long.compareUnsigned(ref.getSizeBytes(), 4 * 1024 * 1024) > 0) {
            throw new IllegalArgumentException("fixture artifact exceeds 4 MiB");
        }
        if (!"application/x-protobuf".equals(ref.getMediaType()) || ref.getRedacted()) {
            throw new IllegalArgumentException("acceptance fixtures must be unredacted protobuf artifacts");
        }
        var stored = artifacts.find(ref.getSha256()).orElseThrow(() ->
                new IllegalArgumentException("missing fixture artifact"));
        if (!stored.reference().equals(ref)) throw new IllegalArgumentException("fixture reference differs from storage");
        DynamicMessage message = DynamicMessage.parseFrom(type, stored.content());
        validate(message);
        return message;
    }

    private static void validate(Message message) {
        if (!message.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("fixture message contains unknown fields");
        }
        for (var field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                if (field.getKey().isRepeated()) {
                    for (Object nested : (List<?>) field.getValue()) validate((Message) nested);
                } else validate((Message) field.getValue());
            }
        }
        var validation = ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
        if (!validation.valid()) throw new IllegalArgumentException("fixture message violates contract: " + validation.violations());
    }
}
