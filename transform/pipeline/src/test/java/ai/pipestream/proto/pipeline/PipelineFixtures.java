package ai.pipestream.proto.pipeline;

import ai.pipestream.proto.grpc.recipe.v1.ServiceDependency;
import ai.pipestream.proto.pipeline.v1.EdgeCardinality;
import ai.pipestream.proto.pipeline.v1.GrpcCallStep;
import ai.pipestream.proto.pipeline.v1.MethodShape;
import ai.pipestream.proto.pipeline.v1.Pipeline;
import ai.pipestream.proto.pipeline.v1.PipelineStep;
import ai.pipestream.proto.pipeline.v1.StructuredStep;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * One compiled schema and the builders for shape-correct pipelines over it, shared by the
 * checker, compiler, and validation tests. The schema covers all four gRPC streaming
 * shapes, a projection-annotated grounding type, and a validated summary type, so every
 * acceptance and rejection test works against real descriptors. No server, no network.
 */
final class PipelineFixtures {

    static final String VALIDATE = "ai/pipestream/proto/validate/v1/validate.proto";
    static final String PROJECTION =
            "ai/pipestream/proto/projection/v1/projection.proto";

    static final String PROTO = """
            syntax = "proto3";
            package pipeline.test;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            import "ai/pipestream/proto/projection/v1/projection.proto";
            message Ticket { string title = 1; }
            message Batch { repeated Ticket items = 1; }
            message TicketBox { repeated Ticket tickets = 1; }
            message LookupResult {
              string doc_id = 1;
              string title = 2;
              string internal_notes = 3;
            }
            message Results { repeated LookupResult results = 1; }
            message Summary {
              string headline = 1 [(ai.pipestream.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3, max_len: 200}
              }];
            }
            // The consumer-visible grounding: internal_notes is never projected.
            message DocGrounding {
              option (ai.pipestream.proto.projection.v1.sources) = {
                source: "pipeline.test.LookupResult"
              };
              string doc_id = 1 [(ai.pipestream.proto.projection.v1.from) = {
                paths: {path: "doc_id"}
              }];
              string title = 2 [(ai.pipestream.proto.projection.v1.from) = {
                paths: {path: "title"}
              }];
            }
            service Lookup { rpc Fetch(Ticket) returns (LookupResult); }
            service Search { rpc Stream(Ticket) returns (stream LookupResult); }
            service Ingest { rpc Upload(stream Ticket) returns (Batch); }
            service Relay { rpc Converse(stream Ticket) returns (stream Ticket); }
            service Worker { rpc Process(Ticket) returns (Ticket); }
            """;

    static final String TICKET = "pipeline.test.Ticket";
    static final String BATCH = "pipeline.test.Batch";
    static final String TICKET_BOX = "pipeline.test.TicketBox";
    static final String LOOKUP_RESULT = "pipeline.test.LookupResult";
    static final String RESULTS = "pipeline.test.Results";
    static final String SUMMARY = "pipeline.test.Summary";
    static final String GROUNDING = "pipeline.test.DocGrounding";
    static final String FETCH = "pipeline.test.Lookup/Fetch";
    static final String STREAM = "pipeline.test.Search/Stream";
    static final String UPLOAD = "pipeline.test.Ingest/Upload";
    static final String CONVERSE = "pipeline.test.Relay/Converse";
    static final String PROCESS = "pipeline.test.Worker/Process";

    private static volatile FileDescriptor file;

    private PipelineFixtures() {
    }

    /** The compiled fixture file, built once per test JVM. */
    static FileDescriptor file() {
        FileDescriptor current = file;
        if (current == null) {
            synchronized (PipelineFixtures.class) {
                if (file == null) {
                    try {
                        CompiledProtos compiled = new ProtoSourceCompiler().compile(
                                ProtoSourceSet.builder()
                                        .add(VALIDATE, resource(VALIDATE), "test")
                                        .add(PROJECTION, resource(PROJECTION), "test")
                                        .add("pipeline/test/pipeline.proto", PROTO, "test")
                                        .build());
                        file = compiled.descriptorFor("pipeline/test/pipeline.proto")
                                .orElseThrow();
                    } catch (Exception e) {
                        throw new IllegalStateException("fixture schema does not compile",
                                e);
                    }
                    current = file;
                }
            }
        }
        return current;
    }

    /** The descriptor set every fixture pipeline binds to. */
    static List<FileDescriptor> files() {
        return List.of(file());
    }

    /** The canonical fingerprint of the fixture descriptor set. */
    static String fingerprint() {
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(file().toProto()).build();
        return ai.pipestream.proto.grpc.profile.ServiceProfileValidation
                .sha256(set.toByteArray());
    }

    static Descriptor type(String fullName) {
        Descriptor descriptor = file().findMessageTypeByName(
                fullName.substring(fullName.lastIndexOf('.') + 1));
        if (descriptor == null) {
            throw new IllegalArgumentException("fixture type missing: " + fullName);
        }
        return descriptor;
    }

    /** A dependency in the recipe compiler's convention: alias and profile are the FQN. */
    static ServiceDependency dependency(String service) {
        return ServiceDependency.newBuilder()
                .setAlias(service)
                .setServiceProfile(service)
                .setEndpoint("local")
                .setDescriptorFingerprint(fingerprint())
                .build();
    }

    /** A pipeline skeleton with the contract invariants already satisfied. */
    static Pipeline.Builder base(String name, String inputType) {
        return Pipeline.newBuilder()
                .setName(name)
                .setInputType(inputType)
                .setDescriptorFingerprint(fingerprint())
                .setDeadline(Duration.newBuilder().setSeconds(30).build());
    }

    /** A gRPC step builder with shape, cardinalities, and completion filled in. */
    static PipelineStep.Builder grpcStep(String name, String service, String method,
                                         MethodShape shape,
                                         EdgeCardinality edgeCardinality,
                                         EdgeCardinality outputCardinality,
                                         ai.pipestream.proto.grpc.recipe.v1.TypedEdge
                                                 edge) {
        GrpcCallStep.Builder call = GrpcCallStep.newBuilder()
                .setMethod(method)
                .setMethodShape(shape)
                .setEdge(edge)
                .setEdgeCardinality(edgeCardinality)
                .setOutputCardinality(outputCardinality)
                .setCompletion(ai.pipestream.proto.grpc.recipe.v1.StepCompletion
                        .STEP_COMPLETION_LIVE);
        return PipelineStep.newBuilder()
                .setName(name)
                .setDependency(service)
                .setGrpcCall(call.build());
    }

    /** A minimal typed edge: declared sources, a produced type, and optional rules. */
    static ai.pipestream.proto.grpc.recipe.v1.TypedEdge edge(String produceType,
                                                             List<String> sources,
                                                             String... rules) {
        return ai.pipestream.proto.grpc.recipe.v1.TypedEdge.newBuilder()
                .addAllSources(sources)
                .setProduceType(produceType)
                .addAllRules(List.of(rules))
                .build();
    }

    /** A valid structured step grounded in a projected upstream result. */
    static PipelineStep structuredStep(String name, String dependency, String model,
                                       ai.pipestream.proto.grpc.recipe.v1.TypedEdge edge) {
        StructuredStep.Builder structured = StructuredStep.newBuilder()
                .setSpec(ai.pipestream.proto.grpc.recipe.v1.StructuredGenerationSpec
                        .newBuilder()
                        .setTargetType(SUMMARY)
                        .setModel(model)
                        .setMaxAttempts(1)
                        .build());
        if (edge != null) {
            structured.setEdge(edge);
        }
        return PipelineStep.newBuilder()
                .setName(name)
                .setDependency(dependency)
                .setStructured(structured.build())
                .build();
    }

    static String resource(String name) {
        try (InputStream in = PipelineFixtures.class.getClassLoader()
                .getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException(name + " not on the test classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
