package ai.pipestream.proto.jobs.service;

import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * The validate.v1 fixture: one runtime-compiled proto whose {@code Tokens.tag}
 * and {@code Review.notes} carry declared validation rules (required, min_len
 * 3), used to exercise the worker's VALIDATION verdict (a step response that
 * fails its declared rules fails the job, no retry) and complete-step's
 * verdict (an invalid supplied response fails the job as a validation
 * rejection). The validate.proto source rides the protobuf-validation jar as
 * a classpath resource, the same pattern the sources module's own options
 * tests use.
 */
public final class ValidatingWorkflows {

    private static final String VALIDATE_PROTO =
            "ai/pipestream/proto/validate/v1/validate.proto";

    /** The fixture proto. */
    public static final String PROTO = """
            syntax = "proto3";
            package jobs.guard;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Text { string text = 1; }
            message Tokens {
              string tag = 1 [(ai.pipestream.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3}
              }];
            }
            message Review {
              string notes = 1 [(ai.pipestream.proto.validate.v1.field) = {
                required: true
                string: {min_len: 3}
              }];
            }
            message Out { string tag = 1; string notes = 2; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service ReviewDesk { rpc Review(Text) returns (Review); }
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final FileDescriptor file;
    private final String descriptorSetBase64;
    private Server server;

    /** Compile the fixture proto with the validate.v1 import; cheap per class. */
    public ValidatingWorkflows() {
        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                    .add(VALIDATE_PROTO, resource(VALIDATE_PROTO), "test")
                    .add("jobs/guard/guard.proto", PROTO, "test")
                    .build());
        } catch (Exception e) {
            throw new IllegalStateException("the validating fixture proto must compile", e);
        }
        file = compiled.descriptorFor("jobs/guard/guard.proto").orElseThrow();
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
    }

    /**
     * The one-step validating workflow: tokenize (validate: true) → Out.tag.
     *
     * @param target the step target
     * @return the workflow-definition JSON
     */
    public ObjectNode validatingTokenizeWorkflow(String target) {
        ObjectNode workflow = envelope("guarded-tokenize", target);
        ArrayNode steps = workflow.putArray("steps");
        ObjectNode tokenize = steps.addObject();
        tokenize.put("name", "tokenize");
        tokenize.put("target", target);
        tokenize.put("method", "jobs.guard.Tokenizer/Tokenize");
        tokenize.putArray("rules").add("text = input.text");
        tokenize.put("validate", true);
        ((ObjectNode) workflow.get("output")).putArray("rules").add("tag = tokenize.tag");
        return workflow;
    }

    /**
     * The one-step external validating workflow: review (external, validate:
     * true) → Out.notes. The job parks before anything would call the desk.
     *
     * @param target the step target
     * @return the workflow-definition JSON
     */
    public ObjectNode externalReviewWorkflow(String target) {
        ObjectNode workflow = envelope("guarded-review", target);
        ArrayNode steps = workflow.putArray("steps");
        ObjectNode review = steps.addObject();
        review.put("name", "review");
        review.put("target", target);
        review.put("method", "jobs.guard.ReviewDesk/Review");
        review.putArray("rules").add("text = input.text");
        review.put("completion", "external");
        review.put("validate", true);
        ((ObjectNode) workflow.get("output")).putArray("rules").add("notes = review.notes");
        return workflow;
    }

    /**
     * Start the Tokenizer on an in-process server, always answering
     * {@code tag} — "no" trips the declared min_len rule.
     *
     * @param tag the tag every Tokenize call answers
     * @return the in-process name
     */
    public String startTokenizer(String tag) {
        ServiceDescriptor tokenizer = file.findServiceByName("Tokenizer");
        var tokenize = DynamicGrpcCalls.methodDescriptor(tokenizer.findMethodByName("Tokenize"));
        Descriptor tokens = file.findMessageTypeByName("Tokens");
        ServerServiceDefinition service = ServerServiceDefinition
                .builder(io.grpc.ServiceDescriptor.newBuilder(tokenizer.getFullName())
                        .addMethod(tokenize).build())
                .addMethod(tokenize, ServerCalls.asyncUnaryCall((request, out) -> {
                    DynamicMessage.Builder response = DynamicMessage.newBuilder(tokens);
                    response.setField(tokens.findFieldByName("tag"), tag);
                    out.onNext(response.build());
                    out.onCompleted();
                }))
                .build();
        String name = InProcessServerBuilder.generateName();
        try {
            server = InProcessServerBuilder.forName(name).addService(service).build().start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return name;
    }

    /** Stop the server, when one was started. */
    public void stop() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    private ObjectNode envelope(String name, String target) {
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", name);
        workflow.putObject("schema").put("descriptorSetBase64", descriptorSetBase64);
        workflow.put("inputType", "jobs.guard.Text");
        workflow.putObject("output").put("type", "jobs.guard.Out");
        return workflow;
    }

    private static String resource(String path) {
        try (InputStream in = ValidatingWorkflows.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
