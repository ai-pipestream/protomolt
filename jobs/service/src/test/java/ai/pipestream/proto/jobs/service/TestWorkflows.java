package ai.pipestream.proto.jobs.service;

import ai.pipestream.proto.workflow.WorkflowRunner;
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
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCalls;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;

/**
 * The unit/IT workflow fixture: one runtime-compiled proto (a tokenize →
 * review → embed pipeline), served as dynamic in-process or localhost-TCP
 * gRPC services, plus builders for the workflow-definition JSON envelopes the
 * tests submit. The Embedder fails UNAVAILABLE when its request carries
 * {@code fail = true} (propagated from the workflow input through the
 * Tokenizer), which is how the retry/dead-letter paths are exercised.
 * ReviewDesk is declared but never served: the external step parks before
 * anything would call it.
 */
public final class TestWorkflows {

    /** The fixture proto. */
    public static final String PROTO = """
            syntax = "proto3";
            package jobs.test;
            message Text { string text = 1; bool fail = 2; }
            message Tokens { repeated int64 ids = 1; bool fail = 2; }
            message Review { string notes = 1; int32 score = 2; }
            message Vector { repeated double values = 1; }
            message Embedding { string source_text = 1; repeated double vector = 2;
                                string notes = 3; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Embedder { rpc Embed(Tokens) returns (Vector); }
            service ReviewDesk { rpc Review(Text) returns (Review); }
            """;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final FileDescriptor file;
    private final String descriptorSetBase64;
    private final ServerServiceDefinition tokenizer;
    private final ServerServiceDefinition embedder;
    private Server server;

    /** Compile the fixture proto; cheap enough per test class. */
    public TestWorkflows() {
        CompiledProtos compiled;
        try {
            compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                    .add("jobs/test/jobs.proto", PROTO, "test").build());
        } catch (Exception e) {
            throw new IllegalStateException("the fixture proto must compile", e);
        }
        file = compiled.descriptorFor("jobs/test/jobs.proto").orElseThrow();
        descriptorSetBase64 = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        tokenizer = tokenizerService();
        embedder = embedderService();
    }

    /** The fixture file's descriptor. */
    public FileDescriptor file() {
        return file;
    }

    /** The serialized descriptor set, base64 — the envelope's schema arm. */
    public String descriptorSetBase64() {
        return descriptorSetBase64;
    }

    /**
     * Start both services on an in-process server.
     *
     * @return the in-process name (the step target is irrelevant — the
     *         runner's channel factory ignores it)
     */
    public String startInProcess() {
        String name = InProcessServerBuilder.generateName();
        try {
            server = InProcessServerBuilder.forName(name)
                    .addService(tokenizer)
                    .addService(embedder)
                    .build()
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return name;
    }

    /**
     * Start both services on a localhost TCP server (the Kafka IT: in-process
     * channels cannot cross a real target string).
     *
     * @return the {@code localhost:port} target
     */
    public String startTcp() {
        try {
            server = io.grpc.ServerBuilder.forPort(0)
                    .addService(tokenizer)
                    .addService(embedder)
                    .build()
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return "localhost:" + server.getPort();
    }

    /** Stop the server. */
    public void stop() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    /**
     * A runner whose channels reach the in-process server, regardless of the
     * step target.
     *
     * @param inProcessName the in-process server name
     * @return the runner
     */
    public WorkflowRunner inProcessRunner(String inProcessName) {
        return new WorkflowRunner(step -> InProcessChannelBuilder.forName(inProcessName).build());
    }

    /** The production runner (channels from the step target), for the TCP IT. */
    public WorkflowRunner tcpRunner() {
        return new WorkflowRunner();
    }

    /**
     * The two-step workflow envelope: tokenize → embed, output Embedding.
     *
     * @param target the step target
     * @param gate an optional {@code when} gate for the embed step, or null
     * @return the workflow-definition JSON
     */
    public ObjectNode twoStepWorkflow(String target, String gate) {
        ObjectNode workflow = envelope(target);
        ArrayNode steps = (ArrayNode) workflow.get("steps");
        if (gate != null) {
            ((ObjectNode) steps.get(1)).put("when", gate);
        }
        ((ObjectNode) workflow.get("output"))
                .putArray("rules").add("source_text = input.text").add("vector = embed.values");
        return workflow;
    }

    /**
     * The three-step workflow envelope: tokenize → review (external) → embed,
     * output Embedding including the review's notes.
     *
     * @param target the step target
     * @return the workflow-definition JSON
     */
    public ObjectNode threeStepWorkflow(String target) {
        ObjectNode workflow = envelope(target);
        ArrayNode steps = (ArrayNode) workflow.get("steps");
        ObjectNode review = steps.insertObject(1);
        review.put("name", "review");
        review.put("target", target);
        review.put("method", "jobs.test.ReviewDesk/Review");
        review.putArray("rules").add("text = input.text");
        review.put("completion", "external");
        ((ObjectNode) workflow.get("output"))
                .putArray("rules").add("source_text = input.text")
                .add("vector = embed.values").add("notes = review.notes");
        return workflow;
    }

    /**
     * A workflow whose embed rule references a scope that does not exist — it
     * parses but does not verify.
     *
     * @param target the step target
     * @return the broken workflow-definition JSON
     */
    public ObjectNode brokenWorkflow(String target) {
        ObjectNode workflow = envelope(target);
        ArrayNode rules = (ArrayNode) ((ObjectNode) workflow.get("steps").get(1)).get("rules");
        rules.removeAll();
        rules.add("ids = typo.ids");
        return workflow;
    }

    /** tokenize → embed without an output mapping (the base envelope). */
    private ObjectNode envelope(String target) {
        ObjectNode workflow = MAPPER.createObjectNode();
        workflow.put("name", "embed-text");
        workflow.putObject("schema").put("descriptorSetBase64", descriptorSetBase64);
        workflow.put("inputType", "jobs.test.Text");
        ArrayNode steps = workflow.putArray("steps");
        ObjectNode tokenize = steps.addObject();
        tokenize.put("name", "tokenize");
        tokenize.put("target", target);
        tokenize.put("method", "jobs.test.Tokenizer/Tokenize");
        tokenize.putArray("rules").add("text = input.text").add("fail = input.fail");
        ObjectNode embed = steps.addObject();
        embed.put("name", "embed");
        embed.put("target", target);
        embed.put("method", "jobs.test.Embedder/Embed");
        embed.putArray("rules").add("ids = tokenize.ids").add("fail = tokenize.fail");
        workflow.putObject("output").put("type", "jobs.test.Embedding");
        return workflow;
    }

    /** A Tokens checkpoint response for {@code text}, as proto3 JSON. */
    public ObjectNode tokensJson(ai.pipestream.proto.actions.ActionContext context,
            String text, boolean fail) {
        Descriptor tokens = file.findMessageTypeByName("Tokens");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(tokens);
        var ids = tokens.findFieldByName("ids");
        for (char c : text.toCharArray()) {
            builder.addRepeatedField(ids, (long) c);
        }
        builder.setField(tokens.findFieldByName("fail"), fail);
        try {
            return (ObjectNode) MAPPER.readTree(
                    context.transcoder().toJson(builder.build()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private ServerServiceDefinition tokenizerService() {
        ServiceDescriptor tokenizer = file.findServiceByName("Tokenizer");
        var tokenize = DynamicGrpcCalls.methodDescriptor(tokenizer.findMethodByName("Tokenize"));
        Descriptor tokens = file.findMessageTypeByName("Tokens");
        return ServerServiceDefinition
                .builder(io.grpc.ServiceDescriptor.newBuilder(tokenizer.getFullName())
                        .addMethod(tokenize).build())
                .addMethod(tokenize, ServerCalls.asyncUnaryCall((request, out) -> {
                    String text = (String) request.getField(
                            request.getDescriptorForType().findFieldByName("text"));
                    boolean fail = (boolean) request.getField(
                            request.getDescriptorForType().findFieldByName("fail"));
                    DynamicMessage.Builder response = DynamicMessage.newBuilder(tokens);
                    var ids = tokens.findFieldByName("ids");
                    for (char c : text.toCharArray()) {
                        response.addRepeatedField(ids, (long) c);
                    }
                    response.setField(tokens.findFieldByName("fail"), fail);
                    out.onNext(response.build());
                    out.onCompleted();
                }))
                .build();
    }

    private ServerServiceDefinition embedderService() {
        ServiceDescriptor embedder = file.findServiceByName("Embedder");
        var embed = DynamicGrpcCalls.methodDescriptor(embedder.findMethodByName("Embed"));
        Descriptor vector = file.findMessageTypeByName("Vector");
        return ServerServiceDefinition
                .builder(io.grpc.ServiceDescriptor.newBuilder(embedder.getFullName())
                        .addMethod(embed).build())
                .addMethod(embed, ServerCalls.asyncUnaryCall((request, out) -> {
                    boolean fail = (boolean) request.getField(
                            request.getDescriptorForType().findFieldByName("fail"));
                    if (fail) {
                        out.onError(Status.UNAVAILABLE.withDescription("model loading")
                                .asRuntimeException());
                        return;
                    }
                    int count = request.getRepeatedFieldCount(
                            request.getDescriptorForType().findFieldByName("ids"));
                    DynamicMessage.Builder response = DynamicMessage.newBuilder(vector);
                    var values = vector.findFieldByName("values");
                    response.addRepeatedField(values, count / 10.0);
                    response.addRepeatedField(values, 1.0);
                    out.onNext(response.build());
                    out.onCompleted();
                }))
                .build();
    }
}
