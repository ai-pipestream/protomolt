package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The chain end to end against live in-process services: a two-step tokenize-then-embed
 * composition with gates, cross-step mappings, an output projection, fail-fast errors, and
 * the verbs' JSON surface (parse, verify, run).
 */
class ChainTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PROTO = """
            syntax = "proto3";
            package chain.test;
            message Text { string text = 1; bool skip_embed = 2; }
            message Tokens { repeated int64 ids = 1; }
            message Vector { repeated double values = 1; string model = 2; }
            message Embedding { string source_text = 1; repeated double vector = 2; }
            service Tokenizer { rpc Tokenize(Text) returns (Tokens); }
            service Embedder { rpc Embed(Tokens) returns (Vector); }
            """;

    private static FileDescriptor file;
    private static Server server;
    private static String serverName;
    private static final AtomicBoolean fail = new AtomicBoolean();

    private static ChainRunner runner;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("chain/test/chain.proto", PROTO, "test").build());
        file = compiled.descriptorFor("chain/test/chain.proto").orElseThrow();

        ServiceDescriptor tokenizer = file.findServiceByName("Tokenizer");
        ServiceDescriptor embedder = file.findServiceByName("Embedder");
        var tokenize = DynamicGrpcCalls.methodDescriptor(tokenizer.findMethodByName("Tokenize"));
        var embed = DynamicGrpcCalls.methodDescriptor(embedder.findMethodByName("Embed"));

        Descriptor tokens = file.findMessageTypeByName("Tokens");
        Descriptor vector = file.findMessageTypeByName("Vector");
        serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(tokenizer.getFullName())
                                .addMethod(tokenize).build())
                        .addMethod(tokenize, ServerCalls.asyncUnaryCall((request, out) -> {
                            String text = (String) request.getField(
                                    request.getDescriptorForType().findFieldByName("text"));
                            DynamicMessage.Builder response = DynamicMessage.newBuilder(tokens);
                            var ids = tokens.findFieldByName("ids");
                            for (char c : text.toCharArray()) {
                                response.addRepeatedField(ids, (long) c);
                            }
                            out.onNext(response.build());
                            out.onCompleted();
                        }))
                        .build())
                .addService(ServerServiceDefinition
                        .builder(io.grpc.ServiceDescriptor.newBuilder(embedder.getFullName())
                                .addMethod(embed).build())
                        .addMethod(embed, ServerCalls.asyncUnaryCall((request, out) -> {
                            if (fail.get()) {
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
                            response.setField(vector.findFieldByName("model"), "toy");
                            out.onNext(response.build());
                            out.onCompleted();
                        }))
                        .build())
                .build()
                .start();
        runner = new ChainRunner(step -> InProcessChannelBuilder.forName(serverName).build());
    }

    @AfterAll
    static void stop() {
        server.shutdownNow();
    }

    private static ChainDefinition definition(String when, ChainDefinition.Output output) {
        List<FileDescriptor> files = List.of(file);
        return new ChainDefinition("embed-text", files,
                file.findMessageTypeByName("Text"), 10_000,
                List.of(new ChainDefinition.Step("tokenize", "in-process", false,
                                ChainDefinition.resolveMethod(files, "chain.test.Tokenizer/Tokenize"),
                                null, List.of("text = input.text"), List.of(), false, 0),
                        new ChainDefinition.Step("embed", "in-process", false,
                                ChainDefinition.resolveMethod(files, "chain.test.Embedder/Embed"),
                                when, List.of("ids = tokenize.ids"), List.of(), false, 0)),
                output);
    }

    private static DynamicMessage text(String value, boolean skipEmbed) {
        Descriptor type = file.findMessageTypeByName("Text");
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("text"), value)
                .setField(type.findFieldByName("skip_embed"), skipEmbed)
                .build();
    }

    @Test
    void verifierPassesTheGoodChainAndCatchesScopeErrors() {
        assertThat(new ChainVerifier().verify(definition(null, null))).isEmpty();

        // 'embed' cannot be referenced before it runs, and gates must be boolean.
        List<FileDescriptor> files = List.of(file);
        ChainDefinition broken = new ChainDefinition("broken", files,
                file.findMessageTypeByName("Text"), 0,
                List.of(new ChainDefinition.Step("tokenize", "in-process", false,
                        ChainDefinition.resolveMethod(files, "chain.test.Tokenizer/Tokenize"),
                        "input.text", List.of("text = embed.model"), List.of(), false, 0)),
                null);
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(broken);
        assertThat(findings).hasSize(2);
        assertThat(findings.get(0).kind()).isEqualTo("rule");
        assertThat(findings.get(0).error()).contains("unknown source 'embed'");
        assertThat(findings.get(1).kind()).isEqualTo("when");
        assertThat(findings.get(1).error()).contains("must be a boolean");
    }

    @Test
    void chainComposesTwoServicesAndProjectsTheOutput() throws Exception {
        ChainDefinition.Output output = new ChainDefinition.Output(
                file.findMessageTypeByName("Embedding"),
                List.of("source_text = input.text", "vector = embed.values"),
                List.of());
        ChainRunner.Result result = runner.run(definition(null, output), text("hi", false));

        Descriptor embedding = file.findMessageTypeByName("Embedding");
        assertThat(result.output().getField(embedding.findFieldByName("source_text")))
                .isEqualTo("hi");
        @SuppressWarnings("unchecked")
        List<Object> vector = (List<Object>) result.output()
                .getField(embedding.findFieldByName("vector"));
        assertThat(vector).containsExactly(0.2, 1.0);
        assertThat(result.steps()).extracting(ChainRunner.StepOutcome::name)
                .containsExactly("tokenize", "embed");
    }

    @Test
    void falseGateSkipsAStepAndTheChainContinues() throws Exception {
        ChainRunner.Result result = runner.run(
                definition("!input.skip_embed", null), text("hi", true));
        // 'embed' skipped: the chain's output is the last executed response (Tokens).
        assertThat(result.output().getDescriptorForType().getName()).isEqualTo("Tokens");
        assertThat(result.steps()).extracting(ChainRunner.StepOutcome::skipped)
                .containsExactly(false, true);
    }

    @Test
    void skippedStepsBindDefaultsSoLaterReferencesStayWellDefined() throws Exception {
        // The output mapping references 'embed' even though the gate skips it. The
        // verifier accepts this, and the runner must honor the same contract: the
        // skipped step's value is its output type's default instance, not an error.
        ChainDefinition.Output output = new ChainDefinition.Output(
                file.findMessageTypeByName("Embedding"),
                List.of("source_text = input.text", "vector = embed.values"),
                List.of());
        ChainDefinition chain = definition("!input.skip_embed", output);
        assertThat(new ChainVerifier().verify(chain)).isEmpty();

        ChainRunner.Result result = runner.run(chain, text("hi", true));
        Descriptor embedding = file.findMessageTypeByName("Embedding");
        assertThat(result.output().getField(embedding.findFieldByName("source_text")))
                .isEqualTo("hi");
        @SuppressWarnings("unchecked")
        List<Object> vector = (List<Object>) result.output()
                .getField(embedding.findFieldByName("vector"));
        assertThat(vector).isEmpty();
        assertThat(result.steps()).extracting(ChainRunner.StepOutcome::skipped)
                .containsExactly(false, true);
    }

    @Test
    void stepFailuresAreFailFastWithTheStepName() {
        fail.set(true);
        try {
            org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                            runner.run(definition(null, null), text("hi", false)))
                    .isInstanceOf(ChainRunner.ChainExecutionException.class)
                    .hasMessageContaining("UNAVAILABLE")
                    .satisfies(e -> assertThat(
                            ((ChainRunner.ChainExecutionException) e).step())
                            .isEqualTo("embed"));
        } finally {
            fail.set(false);
        }
    }

    @Test
    void theVerbsParseVerifyAndRunTheJsonEnvelope() throws Exception {
        String descriptorSet;
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("chain/test/chain.proto", PROTO, "test").build());
        descriptorSet = Base64.getEncoder()
                .encodeToString(compiled.descriptorSet().toByteArray());
        ObjectNode input = (ObjectNode) MAPPER.readTree("""
                {"chain": {
                   "name": "embed-text",
                   "schema": {"descriptorSetBase64": "%s"},
                   "inputType": "chain.test.Text",
                   "steps": [
                     {"name": "tokenize", "target": "in-process",
                      "method": "chain.test.Tokenizer/Tokenize",
                      "rules": ["text = input.text"]},
                     {"name": "embed", "target": "in-process",
                      "method": "chain.test.Embedder/Embed",
                      "rules": ["ids = tokenize.ids"]}
                   ],
                   "output": {"type": "chain.test.Embedding",
                              "rules": ["source_text = input.text",
                                        "vector = embed.values"]}},
                 "input": {"text": "hey"}}
                """.formatted(descriptorSet));

        ActionContext context = ActionContext.create();
        ObjectNode checked = new CheckChainAction().execute(input, context);
        assertThat(checked.get("ok").asBoolean()).isTrue();

        ObjectNode run = new RunChainAction(runner).execute(input, context);
        assertThat(run.get("ok").asBoolean()).isTrue();
        assertThat(run.get("outputType").asText()).isEqualTo("chain.test.Embedding");
        assertThat(run.get("output").get("sourceText").asText()).isEqualTo("hey");
        assertThat(run.get("steps")).hasSize(2);

        // A broken rule comes back as a typed finding, not an error.
        ObjectNode broken = input.deepCopy();
        ((ObjectNode) broken.get("chain").get("steps").get(1))
                .putArray("rules").add("ids = tokenizer.ids");
        ObjectNode findings = new CheckChainAction().execute(broken, context);
        assertThat(findings.get("ok").asBoolean()).isFalse();
        assertThat(findings.get("findings").get(0).get("step").asText()).isEqualTo("embed");
        assertThat(findings.get("findings").get(0).get("error").asText())
                .contains("unknown source 'tokenizer'");
    }
}
