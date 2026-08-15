package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import ai.pipestream.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.pipestream.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.pipestream.proto.inference.spi.ChunkObserver;
import ai.pipestream.proto.inference.spi.InferenceCatalog;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.InferenceException;
import ai.pipestream.proto.inference.spi.InferenceProvider;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStreamRequest;
import ai.pipestream.proto.inference.v1.ModelCapabilities;
import ai.pipestream.proto.inference.v1.ModelEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves the host catalog exposes structured workflow execution to MCP/action callers. */
class StructuredWorkflowCatalogTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String MODEL = "structured-model";
    private static final String PROTO = """
            syntax = "proto3";
            package shop.v1;
            message Order { string id = 1; int32 quantity = 2; }
            """;

    @Test
    void recordWorkflowRunExecutesAnInlineStructuredStepThroughTheHostCatalog(
            @TempDir Path dir) throws Exception {
        ScriptedProvider provider = new ScriptedProvider();
        InferenceCatalog models = new InferenceCatalog();
        InferenceEngines engines = new InferenceEngines(models, List.of(provider));
        engines.register(ModelEntry.newBuilder()
                .setId(MODEL)
                .setProvider(provider.id())
                .setEndpoint("in-process://scripted")
                .setCapabilities(ModelCapabilities.newBuilder().setStructuredOutput(true))
                .build());
        ActionContext context = ActionContext.create();
        ActionCatalog catalog = ProtoMoltCatalog.full(context, null, null, null, 0,
                engines, null, OutboundChannelPolicy.defaults(),
                new FileSystemArtifactRepository(dir.resolve("artifacts")),
                new FileSystemRunEvidenceRepository(dir.resolve("runs")), null);

        ObjectNode input = (ObjectNode) MAPPER.readTree("""
                {
                  "workflow": {
                    "name": "fill-order",
                    "schema": {
                      "sources": {"shop/v1/order.proto": %s},
                      "root": "shop/v1/order.proto"
                    },
                    "inputType": "shop.v1.Order",
                    "steps": [{
                      "name": "fill",
                      "structured": {
                        "targetType": "shop.v1.Order",
                        "model": "structured-model",
                        "maxAttempts": 1
                      }
                    }]
                  },
                  "input": {"id": "input"},
                  "runId": "catalog-structured"
                }
                """.formatted(MAPPER.writeValueAsString(PROTO)));

        ObjectNode result = catalog.execute("record-workflow-run", input);

        assertThat(result.path("ok").asBoolean()).isTrue();
        assertThat(result.path("evidence").path("steps").get(0)
                .path("structured").path("model").asText()).isEqualTo(MODEL);
        assertThat(provider.invocations.get()).isEqualTo(1);
    }

    private static final class ScriptedProvider implements InferenceProvider {
        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public String id() {
            return "scripted";
        }

        @Override
        public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
            invocations.incrementAndGet();
            return GenerateResponse.newBuilder()
                    .setText("{\"id\":\"generated\",\"quantity\":2}")
                    .setModel(model.getId())
                    .setProvider(id())
                    .build();
        }

        @Override
        public void generateStream(ModelEntry model, GenerateStreamRequest request,
                                   ChunkObserver observer) {
            throw new InferenceException("streaming is not used");
        }
    }
}
