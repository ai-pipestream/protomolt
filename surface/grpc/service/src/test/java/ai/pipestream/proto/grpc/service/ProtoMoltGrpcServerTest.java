package ai.pipestream.proto.grpc.service;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.invoke.DynamicGrpcCalls;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoMoltGrpcServerTest {

    @Test
    void actionsRunOnVirtualThreads() throws Exception {
        AtomicBoolean virtual = new AtomicBoolean();
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .replace(new ThreadCapturingCompileAction(virtual));

        try (ProtoMoltGrpcServer server = ProtoMoltGrpcServer.start(0, catalog)) {
            var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.port())
                    .usePlaintext()
                    .build();
            try {
                MethodDescriptor method = ProtoMoltServiceSchema.service()
                        .findMethodByName("Compile");
                DynamicMessage request = DynamicMessage.newBuilder(method.getInputType())
                        .build();

                var responses = DynamicGrpcCalls.call(channel,
                        method, request,
                        CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS),
                        new Metadata(), 1);

                assertThat(responses).hasSize(1);
                assertThat(virtual.get()).isTrue();
            } finally {
                channel.shutdownNow();
                assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private record ThreadCapturingCompileAction(AtomicBoolean virtual) implements ProtoAction {

        @Override
        public String name() {
            return "compile";
        }

        @Override
        public String description() {
            return "Captures the server dispatch thread for testing.";
        }

        @Override
        public ObjectNode inputSchema() {
            return JsonNodeFactory.instance.objectNode();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            virtual.set(Thread.currentThread().isVirtual());
            ObjectNode response = context.objectMapper().createObjectNode();
            response.put("ok", true);
            response.putArray("files");
            response.put("descriptorSetBase64", "");
            return response;
        }
    }
}
