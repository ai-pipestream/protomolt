package ai.protomolt.proto.grpc.service;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
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
                // A request the contract accepts: the catalog checks it before dispatch,
                // so an empty one would be refused before any thread is captured.
                Descriptor input = method.getInputType();
                FieldDescriptor sources = input.findFieldByName("sources");
                Descriptor entry = sources.getMessageType();
                DynamicMessage request = DynamicMessage.newBuilder(input)
                        .addRepeatedField(sources, DynamicMessage.newBuilder(entry)
                                .setField(entry.findFieldByName("key"), "a.proto")
                                .setField(entry.findFieldByName("value"),
                                        "syntax = \"proto3\"; message A {}")
                                .build())
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
        public Descriptor requestType() {
            return CatalogContract.request("CompileRequest");
        }

        @Override
        public Descriptor responseType() {
            return CatalogContract.response("CompileResponse");
        }

        @Override
        public Message execute(Message input, ActionContext context)
                throws ActionException {
            virtual.set(Thread.currentThread().isVirtual());
            return Reply.of(responseType())
                    .set("ok", true)
                    .set("descriptorSetBase64", "")
                    .build();
        }
    }
}
