package ai.protomolt.proto.acp.agent;

import ai.protomolt.proto.acp.PromptContext;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.service.CatalogBridge;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** ACP commands forwarded to the running coordinator; no local action fallback. */
final class RemoteCatalogLineRunner implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final ManagedChannel channel;
    private final Metadata headers;
    private final Map<String, MethodDescriptor> methods = new LinkedHashMap<>();

    static RemoteCatalogLineRunner connect(String target, boolean tls, String token) {
        if (token == null || token.isBlank() || token.length() > 4096
                || token.chars().anyMatch(c -> c < 33 || c > 126)) {
            throw new IllegalArgumentException("remote ACP requires a valid PROTOMOLT_API_TOKEN");
        }
        return new RemoteCatalogLineRunner(ChannelFactory.standard().open(target, tls), token);
    }

    RemoteCatalogLineRunner(ManagedChannel channel, String token) {
        this.channel = channel;
        this.headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        for (MethodDescriptor method : ProtoMoltServiceSchema.service().getMethods()) {
            methods.put(CatalogBridge.actionName(method), method);
        }
    }

    void run(String command, PromptContext context) {
        String line = command.trim();
        if (line.isEmpty()) return;
        if (line.equals("list") || line.equals("help")) {
            context.sendMessage("Remote coordinator RPC commands (availability depends on server configuration):\n"
                    + String.join("\n", methods.keySet()));
            return;
        }
        int separator = line.indexOf(' ');
        String verb = separator < 0 ? line : line.substring(0, separator);
        MethodDescriptor method = methods.get(verb);
        if (method == null) {
            context.sendMessage("unknown-action: no remote RPC for '" + verb + "'; use service-invoke for registered services");
            return;
        }
        String input = separator < 0 ? "{}" : line.substring(separator + 1).trim();
        DynamicMessage request;
        try {
            if (input.length() > 1024 * 1024 || !JSON.readTree(input).isObject()) {
                context.sendMessage("invalid-input: expected a JSON object of at most 1 MiB");
                return;
            }
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(method.getInputType());
            JsonFormat.parser().merge(input, builder);
            request = builder.build();
        } catch (Exception invalid) {
            context.sendMessage("invalid-input: JSON does not match the remote RPC request");
            return;
        }
        try {
            for (DynamicMessage response : DynamicGrpcCalls.call(channel, method, request,
                    CallOptions.DEFAULT.withDeadlineAfter(360, TimeUnit.SECONDS), headers, 1)) {
                context.sendMessage(JsonFormat.printer().print(response));
            }
        } catch (StatusRuntimeException failure) {
            String code = failure.getTrailers() == null ? null
                    : failure.getTrailers().get(CatalogBridge.ERROR_CODE_KEY);
            // Do not echo upstream descriptions, which may contain input or credentials.
            context.sendMessage((code == null ? failure.getStatus().getCode().name() : code)
                    + ": remote coordinator refused or could not complete the command");
        } catch (Exception failure) {
            context.sendMessage("remote-error: could not decode the coordinator response");
        }
    }

    @Override public void close() { channel.shutdownNow(); }
}
