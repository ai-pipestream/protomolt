package ai.protomolt.proto.acp.agent;

import ai.protomolt.proto.acp.PromptContext;
import ai.protomolt.proto.delegation.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.ObservedEvent;
import ai.protomolt.proto.delegation.v1.ReadTranscriptRequest;
import ai.protomolt.proto.delegation.v1.ReadTranscriptResponse;
import ai.protomolt.proto.delegation.v1.WatchEventsResponse;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.service.CatalogBridge;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** ACP commands forwarded to the running coordinator; no local action fallback. */
final class RemoteCatalogLineRunner implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int HISTORY_PAGE_SIZE = 500;
    private static final int MAX_HISTORY_PAGES = 64;
    private final ManagedChannel channel;
    private final Metadata headers;
    private final Map<String, MethodDescriptor> methods = new LinkedHashMap<>();
    private final MethodDescriptor readTranscript;

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
        var delegation = ai.protomolt.proto.delegation.v1.DelegationActions.getDescriptor()
                .findServiceByName("DelegationService");
        for (MethodDescriptor method : delegation.getMethods()) {
            methods.put("delegation/" + method.getName(), method);
        }
        readTranscript = delegation.findMethodByName("ReadTranscript");
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
            JsonNode document = input.length() > 1024 * 1024 ? null : JSON.readTree(input);
            if (document == null || !document.isObject()) {
                context.sendMessage("invalid-input: expected a JSON object of at most 1 MiB");
                return;
            }
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(method.getInputType());
            JsonFormat.TypeRegistry registry = method.getService().getName().equals("DelegationService")
                    && method.getName().equals("SubmitCandidate")
                    ? candidateRegistry(document)
                    : JsonFormat.TypeRegistry.getEmptyTypeRegistry();
            JsonFormat.parser().usingTypeRegistry(registry).merge(input, builder);
            request = builder.build();
        } catch (StatusRuntimeException failure) {
            context.sendMessage(redactedStatus(failure));
            return;
        } catch (Exception invalid) {
            context.sendMessage("invalid-input: JSON does not match the remote RPC request");
            return;
        }
        try {
            for (DynamicMessage response : DynamicGrpcCalls.call(channel, method, request,
                    CallOptions.DEFAULT.withDeadlineAfter(360, TimeUnit.SECONDS), headers, 1)) {
                context.sendMessage(render(method, response));
            }
        } catch (StatusRuntimeException failure) {
            context.sendMessage(redactedStatus(failure));
        } catch (Exception failure) {
            context.sendMessage("remote-error: could not decode the coordinator response");
        }
    }

    private static String redactedStatus(StatusRuntimeException failure) {
        String code = failure.getTrailers() == null ? null
                : failure.getTrailers().get(CatalogBridge.ERROR_CODE_KEY);
        // Do not echo upstream descriptions, which may contain input or credentials.
        return (code == null ? failure.getStatus().getCode().name() : code)
                + ": remote coordinator refused or could not complete the command";
    }

    private JsonFormat.TypeRegistry candidateRegistry(com.fasterxml.jackson.databind.JsonNode input)
            throws Exception {
        if (!input.path("candidate").has("result")) {
            return JsonFormat.TypeRegistry.getEmptyTypeRegistry();
        }
        String taskId = input.path("taskId").asText(input.path("task_id").asText());
        int attempt = input.path("candidate").path("attempt").asInt();
        if (taskId.isBlank() || attempt < 1) {
            throw new IllegalArgumentException("candidate task and attempt are required");
        }
        DeliverableContract contract = contractFor(taskId, attempt,
                new LinkedHashMap<>(), new HashSet<>(), historyDeadline());
        if (contract == null) {
            throw new IllegalArgumentException("candidate has no declared deliverable contract");
        }
        return DeliverableContracts.typeRegistry(contract);
    }

    private String render(MethodDescriptor method, DynamicMessage response) throws Exception {
        if (!method.getService().getName().equals("DelegationService")) {
            return JsonFormat.printer().print(response);
        }
        if (!method.getName().equals("ReadTranscript")
                && !method.getName().equals("WatchEvents")) {
            return JsonFormat.printer().print(response);
        }
        List<ObservedEvent> events;
        if (method.getName().equals("ReadTranscript")) {
            events = ReadTranscriptResponse.parseFrom(response.toByteString()).getEventsList();
        } else {
            events = WatchEventsResponse.parseFrom(response.toByteString()).getEventsList();
        }
        DynamicMessage empty = DynamicMessage.newBuilder(method.getOutputType())
                .mergeFrom(response).clearField(method.getOutputType().findFieldByName("events"))
                .build();
        ObjectNode rendered = (ObjectNode) JSON.readTree(JsonFormat.printer().print(empty));
        ArrayNode output = rendered.putArray("events");
        Map<String, Map<Integer, DeliverableContract>> history = new LinkedHashMap<>();
        Set<String> loaded = new HashSet<>();
        long deadline = historyDeadline();
        for (ObservedEvent event : events) {
            if (event.getEntry().hasCoordinatorFrame()
                    && event.getEntry().getCoordinatorFrame().hasOffer()
                    && event.getEntry().getCoordinatorFrame().getOffer().getSpec().hasContract()) {
                var offer = event.getEntry().getCoordinatorFrame().getOffer();
                history.computeIfAbsent(event.getTaskId(), ignored -> new LinkedHashMap<>())
                        .put(offer.getAttempt(), offer.getSpec().getContract());
            }
        }
        for (ObservedEvent event : events) {
            JsonFormat.TypeRegistry registry = JsonFormat.TypeRegistry.getEmptyTypeRegistry();
            if (event.getEntry().hasWorkerFrame()
                    && event.getEntry().getWorkerFrame().hasCompletion()
                    && event.getEntry().getWorkerFrame().getCompletion().hasResult()) {
                int attempt = event.getEntry().getWorkerFrame().getCompletion().getAttempt();
                DeliverableContract contract = contractFor(event.getTaskId(), attempt,
                        history, loaded, deadline);
                if (contract == null) {
                    throw new IllegalStateException("completion has no historical contract");
                }
                registry = DeliverableContracts.typeRegistry(contract);
            }
            output.add(JSON.readTree(JsonFormat.printer().usingTypeRegistry(registry)
                    .print(event)));
        }
        return rendered.toString();
    }

    /** Read only this task's recorded offers from the beginning; never infer from a later offer. */
    private DeliverableContract contractFor(String taskId, int attempt,
            Map<String, Map<Integer, DeliverableContract>> cache, Set<String> loaded,
            long deadline)
            throws Exception {
        Map<Integer, DeliverableContract> known = cache.get(taskId);
        if (known != null && known.containsKey(attempt)) {
            return known.get(attempt);
        }
        if (!loaded.contains(taskId)) {
            Map<Integer, DeliverableContract> byAttempt = new LinkedHashMap<>();
            long cursor = 0;
            boolean finished = false;
            for (int page = 0; page < MAX_HISTORY_PAGES; page++) {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                if (remainingMs <= 0) {
                    throw new IllegalStateException("transcript history deadline exceeded");
                }
                var request = ReadTranscriptRequest.newBuilder().setTaskId(taskId)
                        .setAfterCursor(cursor).setMaxEntries(HISTORY_PAGE_SIZE).build();
                DynamicMessage dynamic = DynamicMessage.parseFrom(readTranscript.getInputType(),
                        request.toByteString());
                List<DynamicMessage> replies = DynamicGrpcCalls.call(channel, readTranscript,
                        dynamic, CallOptions.DEFAULT.withDeadlineAfter(
                                Math.min(15_000, remainingMs), TimeUnit.MILLISECONDS),
                        headers, 1);
                ReadTranscriptResponse response = ReadTranscriptResponse.parseFrom(
                        replies.getFirst().toByteString());
                for (ObservedEvent event : response.getEventsList()) {
                    if (event.getEntry().hasCoordinatorFrame()
                            && event.getEntry().getCoordinatorFrame().hasOffer()) {
                        var offer = event.getEntry().getCoordinatorFrame().getOffer();
                        if (offer.getSpec().hasContract()) {
                            byAttempt.put(offer.getAttempt(), offer.getSpec().getContract());
                        }
                    }
                }
                if (!response.getTruncated()) {
                    finished = true;
                    break;
                }
                if (response.getCursor() <= cursor) {
                    throw new IllegalStateException("transcript cursor did not advance");
                }
                cursor = response.getCursor();
            }
            if (!finished) {
                throw new IllegalStateException("transcript history exceeds ACP page bound");
            }
            cache.computeIfAbsent(taskId, ignored -> new LinkedHashMap<>()).putAll(byAttempt);
            loaded.add(taskId);
        }
        return cache.get(taskId).get(attempt);
    }

    private static long historyDeadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    }

    @Override public void close() { channel.shutdownNow(); }
}
