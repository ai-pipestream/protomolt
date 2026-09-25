package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.validate.ValidateProto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** A worker-only starter fixture; it reports only its own validated report artifact. */
final class FixtureAgentProvider implements AgentProvider {

    static final String OBJECTIVE_PREFIX = "Produce a coordination report";
    static final String CHECK = "fixture-report-valid";
    static final String TYPE = "ai.protomolt.proto.samples.starter.v1.CoordinationReport";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PACKET = "\nPacket:\n";
    private static final String MEDIA_TYPE = "application/x-protobuf";

    private final Path artifacts;
    private Map<String, DeliverableContract> contracts = Map.of();

    FixtureAgentProvider(Path workspace) {
        this.artifacts = workspace.resolve("artifacts");
    }

    @Override public String name() { return "fixture"; }

    @Override public String sessionId() { return "fixture-local"; }

    @Override public void deliverableContracts(Map<String, DeliverableContract> contracts) {
        this.contracts = Map.copyOf(contracts);
    }

    @Override public String prompt(String prompt) {
        int at = prompt.lastIndexOf(PACKET);
        if (at < 0) throw new AgentHostException("fixture received no event packet");
        ObjectNode packet;
        try {
            JsonNode parsed = JSON.readTree(prompt.substring(at + PACKET.length()));
            if (!(parsed instanceof ObjectNode object)) {
                throw new AgentHostException("fixture event packet is not an object");
            }
            packet = object;
        } catch (IOException invalid) {
            throw new AgentHostException("fixture could not read event packet", invalid);
        }
        if (!"worker".equals(packet.path("role").asText())) {
            throw new AgentHostException("fixture supports only worker event packets");
        }
        ObjectNode reply = JSON.createObjectNode();
        ArrayNode cursors = reply.putArray("handledEventCursors");
        ArrayNode commands = reply.putArray("commands");
        for (JsonNode event : packet.path("events")) {
            cursors.add(event.path("cursor").asLong());
            String taskId = event.path("taskId").asText();
            JsonNode frame = event.path("entry").path("coordinatorFrame");
            if (frame.has("offer")) {
                offer(commands, taskId, frame.path("offer"));
            } else if (frame.has("revisionRequested")) {
                JsonNode revision = frame.path("revisionRequested");
                candidate(commands, taskId, revision.path("attempt").asInt(),
                        revision.path("revision").asInt() + 1,
                        revision.path("feedback").asText(""));
            } else if (frame.path("taskMessage").isObject()) {
                message(commands, taskId, frame.path("taskMessage"));
            }
        }
        if (commands.isEmpty()) {
            command(commands, "host-ack", JSON.createObjectNode()
                    .put("reason", "fixture observed a coordinator update"));
        }
        return reply.toString();
    }

    private void offer(ArrayNode commands, String taskId, JsonNode offer) {
        int attempt = offer.path("attempt").asInt();
        JsonNode spec = offer.path("spec");
        DeliverableContract contract = contracts.get(taskId);
        String unsupported = unsupported(spec, contract);
        if (unsupported != null) {
            command(commands, "delegation-reject", JSON.createObjectNode()
                    .put("taskId", taskId).put("attempt", attempt)
                    .put("reason", unsupported).put("retryable", true));
            return;
        }
        command(commands, "delegation-accept", JSON.createObjectNode()
                .put("taskId", taskId).put("attempt", attempt));
        candidate(commands, taskId, attempt, 1, "");
    }

    private String unsupported(JsonNode spec, DeliverableContract contract) {
        if (!spec.path("objective").asText("").startsWith(OBJECTIVE_PREFIX)) {
            return "Fixture supports only objectives beginning '" + OBJECTIVE_PREFIX + "'";
        }
        JsonNode checks = spec.path("requiredChecks");
        if (!checks.isArray() || checks.size() != 1
                || !CHECK.equals(checks.get(0).path("name").asText())) {
            return "Fixture supports only the " + CHECK + " reported check";
        }
        if (contract == null || !TYPE.equals(contract.getTypeName())) {
            return "Fixture supports only the CoordinationReport deliverable contract";
        }
        try {
            Descriptor descriptor = DeliverableContracts.compile(contract).descriptor();
            if (!reportShape(descriptor)) {
                return "Fixture requires the reviewed CoordinationReport field shape";
            }
        } catch (IllegalArgumentException invalid) {
            return "Fixture cannot link the offered CoordinationReport contract";
        }
        return null;
    }

    private static boolean reportShape(Descriptor descriptor) {
        if (!TYPE.equals(descriptor.getFullName()) || descriptor.getFields().size() != 3) {
            return false;
        }
        FieldDescriptor headline = descriptor.findFieldByNumber(1);
        FieldDescriptor findings = descriptor.findFieldByNumber(2);
        FieldDescriptor count = descriptor.findFieldByNumber(3);
        if (!(headline != null && "headline".equals(headline.getName())
                && headline.getType() == FieldDescriptor.Type.STRING && !headline.isRepeated()
                && findings != null && "findings".equals(findings.getName())
                && findings.getType() == FieldDescriptor.Type.STRING && findings.isRepeated()
                && count != null && "finding_count".equals(count.getName())
                && count.getType() == FieldDescriptor.Type.INT32 && !count.isRepeated())) {
            return false;
        }
        var messageRule = descriptor.getOptions().getExtension(ValidateProto.message);
        var headlineRule = headline.getOptions().getExtension(ValidateProto.field);
        var findingsRule = findings.getOptions().getExtension(ValidateProto.field);
        var countRule = count.getOptions().getExtension(ValidateProto.field);
        return messageRule.getCelCount() == 1
                && "report-count-matches-findings".equals(messageRule.getCel(0).getId())
                && "this.finding_count == this.findings.size()".equals(
                        messageRule.getCel(0).getExpression())
                && headlineRule.getRequired() && headlineRule.getString().getMinLen() == 8
                && headlineRule.getString().getMaxLen() == 256
                && findingsRule.getRepeated().getMinItems() == 1
                && findingsRule.getRepeated().getMaxItems() == 8
                && findingsRule.getRepeated().getItems().getString().getMinLen() == 1
                && findingsRule.getRepeated().getItems().getString().getMaxLen() == 1024
                && countRule.getInt32().getGte() == 1
                && countRule.getInt32().getLte() == 8;
    }

    private void candidate(ArrayNode commands, String taskId, int attempt, int revision,
                           String feedback) {
        DeliverableContract contract = contracts.get(taskId);
        if (contract == null || !TYPE.equals(contract.getTypeName())) {
            throw new AgentHostException("fixture has no supported contract for task " + taskId);
        }
        var compiled = DeliverableContracts.compile(contract);
        if (!reportShape(compiled.descriptor())) {
            throw new AgentHostException("fixture report descriptor changed for task " + taskId);
        }
        List<String> findings = feedback.isBlank()
                ? List.of("Fixture worker created and validated this report artifact.")
                : List.of("Fixture worker created and validated this report artifact.",
                        "Reviewer requested revision: " + feedback.substring(0,
                                Math.min(feedback.length(), 900)));
        Descriptor descriptor = compiled.descriptor();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByNumber(1), "Fixture coordination report")
                .setField(descriptor.findFieldByNumber(3), findings.size());
        for (String finding : findings) {
            builder.addRepeatedField(descriptor.findFieldByNumber(2), finding);
        }
        DynamicMessage report = builder.build();
        if (!compiled.validator().validate(report).valid()) {
            throw new AgentHostException("fixture report failed offered runtime validation");
        }
        byte[] bytes = report.toByteArray();
        String digest = store(bytes);
        ObjectNode result;
        try {
            result = (ObjectNode) JSON.readTree(JsonFormat.printer().print(report));
        } catch (IOException e) {
            throw new AgentHostException("fixture could not render validated report", e);
        }
        result.put("@type", "type.googleapis.com/" + TYPE);
        ObjectNode artifact = JSON.createObjectNode().put("sha256", digest)
                .put("mediaType", MEDIA_TYPE).put("sizeBytes", bytes.length)
                .put("redacted", false);
        ObjectNode evidence = JSON.createObjectNode()
                .put("checkName", CHECK).put("verdict", "CHECK_VERDICT_PASSED")
                .put("ranAt", Instant.now().toString())
                .put("detail", "Fixture-only check: report passed the offered runtime validator"
                        + " and its bytes were stored; no external tests were run.");
        ObjectNode candidate = JSON.createObjectNode()
                .put("attempt", attempt).put("revision", revision)
                .put("summary", "Fixture report submitted for human review; no external"
                        + " checks were run.");
        candidate.putArray("evidence").add(evidence);
        candidate.putArray("artifacts").add(artifact);
        candidate.set("result", result);
        ObjectNode request = JSON.createObjectNode().put("taskId", taskId);
        request.set("candidate", candidate);
        command(commands, "delegation-candidate", request);
    }

    private void message(ArrayNode commands, String taskId, JsonNode message) {
        String kind = message.path("kind").asText();
        if ("TASK_MESSAGE_KIND_QUESTION".equals(kind)
                || "TASK_MESSAGE_KIND_GUIDANCE".equals(kind)) {
            boolean question = "TASK_MESSAGE_KIND_QUESTION".equals(kind);
            String text = question
                    ? "Fixture worker can produce only the supported coordination report."
                    : "Fixture worker recorded the guidance; a new report requires a"
                    + " revision request.";
            ObjectNode reply = JSON.createObjectNode().put("taskId", taskId)
                    .put("kind", question ? "TASK_MESSAGE_KIND_ANSWER"
                            : "TASK_MESSAGE_KIND_NOTE").put("text", text);
            if (question && !message.path("messageId").asText("").isBlank()) {
                reply.put("replyTo", message.path("messageId").asText());
            }
            command(commands, "delegation-message", reply);
        }
    }

    private String store(byte[] bytes) {
        String digest;
        try {
            digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
        try {
            Files.createDirectories(artifacts);
            Path target = artifacts.resolve(digest);
            if (Files.exists(target)) {
                if (!java.util.Arrays.equals(bytes, Files.readAllBytes(target))) {
                    throw new AgentHostException("fixture artifact digest collides with stored bytes");
                }
                return digest;
            }
            Path temporary = Files.createTempFile(artifacts, ".fixture-", ".tmp");
            try {
                Files.write(temporary, bytes);
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(temporary);
            }
            return digest;
        } catch (IOException e) {
            throw new AgentHostException("fixture could not persist report artifact", e);
        }
    }

    private static void command(ArrayNode commands, String tool, ObjectNode arguments) {
        ObjectNode command = JSON.createObjectNode().put("tool", tool);
        command.set("arguments", arguments);
        commands.add(command);
    }

    @Override public void close() { }
}
