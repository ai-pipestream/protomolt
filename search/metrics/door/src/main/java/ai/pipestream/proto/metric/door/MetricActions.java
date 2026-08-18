package ai.pipestream.proto.metric.door;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.metric.DescribeMappingResponse;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.List;
import java.util.Map;

/**
 * The metric door's three catalog verbs. Registered on an action catalog
 * they are MCP tools with no translation layer, which is the moment an
 * agent surface can answer an aggregate question: {@code describe-mapping}
 * tells it what is queryable, {@code query-metrics} runs the proto3-JSON
 * {@code QueryMetricsRequest} it writes, and {@code rebuild-rollup}
 * replaces a declared lake rollup with a fresh complete answer. Refusals
 * surface the SPI's stable code with the legal set in details, so a
 * tool-using model can correct itself without parsing prose.
 */
public final class MetricActions {

    private MetricActions() {
    }

    /** The verbs over the same served subjects, without a rollup sink. */
    public static List<ProtoAction> over(Map<String, ServedMetricSubject> subjects) {
        return over(subjects, null, null);
    }

    /** The verbs over the same served subjects, without a resolver. */
    public static List<ProtoAction> over(
            Map<String, ServedMetricSubject> subjects,
            ai.pipestream.proto.metric.spi.RollupSink rollups) {
        return over(subjects, rollups, null);
    }

    /**
     * The verbs over the same served subjects.
     *
     * @param subjects the served subjects, keyed by name
     * @param rollups where rebuilt rollups land; {@code null} makes the
     *        rebuild verb refuse with {@code missing-sink}, exactly like
     *        the RPC on a sinkless mount
     * @param resolver resolves subjects beyond the static set (rollup
     *        tables), or {@code null} for none
     * @return the catalog verbs
     */
    public static List<ProtoAction> over(
            Map<String, ServedMetricSubject> subjects,
            ai.pipestream.proto.metric.spi.RollupSink rollups,
            ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver) {
        Map<String, ServedMetricSubject> served = Map.copyOf(subjects);
        return List.of(new DescribeMappingAction(served, resolver),
                new QueryMetricsAction(served, resolver),
                new RebuildRollupAction(served, rollups, resolver));
    }

    private static ServedMetricSubject subject(
            Map<String, ServedMetricSubject> subjects,
            ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver,
            String name, ObjectMapper mapper) throws ActionException {
        try {
            return Subjects.find(subjects, resolver, name);
        } catch (MetricRefusal refusal) {
            throw refusal(refusal, mapper);
        }
    }

    private static ActionException refusal(MetricRefusal refusal, ObjectMapper mapper) {
        ObjectNode details = mapper.createObjectNode();
        refusal.legal().forEach(details.withArray("legal")::add);
        return new ActionException(refusal.code(), refusal.getMessage(), details);
    }

    private static ObjectNode toJson(com.google.protobuf.Message message, ObjectMapper mapper)
            throws ActionException {
        try {
            return (ObjectNode) mapper.readTree(JsonFormat.printer().print(message));
        } catch (InvalidProtocolBufferException | com.fasterxml.jackson.core.JacksonException e) {
            throw new ActionException("render-failed",
                    "cannot render the response as JSON: " + e.getMessage());
        }
    }

    /** One subject's queryable surface: members, roles, backends. */
    static final class DescribeMappingAction implements ProtoAction {

        private final Map<String, ServedMetricSubject> subjects;
        private final ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver;

        DescribeMappingAction(Map<String, ServedMetricSubject> subjects,
                ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver) {
            this.subjects = subjects;
            this.resolver = resolver;
        }

        @Override
        public String name() {
            return "describe-mapping";
        }

        @Override
        public String description() {
            return "Describe one metric mapping subject: its members with roles, aggregates, "
                    + "descriptions and sensitivity, and the backends this mount can run.";
        }

        @Override
        public ObjectNode inputSchema() {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode schema = mapper.createObjectNode();
            schema.put("type", "object");
            schema.put("additionalProperties", false);
            ObjectNode properties = schema.putObject("properties");
            properties.putObject("mappingSubject")
                    .put("type", "string")
                    .put("description", "The mapping subject to describe.");
            schema.putArray("required").add("mappingSubject");
            return schema;
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            JsonNode name = input.get("mappingSubject");
            if (name == null || !name.isTextual() || name.asText().isBlank()) {
                throw new ActionException("invalid-input", "mappingSubject is required");
            }
            ServedMetricSubject subject = subject(subjects, resolver, name.asText(), mapper);
            DescribeMappingResponse response = MetricQueries.describe(subject.mapping(),
                    List.copyOf(subject.executors().keySet()));
            return toJson(response, mapper);
        }
    }

    /** One aggregate query, request and response in proto3 JSON. */
    static final class QueryMetricsAction implements ProtoAction {

        private final Map<String, ServedMetricSubject> subjects;
        private final ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver;

        QueryMetricsAction(Map<String, ServedMetricSubject> subjects,
                ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver) {
            this.subjects = subjects;
            this.resolver = resolver;
        }

        @Override
        public String name() {
            return "query-metrics";
        }

        @Override
        public String description() {
            return "Run one aggregate query against a metric mapping subject: measures, "
                    + "group-by dimensions with optional grains, equality filters, and a "
                    + "bounded limit, as a proto3-JSON QueryMetricsRequest under 'request'.";
        }

        @Override
        public ObjectNode inputSchema() {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode schema = mapper.createObjectNode();
            schema.put("type", "object");
            schema.put("additionalProperties", false);
            ObjectNode properties = schema.putObject("properties");
            properties.putObject("request")
                    .put("type", "object")
                    .put("description", "A proto3-JSON ai.pipestream.proto.metric.v1."
                            + "QueryMetricsRequest: mappingSubject, measures, dimensions, "
                            + "filters, limit, optional backend.");
            schema.putArray("required").add("request");
            return schema;
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            JsonNode requestNode = input.get("request");
            if (requestNode == null || !requestNode.isObject()) {
                throw new ActionException("invalid-input",
                        "request must be a proto3-JSON QueryMetricsRequest object");
            }
            QueryMetricsRequest.Builder request = QueryMetricsRequest.newBuilder();
            try {
                JsonFormat.parser().merge(requestNode.toString(), request);
            } catch (InvalidProtocolBufferException e) {
                throw new ActionException("invalid-input",
                        "request is not a valid QueryMetricsRequest: " + e.getMessage());
            }
            ServedMetricSubject subject = subject(
                    subjects, resolver, request.getMappingSubject(), mapper);
            try {
                QueryMetricsResponse response = MetricQueries.query(
                        subject.mapping(), subject.executors(), request.build());
                return toJson(response, mapper);
            } catch (MetricRefusal refusal) {
                throw refusal(refusal, mapper);
            }
        }
    }

    /** One declared-rollup rebuild, request and response in proto3 JSON. */
    static final class RebuildRollupAction implements ProtoAction {

        private final Map<String, ServedMetricSubject> subjects;
        private final ai.pipestream.proto.metric.spi.RollupSink rollups;
        private final ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver;

        RebuildRollupAction(Map<String, ServedMetricSubject> subjects,
                ai.pipestream.proto.metric.spi.RollupSink rollups,
                ai.pipestream.proto.metric.spi.MetricSubjectResolver resolver) {
            this.subjects = subjects;
            this.rollups = rollups;
            this.resolver = resolver;
        }

        @Override
        public String name() {
            return "rebuild-rollup";
        }

        @Override
        public String description() {
            return "Rebuild one declared rollup: the aggregate query runs on the named "
                    + "engine and its complete result atomically replaces the named lake "
                    + "table. Exact or refused, never truncated. The request is a "
                    + "proto3-JSON RebuildRollupRequest under 'request'.";
        }

        @Override
        public ObjectNode inputSchema() {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode schema = mapper.createObjectNode();
            schema.put("type", "object");
            schema.put("additionalProperties", false);
            ObjectNode properties = schema.putObject("properties");
            properties.putObject("request")
                    .put("type", "object")
                    .put("description", "A proto3-JSON ai.pipestream.proto.metric.v1."
                            + "RebuildRollupRequest: mappingSubject, measures, dimensions, "
                            + "filters, table, optional backend.");
            schema.putArray("required").add("request");
            return schema;
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            JsonNode requestNode = input.get("request");
            if (requestNode == null || !requestNode.isObject()) {
                throw new ActionException("invalid-input",
                        "request must be a proto3-JSON RebuildRollupRequest object");
            }
            ai.pipestream.proto.metric.RebuildRollupRequest.Builder request =
                    ai.pipestream.proto.metric.RebuildRollupRequest.newBuilder();
            try {
                JsonFormat.parser().merge(requestNode.toString(), request);
            } catch (InvalidProtocolBufferException e) {
                throw new ActionException("invalid-input",
                        "request is not a valid RebuildRollupRequest: " + e.getMessage());
            }
            if (request.getTable().isBlank()) {
                throw new ActionException("invalid-input",
                        "table is required: name the rollup table to replace");
            }
            if (request.getMeasuresList().isEmpty()) {
                throw new ActionException("invalid-input",
                        "measures is required: at least one measure member");
            }
            ServedMetricSubject subject = subject(
                    subjects, resolver, request.getMappingSubject(), mapper);
            try {
                return toJson(Rollups.rebuild(subject, rollups, request.build()), mapper);
            } catch (MetricRefusal refusal) {
                throw refusal(refusal, mapper);
            }
        }
    }
}
