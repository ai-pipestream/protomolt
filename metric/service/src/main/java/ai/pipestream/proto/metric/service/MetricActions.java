package ai.pipestream.proto.metric.service;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.metric.DescribeMappingRequest;
import ai.pipestream.proto.metric.DescribeMappingResponse;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.RebuildRollupRequest;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.util.List;
import java.util.Map;

/**
 * The metric service's three catalog verbs. Registered on an action catalog
 * they are MCP tools with no translation layer, which is the moment an
 * agent surface can answer an aggregate question: {@code describe-mapping}
 * tells it what is queryable, {@code query-metrics} runs the proto3-JSON
 * {@code QueryMetricsRequest} it writes, and {@code rebuild-rollup}
 * replaces a declared lake rollup with a fresh complete answer. Refusals
 * surface the SPI's stable code with the legal set in details, so a
 * tool-using model can correct itself without parsing prose.
 */
public final class MetricActions {

    /**
     * Enforces the request contract on the catalog path.
     *
     * <p>Calls arriving over gRPC pass a validating interceptor before they reach a handler.
     * Calls arriving as catalog verbs do not, so without this the same request would be
     * refused on one surface and accepted on the other.
     */
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private MetricActions() {
    }

    /**
     * The input schema for a verb, derived from the request message it accepts.
     *
     * <p>Deriving rather than hand-writing keeps one description of the contract. The generator
     * folds the message's declared validation rules into the schema, so a caller reading the
     * tool manifest sees the same bounds the gate enforces, and a rule added to the proto
     * reaches every surface without a second edit.
     */
    private static ObjectNode schemaFor(Descriptor request) {
        return new ObjectMapper().valueToTree(
                ProtoJsonSchemaGenerator.create().generateRooted(request));
    }

    /**
     * Parses an action envelope into the request message it must satisfy.
     *
     * <p>The envelope is the message's canonical proto3 JSON form, so the same document works
     * over gRPC, over the JSON gateway, and as a tool call. Unknown members are refused rather
     * than ignored: a caller that misspells a field has written a query it did not mean, and
     * silently dropping it would answer a different question.
     */
    private static <B extends Message.Builder> B parse(ObjectNode input, B builder, String verb)
            throws ActionException {
        try {
            JsonFormat.parser().merge(input.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    verb + " expects a " + builder.getDescriptorForType().getName()
                            + ": " + e.getMessage());
        }
        ValidationResult result = VALIDATOR.validate(builder.build());
        if (!result.valid()) {
            throw new ActionException("invalid-input",
                    verb + " does not satisfy the request contract: " + describe(result),
                    violations(result));
        }
        return builder;
    }

    /** The violations as machine-readable details, each naming its field and its rule. */
    private static ObjectNode violations(ValidationResult result) {
        ObjectNode details = new ObjectMapper().createObjectNode();
        ArrayNode listed = details.putArray("violations");
        for (ValidationResult.Violation violation : result.violations()) {
            ObjectNode node = listed.addObject();
            node.put("field", violation.path());
            node.put("ruleId", violation.ruleId());
            node.put("message", violation.message());
        }
        return details;
    }

    private static String describe(ValidationResult result) {
        StringBuilder out = new StringBuilder();
        for (ValidationResult.Violation violation : result.violations()) {
            if (out.length() > 0) {
                out.append("; ");
            }
            out.append(violation.path()).append(' ').append(violation.message());
        }
        return out.toString();
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
        public String requiredScope() {
            return Scopes.METRICS_QUERY;
        }

        @Override
        public String description() {
            return "Describe one metric mapping subject: its members with roles, aggregates, "
                    + "descriptions and sensitivity, and the backends this mount can run.";
        }

        @Override public Descriptor requestType() {
            return DescribeMappingRequest.getDescriptor();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            DescribeMappingRequest request =
                    parse(input, DescribeMappingRequest.newBuilder(), "describe-mapping").build();
            ServedMetricSubject subject =
                    subject(subjects, resolver, request.getMappingSubject(), mapper);
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
        public String requiredScope() {
            return Scopes.METRICS_QUERY;
        }

        @Override
        public String description() {
            return "Run one aggregate query against a metric mapping subject: measures, "
                    + "group-by dimensions with optional grains, equality filters, and a "
                    + "bounded limit, as a proto3-JSON QueryMetricsRequest under 'request'.";
        }

        @Override public Descriptor requestType() {
            return QueryMetricsRequest.getDescriptor();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            QueryMetricsRequest.Builder request =
                    parse(input, QueryMetricsRequest.newBuilder(), "query-metrics");
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
        public String requiredScope() {
            return Scopes.METRICS_REBUILD;
        }

        @Override
        public String description() {
            return "Rebuild one declared rollup: the aggregate query runs on the named "
                    + "engine and its complete result atomically replaces the named lake "
                    + "table. Exact or refused, never truncated. The request is a "
                    + "proto3-JSON RebuildRollupRequest under 'request'.";
        }

        @Override public Descriptor requestType() {
            return RebuildRollupRequest.getDescriptor();
        }

        @Override
        public ObjectNode execute(ObjectNode input, ActionContext context)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            // table and measures carry required and min_items rules on the request message, so
            // the gate refuses an empty one before it reaches here. Repeating those checks in
            // Java would give the same query two different refusal messages.
            RebuildRollupRequest.Builder request =
                    parse(input, RebuildRollupRequest.newBuilder(), "rebuild-rollup");
            ServedMetricSubject subject = subject(
                    subjects, resolver, request.getMappingSubject(), mapper);
            try {
                return toJson(Rollups.rebuild(subject, rollups, request.build(),
                        name -> Subjects.find(subjects, resolver, name)), mapper);
            } catch (MetricRefusal refusal) {
                throw refusal(refusal, mapper);
            }
        }
    }
}
