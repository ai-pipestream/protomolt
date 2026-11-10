package ai.protomolt.proto.metric.service;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.MetricAccess;
import ai.protomolt.proto.metric.DescribeMappingRequest;
import ai.protomolt.proto.metric.DescribeMappingResponse;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.metric.RebuildRollupRequest;
import ai.protomolt.proto.metric.RebuildRollupResponse;
import ai.protomolt.proto.metric.spi.MetricQueries;
import ai.protomolt.proto.metric.spi.MetricRefusal;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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

    private MetricActions() {
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
            ai.protomolt.proto.metric.spi.RollupSink rollups) {
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
            ai.protomolt.proto.metric.spi.RollupSink rollups,
            ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver) {
        return over(subjects, rollups, resolver, null);
    }

    /**
     * The verbs over the same served subjects, enforcing per-principal
     * {@code metric_access} rules from {@code accessPolicy}.
     */
    public static List<ProtoAction> over(
            Map<String, ServedMetricSubject> subjects,
            ai.protomolt.proto.metric.spi.RollupSink rollups,
            ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
            Supplier<AccessPolicy> accessPolicy) {
        Map<String, ServedMetricSubject> served = Map.copyOf(subjects);
        MetricAccessRewrites rewrites = new MetricAccessRewrites(accessPolicy);
        return List.of(new DescribeMappingAction(served, resolver, rewrites),
                new QueryMetricsAction(served, resolver, rewrites),
                new RebuildRollupAction(served, rollups, resolver, rewrites));
    }

    private static ServedMetricSubject restrictedSubject(
            Map<String, ServedMetricSubject> subjects,
            ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
            MetricAccessRewrites rewrites,
            Caller caller,
            String name,
            ObjectMapper mapper) throws ActionException {
        if (rewrites.accessFor(caller) != null && !subjects.containsKey(name)) {
            throw new ActionException("permission-denied",
                    "caller '" + caller.name() + "' carries metric access rules, which"
                            + " subject '" + name + "' (not statically served here)"
                            + " cannot enforce");
        }
        return subject(subjects, resolver, name, mapper);
    }

    private static ActionException statusFailure(StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        String description = e.getStatus().getDescription() != null
                ? e.getStatus().getDescription()
                : e.getMessage();
        if (code == Status.Code.PERMISSION_DENIED) {
            return new ActionException("permission-denied", description);
        }
        return new ActionException("failed-precondition", description);
    }

    private static ServedMetricSubject subject(
            Map<String, ServedMetricSubject> subjects,
            ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
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

    /** One subject's queryable surface: members, roles, backends. */
    static final class DescribeMappingAction implements ProtoAction {

        private final Map<String, ServedMetricSubject> subjects;
        private final ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver;
        private final MetricAccessRewrites rewrites;

        DescribeMappingAction(Map<String, ServedMetricSubject> subjects,
                ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
                MetricAccessRewrites rewrites) {
            this.subjects = subjects;
            this.resolver = resolver;
            this.rewrites = rewrites;
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

        @Override public Descriptor responseType() {
            return DescribeMappingResponse.getDescriptor();
        }

        @Override
        public Message execute(Message input, ActionContext context)
                throws ActionException {
            return execute(input, context, Caller.operator());
        }

        @Override
        public Message execute(Message input, ActionContext context, Caller caller)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            DescribeMappingRequest request = CatalogContract.as(
                    input, DescribeMappingRequest.getDefaultInstance(), name());
            ServedMetricSubject subject = restrictedSubject(
                    subjects, resolver, rewrites, caller, request.getMappingSubject(), mapper);
            DescribeMappingResponse response = MetricQueries.describe(subject.mapping(),
                    List.copyOf(subject.executors().keySet()));
            MetricAccess access = rewrites.accessFor(caller);
            return access == null ? response
                    : MetricAccessRewrites.filter(response, access, request.getMappingSubject());
        }
    }

    /** One aggregate query, request and response in proto3 JSON. */
    static final class QueryMetricsAction implements ProtoAction {

        private final Map<String, ServedMetricSubject> subjects;
        private final ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver;
        private final MetricAccessRewrites rewrites;

        QueryMetricsAction(Map<String, ServedMetricSubject> subjects,
                ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
                MetricAccessRewrites rewrites) {
            this.subjects = subjects;
            this.resolver = resolver;
            this.rewrites = rewrites;
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

        @Override public Descriptor responseType() {
            return QueryMetricsResponse.getDescriptor();
        }

        @Override
        public Message execute(Message input, ActionContext context)
                throws ActionException {
            return execute(input, context, Caller.operator());
        }

        @Override
        public Message execute(Message input, ActionContext context, Caller caller)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            QueryMetricsRequest request = CatalogContract.as(
                    input, QueryMetricsRequest.getDefaultInstance(), name());
            ServedMetricSubject subject = restrictedSubject(
                    subjects, resolver, rewrites, caller, request.getMappingSubject(), mapper);
            MetricAccess access = rewrites.accessFor(caller);
            QueryMetricsRequest effective;
            try {
                effective = access == null ? request
                        : MetricAccessRewrites.rewrite(
                                request, access, caller, subject.mapping());
            } catch (StatusRuntimeException e) {
                throw statusFailure(e);
            }
            try {
                return MetricQueries.query(
                        subject.mapping(), subject.executors(), effective);
            } catch (MetricRefusal refusal) {
                throw refusal(refusal, mapper);
            }
        }
    }

    /** One declared-rollup rebuild, request and response in proto3 JSON. */
    static final class RebuildRollupAction implements ProtoAction {

        private final Map<String, ServedMetricSubject> subjects;
        private final ai.protomolt.proto.metric.spi.RollupSink rollups;
        private final ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver;
        private final MetricAccessRewrites rewrites;

        RebuildRollupAction(Map<String, ServedMetricSubject> subjects,
                ai.protomolt.proto.metric.spi.RollupSink rollups,
                ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
                MetricAccessRewrites rewrites) {
            this.subjects = subjects;
            this.rollups = rollups;
            this.resolver = resolver;
            this.rewrites = rewrites;
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

        @Override public Descriptor responseType() {
            return RebuildRollupResponse.getDescriptor();
        }

        @Override
        public Message execute(Message input, ActionContext context)
                throws ActionException {
            return execute(input, context, Caller.operator());
        }

        @Override
        public Message execute(Message input, ActionContext context, Caller caller)
                throws ActionException {
            ObjectMapper mapper = context.objectMapper();
            if (rewrites.accessFor(caller) != null) {
                throw new ActionException("permission-denied",
                        "caller '" + caller.name() + "' carries metric access rules; a"
                                + " rebuilt rollup is an unrestricted reduction, so the"
                                + " rebuild verb is refused");
            }
            RebuildRollupRequest request = CatalogContract.as(
                    input, RebuildRollupRequest.getDefaultInstance(), name());
            ServedMetricSubject subject = subject(
                    subjects, resolver, request.getMappingSubject(), mapper);
            try {
                return Rollups.rebuild(subject, rollups, request,
                        name -> Subjects.find(subjects, resolver, name));
            } catch (MetricRefusal refusal) {
                throw refusal(refusal, mapper);
            }
        }
    }
}
