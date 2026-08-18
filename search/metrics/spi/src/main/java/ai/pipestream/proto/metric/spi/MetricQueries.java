package ai.pipestream.proto.metric.spi;

import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.DescribeMappingResponse;
import ai.pipestream.proto.metric.MappingMember;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricFilter;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.Dimension;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.Measure;
import ai.pipestream.proto.metric.spi.MetricMapping.FieldKind;
import ai.pipestream.proto.metric.spi.MetricMapping.MetricMember;
import dev.cel.common.types.SimpleType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The metric surface's policy layer: compiles a request against a built
 * mapping, refuses everything refusable with a stable code and the legal
 * set, hands the executor a fully-resolved {@link CompiledMetricQuery}, and
 * evaluates calculated measures over the engine's rows. Hosts (the gRPC
 * service, the actions) call this and map refusals onto their surface; the
 * shape rules the request proto declares are the validating interceptor's
 * job and are only re-checked here for hosts that bypass gRPC.
 */
public final class MetricQueries {

    /** The per-query row cap, matching the request proto's declared bound. */
    public static final int MAX_LIMIT = 1000;

    private MetricQueries() {
    }

    // ------------------------------------------------------------- describe

    /** One mapping's queryable surface, for the given mounted backends. */
    public static DescribeMappingResponse describe(
            MetricMapping mapping, List<MetricBackend> backends) {
        DescribeMappingResponse.Builder response = DescribeMappingResponse.newBuilder()
                .setMappingSubject(mapping.subject())
                .setMessageType(mapping.messageType())
                .addAllBackends(backends);
        for (MetricMember member : mapping.members().values()) {
            response.addMembers(MappingMember.newBuilder()
                    .setName(member.name())
                    .setRole(member.role())
                    .setAggregate(member.aggregate())
                    .setFieldPath(member.kind() == FieldKind.SYNTHETIC ? "" : member.fieldName())
                    .setDescription(member.description())
                    .setSensitivity(member.sensitivity())
                    .setDefaultGrain(member.defaultGrain()));
        }
        return response.build();
    }

    // ------------------------------------------------------------- query

    /**
     * Runs one request end to end: compile, capability-check, execute,
     * evaluate calculated measures, and trim rows to what was requested.
     *
     * @param mapping the subject's built mapping
     * @param executors the mounted executors, keyed by backend
     * @param request the raw request
     * @return the response, with the resolved backend echoed
     * @throws MetricRefusal naming what was refused and the legal set
     */
    public static QueryMetricsResponse query(
            MetricMapping mapping,
            Map<MetricBackend, MetricExecutor> executors,
            QueryMetricsRequest request) {
        MetricBackend backend = resolveBackend(request.getBackend(), executors.keySet());
        MetricExecutor executor = executors.get(backend);
        Plan plan = compile(mapping, backend, executor.capabilities(), request);
        MetricExecutor.Result result = executor.execute(plan.query());
        List<MetricRow> rows = finishRows(result.rows(), plan);
        return QueryMetricsResponse.newBuilder()
                .setMappingSubject(mapping.subject())
                .setBackend(backend)
                .addAllRows(rows)
                .setRowCount(rows.size())
                .setPhysicalPlan(result.physicalPlan())
                .build();
    }

    /** The engine the request runs on, or a refusal naming the mounted set. */
    static MetricBackend resolveBackend(MetricBackend requested, Set<MetricBackend> mounted) {
        List<String> legal = mounted.stream().map(Enum::name).sorted().toList();
        if (requested == MetricBackend.METRIC_BACKEND_UNSPECIFIED) {
            if (mounted.size() == 1) {
                return mounted.iterator().next();
            }
            throw new MetricRefusal(MetricRefusal.AMBIGUOUS_BACKEND,
                    "backend is unset and this subject mounts more than one engine; "
                            + "name one of: " + String.join(", ", legal), legal);
        }
        if (!mounted.contains(requested)) {
            throw new MetricRefusal(MetricRefusal.UNKNOWN_BACKEND,
                    "backend " + requested + " is not mounted for this subject; mounted: "
                            + String.join(", ", legal), legal);
        }
        return requested;
    }

    // ------------------------------------------------------------- compile

    /**
     * Compiles the request against the mapping and the executor's declared
     * capabilities. Calculated measures pull the physical measures they read
     * into the engine query; the post-pass strips what was not requested.
     */
    static Plan compile(
            MetricMapping mapping,
            MetricBackend backend,
            MetricExecutor.Capabilities capabilities,
            QueryMetricsRequest request) {
        if (request.getMeasuresCount() == 0) {
            throw new MetricRefusal(MetricRefusal.EMPTY_MEASURES,
                    "measures must name at least one member", measureNames(mapping));
        }
        if (request.getLimit() <= 0 || request.getLimit() > MAX_LIMIT) {
            throw new MetricRefusal(MetricRefusal.INVALID_LIMIT,
                    "limit must be within 1.." + MAX_LIMIT + ", got " + request.getLimit(),
                    List.of());
        }

        List<MetricMember> requested = new ArrayList<>();
        Set<String> engineMeasureNames = new LinkedHashSet<>();
        List<MetricMember> calculated = new ArrayList<>();
        for (String name : request.getMeasuresList()) {
            MetricMember member = measure(mapping, name);
            requested.add(member);
            if (member.calculated()) {
                calculated.add(member);
                for (String required : member.celRequires()) {
                    engineMeasureNames.add(measure(mapping, required).name());
                }
            } else {
                engineMeasureNames.add(member.name());
            }
        }

        List<Measure> measures = new ArrayList<>();
        for (String name : engineMeasureNames) {
            MetricMember member = mapping.members().get(name);
            if (!capabilities.aggregates().contains(member.aggregate())) {
                throw new MetricRefusal(MetricRefusal.UNSUPPORTED_AGGREGATE,
                        "aggregate " + member.aggregate() + " of measure '" + name
                                + "' is not supported by the " + backend + " executor; supported: "
                                + capabilities.aggregates().stream().map(Enum::name).sorted()
                                        .toList(),
                        capabilities.aggregates().stream().map(Enum::name).sorted().toList());
            }
            if (!member.rowFilters().isEmpty() && !capabilities.measureRowFilters()) {
                throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                        "measure '" + name + "' carries a row filter, which the " + backend
                                + " executor does not support", List.of());
            }
            measures.add(new Measure(
                    member.name(),
                    member.aggregate() == Aggregate.AGGREGATE_COUNT ? "" : member.fieldName(),
                    member.aggregate() == Aggregate.AGGREGATE_COUNT ? "" : member.fieldPath(),
                    member.aggregate(),
                    member.rowFilters()));
        }

        List<Dimension> dimensions = new ArrayList<>();
        for (MemberRef ref : request.getDimensionsList()) {
            MetricMember member = member(mapping, ref.getName());
            if (member.role() != MemberRole.MEMBER_ROLE_DIMENSION) {
                throw new MetricRefusal(MetricRefusal.ROLE_MISMATCH,
                        "'" + ref.getName() + "' is a measure; dimensions group, they are not "
                                + "computed", dimensionNames(mapping));
            }
            dimensions.add(dimensionOf(member, ref.getGrain(), backend, capabilities));
        }

        List<EqualsFilter> filters = new ArrayList<>();
        List<CompiledMetricQuery.DateRangeFilter> dateRanges = new ArrayList<>();
        for (MetricFilter filter : request.getFiltersList()) {
            MetricMember member = member(mapping, filter.getMember());
            if (member.role() != MemberRole.MEMBER_ROLE_DIMENSION) {
                throw new MetricRefusal(MetricRefusal.ROLE_MISMATCH,
                        "'" + filter.getMember() + "' is a measure; filters apply to dimensions",
                        dimensionNames(mapping));
            }
            if (filter.hasRange()) {
                if (filter.getEqualsCount() > 0) {
                    throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                            "filter on '" + filter.getMember() + "' declares both an"
                                    + " equality set and a range; pick one form",
                            List.of());
                }
                dateRanges.add(dateRange(member, filter));
                continue;
            }
            if (filter.getEqualsCount() == 0) {
                throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                        "filter on '" + filter.getMember()
                                + "' has an empty equality set, which would match nothing",
                        List.of());
            }
            DimensionKind kind = switch (member.kind()) {
                case KEYWORD -> DimensionKind.TERM;
                case BOOLEAN -> DimensionKind.BOOLEAN;
                default -> throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                        "filter on '" + filter.getMember() + "' needs a keyword or bool "
                                + "dimension, got " + member.kind()
                                + (member.kind() == FieldKind.DATE
                                        ? "; a DATE dimension filters by range" : ""),
                        List.of());
            };
            filters.add(new EqualsFilter(
                    member.name(), member.fieldName(), member.fieldPath(), kind,
                    filter.getEqualsList()));
        }

        return new Plan(
                new CompiledMetricQuery(mapping.subject(), backend, measures, dimensions,
                        filters, dateRanges, request.getLimit()),
                requested.stream().map(MetricMember::name).toList(),
                calculated);
    }

    /** One date-range filter, bounds resolved to inclusive UTC epoch millis. */
    private static CompiledMetricQuery.DateRangeFilter dateRange(
            MetricMember member, MetricFilter filter) {
        if (member.kind() != FieldKind.DATE) {
            throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                    "range on '" + filter.getMember() + "' needs a DATE dimension, got "
                            + member.kind() + "; keyword and bool dimensions filter by"
                            + " equality", List.of());
        }
        Long gte = dateBound(filter.getMember(), filter.getRange().getGte(), true);
        Long lte = dateBound(filter.getMember(), filter.getRange().getLte(), false);
        if (gte == null && lte == null) {
            throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                    "range on '" + filter.getMember() + "' has no bounds; give gte,"
                            + " lte, or both as ISO-8601 dates", List.of());
        }
        if (gte != null && lte != null && gte > lte) {
            throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                    "range on '" + filter.getMember() + "' is inverted: gte is after"
                            + " lte, so it would match nothing", List.of());
        }
        return new CompiledMetricQuery.DateRangeFilter(
                member.name(), member.fieldName(), member.fieldPath(), gte, lte);
    }

    /** An ISO date bound as inclusive UTC epoch millis; null when empty. */
    private static Long dateBound(String member, String bound, boolean lower) {
        if (bound.isEmpty()) {
            return null;
        }
        java.time.LocalDate date;
        try {
            date = java.time.LocalDate.parse(bound);
        } catch (java.time.format.DateTimeParseException e) {
            throw new MetricRefusal(MetricRefusal.UNSUPPORTED_FILTER,
                    "range bound '" + bound + "' on '" + member + "' is not an"
                            + " ISO-8601 date (like 2026-07-01)", List.of());
        }
        return lower
                ? date.atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli()
                : date.plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC)
                        .toInstant().toEpochMilli() - 1;
    }

    private static Dimension dimensionOf(
            MetricMember member,
            TimeGrain requestedGrain,
            MetricBackend backend,
            MetricExecutor.Capabilities capabilities) {
        if (member.kind() != FieldKind.DATE) {
            if (requestedGrain != TimeGrain.TIME_GRAIN_UNSPECIFIED) {
                throw new MetricRefusal(MetricRefusal.INVALID_GRAIN,
                        "grain on '" + member.name() + "' needs a DATE dimension, got "
                                + member.kind(), List.of());
            }
            DimensionKind kind = member.kind() == FieldKind.BOOLEAN
                    ? DimensionKind.BOOLEAN
                    : DimensionKind.TERM;
            return new Dimension(member.name(), member.fieldName(), member.fieldPath(), kind,
                    TimeGrain.TIME_GRAIN_UNSPECIFIED);
        }
        TimeGrain grain = requestedGrain != TimeGrain.TIME_GRAIN_UNSPECIFIED
                ? requestedGrain
                : member.defaultGrain();
        if (grain == TimeGrain.TIME_GRAIN_UNSPECIFIED) {
            throw new MetricRefusal(MetricRefusal.INVALID_GRAIN,
                    "date dimension '" + member.name() + "' needs a grain: none requested and "
                            + "none declared as default", grainNames());
        }
        if (!capabilities.dateGrains()) {
            throw new MetricRefusal(MetricRefusal.INVALID_GRAIN,
                    "the " + backend + " executor does not support date-grain bucketing",
                    List.of());
        }
        return new Dimension(member.name(), member.fieldName(), member.fieldPath(),
                DimensionKind.DATE, grain);
    }

    // ------------------------------------------------------------- post-pass

    /**
     * Evaluates calculated measures over each engine row, then trims every
     * row's measures to exactly what the request named.
     */
    private static List<MetricRow> finishRows(List<MetricRow> engineRows, Plan plan) {
        if (plan.calculated().isEmpty()) {
            return engineRows;
        }
        CelEnvironmentFactory env = CelEnvironmentFactory.builder();
        Set<String> vars = new LinkedHashSet<>();
        for (MetricMember member : plan.calculated()) {
            vars.addAll(member.celRequires());
        }
        vars.forEach(name -> env.addVar(name, SimpleType.DOUBLE));
        CelEvaluator evaluator = new CelEvaluator(env.build());

        List<MetricRow> finished = new ArrayList<>(engineRows.size());
        for (MetricRow row : engineRows) {
            Map<String, Double> values = new LinkedHashMap<>(row.getMeasuresMap());
            for (MetricMember member : plan.calculated()) {
                Map<String, Object> bindings = new HashMap<>();
                member.celRequires().forEach(
                        name -> bindings.put(name, values.getOrDefault(name, 0.0)));
                Object value;
                try {
                    value = evaluator.evaluateValue(member.cel(), bindings);
                } catch (CelEvaluationException e) {
                    throw new IllegalStateException("calculated measure '" + member.name()
                            + "' failed over a result row: " + e.getMessage(), e);
                }
                values.put(member.name(), ((Number) value).doubleValue());
            }
            MetricRow.Builder trimmed = MetricRow.newBuilder()
                    .putAllDimensions(row.getDimensionsMap());
            for (String name : plan.requestedMeasures()) {
                Double value = values.get(name);
                if (value != null) {
                    trimmed.putMeasures(name, value);
                }
            }
            finished.add(trimmed.build());
        }
        return finished;
    }

    // ------------------------------------------------------------- lookups

    private static MetricMember member(MetricMapping mapping, String name) {
        return mapping.member(name).orElseThrow(() -> new MetricRefusal(
                MetricRefusal.UNKNOWN_MEMBER,
                "unknown member '" + name + "'; members: "
                        + String.join(", ", mapping.memberNames()),
                mapping.memberNames()));
    }

    private static MetricMember measure(MetricMapping mapping, String name) {
        MetricMember member = member(mapping, name);
        if (member.role() != MemberRole.MEMBER_ROLE_MEASURE) {
            throw new MetricRefusal(MetricRefusal.ROLE_MISMATCH,
                    "'" + name + "' is a dimension; measures compute, they do not group",
                    measureNames(mapping));
        }
        return member;
    }

    private static List<String> measureNames(MetricMapping mapping) {
        return mapping.members().values().stream()
                .filter(m -> m.role() == MemberRole.MEMBER_ROLE_MEASURE)
                .map(MetricMember::name)
                .toList();
    }

    private static List<String> dimensionNames(MetricMapping mapping) {
        return mapping.members().values().stream()
                .filter(m -> m.role() == MemberRole.MEMBER_ROLE_DIMENSION)
                .map(MetricMember::name)
                .toList();
    }

    private static List<String> grainNames() {
        return List.of("TIME_GRAIN_DAY", "TIME_GRAIN_WEEK", "TIME_GRAIN_MONTH",
                "TIME_GRAIN_QUARTER", "TIME_GRAIN_YEAR");
    }

    /**
     * One compiled request: the engine query plus what the post-pass needs.
     *
     * @param query the engine-facing query (calculated inputs included)
     * @param requestedMeasures the measures the caller actually named
     * @param calculated the calculated measures to evaluate per row
     */
    record Plan(
            CompiledMetricQuery query,
            List<String> requestedMeasures,
            List<MetricMember> calculated) {
        Plan {
            requestedMeasures = List.copyOf(requestedMeasures);
            calculated = List.copyOf(calculated);
        }
    }
}
