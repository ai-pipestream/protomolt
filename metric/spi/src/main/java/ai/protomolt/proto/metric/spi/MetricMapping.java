package ai.protomolt.proto.metric.spi;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.TimeGrain;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * One message type's queryable surface, built from its metric.v1
 * declarations: the members, by public name, in declaration order. Built
 * once at mount time — every schema error fails the build, never the first
 * query — and independent of any backend, so describing a mapping needs no
 * index and no table.
 *
 * @param subject the mapping subject this surface serves
 * @param messageType the fully-qualified message type it was built from
 * @param members the queryable members, keyed by public name, in
 *        declaration order
 */
public record MetricMapping(
        String subject, String messageType, Map<String, MetricMember> members) {

    public MetricMapping {
        members = new LinkedHashMap<>(members);
    }

    /** The member by public name, when declared. */
    public Optional<MetricMember> member(String name) {
        return Optional.ofNullable(members.get(name));
    }

    /** Every member name, in declaration order. */
    public List<String> memberNames() {
        return List.copyOf(members.keySet());
    }

    /**
     * One queryable member. Row filters are translated from {@code
     * filter_cel} at build time into the engine-neutral equality form, so
     * executors never see CEL; a calculated member carries its CEL source
     * and the sibling measures it reads, evaluated by the host after the
     * engine reduces.
     *
     * @param name the public member name (declared name, or the field name)
     * @param role dimension or measure
     * @param aggregate the measure's reduction; UNSPECIFIED on dimensions
     *        and calculated measures
     * @param fieldName the flattened engine field name (the flat mapper
     *        convention, underscore-joined)
     * @param fieldPath the proto field path (dot-joined field names), the
     *        column address for engines that keep the message's nesting
     * @param kind how engines bucket or reduce the field
     * @param rowFilters this measure's row filters, translated from its
     *        filter_cel; empty = all rows
     * @param cel the calculated measure's CEL over sibling measure names;
     *        empty = a physical measure
     * @param celRequires the sibling measures the calculated CEL reads
     * @param defaultGrain the DATE dimension's fallback grain; UNSPECIFIED
     *        elsewhere
     * @param description human description from meta.v1; empty when absent
     * @param sensitivity sensitivity class from meta.v1; empty when absent
     */
    public record MetricMember(
            String name,
            MemberRole role,
            Aggregate aggregate,
            String fieldName,
            String fieldPath,
            FieldKind kind,
            List<CompiledMetricQuery.EqualsFilter> rowFilters,
            String cel,
            List<String> celRequires,
            TimeGrain defaultGrain,
            String description,
            String sensitivity) {

        public MetricMember {
            rowFilters = List.copyOf(rowFilters);
            celRequires = List.copyOf(celRequires);
        }

        /** Whether this measure is calculated (CEL over siblings). */
        public boolean calculated() {
            return !cel.isEmpty();
        }
    }

    /** The field shapes the metric surface distinguishes. */
    public enum FieldKind {
        /** String-valued: term dimensions, COUNT/COUNT_DISTINCT measures. */
        KEYWORD,
        /** Integer or floating numeric: any aggregate, term-free. */
        NUMERIC,
        /** Boolean: two-bucket dimensions. */
        BOOLEAN,
        /** google.protobuf.Timestamp: calendar-grain dimensions. */
        DATE,
        /** ai.pipestream.proto.types.v1.TreePath: whole-path dimensions and prefix filters. */
        TREE_PATH,
        /** No backing field: a calculated measure. */
        SYNTHETIC
    }
}
