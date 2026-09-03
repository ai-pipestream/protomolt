package ai.protomolt.proto.metric.spi;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MetricRow;
import java.util.List;

/**
 * Where a rebuilt rollup lands: one aggregate result replacing one named
 * lake table, atomically, so readers see the old rollup or the new one and
 * never a mix. The sink owns the lake's addressing (catalog, namespace)
 * and the table's schema, derived from the rollup's member names —
 * dimension columns are the rendered strings, measure columns are the
 * wire's doubles. The service owns everything before the write: membership,
 * backend resolution, and the completeness gate.
 *
 * <p>The sink also owns making the rollup self-describing: the source
 * subject and each measure's source aggregate ride along, so a sink can
 * stamp the declaration onto the table itself and a
 * {@link MetricSubjectResolver} can serve the rollup back as a queryable
 * subject without any side-channel configuration.</p>
 */
public interface RollupSink {

    /**
     * Replaces the table with the rollup rows.
     *
     * @param sourceSubject the mapping subject the rollup was built from
     * @param table the rollup table name inside the sink's namespace
     * @param dimensions the dimension member names, in column order
     * @param measures the measure columns, in column order, each carrying
     *        the aggregate that produced it
     * @param rows the complete rollup
     * @return what landed
     */
    Written replace(String sourceSubject, String table, List<String> dimensions,
            List<MeasureColumn> measures, List<MetricRow> rows);

    /**
     * One measure column of a rollup.
     *
     * @param member the measure member name (the column name)
     * @param sourceAggregate the aggregate that produced the column's
     *        values; what re-aggregation is honest depends on it
     */
    record MeasureColumn(String member, Aggregate sourceAggregate) {

        public MeasureColumn {
            if (member == null || member.isBlank()) {
                throw new IllegalArgumentException("member must not be blank");
            }
            if (sourceAggregate == null) {
                throw new IllegalArgumentException("sourceAggregate must not be null");
            }
        }
    }

    /**
     * What one replace committed.
     *
     * @param table the replaced table as the lake knows it
     *        (namespace-qualified)
     * @param rowsWritten rows the rollup now holds
     * @param snapshotId the lake snapshot the replace committed
     */
    record Written(String table, long rowsWritten, long snapshotId) {
    }
}
