package ai.pipestream.proto.metric.spi;

import ai.pipestream.proto.metric.MetricRow;
import java.util.List;

/**
 * Where a rebuilt rollup lands: one aggregate result replacing one named
 * lake table, atomically, so readers see the old rollup or the new one and
 * never a mix. The sink owns the lake's addressing (catalog, namespace)
 * and the table's schema, derived from the rollup's member names —
 * dimension columns are the rendered strings, measure columns are the
 * wire's doubles. The door owns everything before the write: membership,
 * backend resolution, and the completeness gate.
 */
public interface RollupSink {

    /**
     * Replaces the table with the rollup rows.
     *
     * @param table the rollup table name inside the sink's namespace
     * @param dimensions the dimension member names, in column order
     * @param measures the measure member names, in column order
     * @param rows the complete rollup
     * @return what landed
     */
    Written replace(String table, List<String> dimensions, List<String> measures,
            List<MetricRow> rows);

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
