package ai.pipestream.proto.acquire.jdbc;

import ai.pipestream.proto.acquire.pull.IntakeFeed;
import ai.pipestream.proto.acquire.pull.PullDocuments;
import ai.pipestream.proto.acquire.pull.PullReport;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One JDBC pull pass: run the caller's query against the source database, wrap each row as a
 * JSON document with stable identity {@code <idColumn>=<id>}, and feed it through the intake
 * door — an updated row re-saves its own document, an unchanged one dedupes at the repository.
 *
 * <p><b>The query owns its SQL and its types.</b> An incremental query carries exactly one
 * {@code ?} placeholder, bound with the watermark string (cast it in SQL as the column needs,
 * e.g. {@code updated_at > ?::timestamptz}); a first pull uses a placeholder-free query. A
 * watermark with no placeholder, or a placeholder with no watermark, is a contradiction and is
 * refused by name. The query must order by the watermark column ascending — out-of-order rows
 * are detected and refuse the pull, because a wrong watermark silently loses data.</p>
 *
 * <p>The watermark is the last processed row's watermark-column value as a string. Values are
 * compared numerically when both sides parse as numbers (auto-increment ids), lexically
 * otherwise (ISO-ish timestamps render in lexical order).</p>
 */
public final class JdbcPull {

    /** The connector identity stamped on pulled documents: {@value}. */
    public static final String CONNECTOR_ID = "jdbc-pull";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** The connection seam; each pull opens and closes one connection. */
    public interface ConnectionFactory {

        /** Opens a connection to the source database. */
        Connection open() throws SQLException;
    }

    private final ConnectionFactory connections;
    private final IntakeFeed feed;

    /**
     * Creates the pull core.
     *
     * @param connections the source-side connection seam
     * @param feed the intake submission seam
     */
    public JdbcPull(ConnectionFactory connections, IntakeFeed feed) {
        if (connections == null) {
            throw new IllegalArgumentException("connections must not be null");
        }
        if (feed == null) {
            throw new IllegalArgumentException("feed must not be null");
        }
        this.connections = connections;
        this.feed = feed;
    }

    /**
     * One pull pass.
     *
     * @param query the source query; one {@code ?} placeholder for incremental pulls, none for
     *        a first pull; must order by the watermark column ascending
     * @param idColumn result-set column holding the row's stable identity
     * @param watermarkColumn result-set column the watermark advances along
     * @param datasourceId the datasource pulled documents belong to
     * @param drive the target drive, or blank for intake's default
     * @param watermark the previous pull's watermark, or blank for a first pull
     * @param maxRows cap on rows processed this pass, or 0 for no cap
     */
    public PullReport pull(String query, String idColumn, String watermarkColumn,
                           String datasourceId, String drive, String watermark, int maxRows) {
        requireNonBlank(query, "query");
        requireNonBlank(idColumn, "idColumn");
        requireNonBlank(watermarkColumn, "watermarkColumn");
        requireNonBlank(datasourceId, "datasourceId");
        boolean hasWatermark = watermark != null && !watermark.isBlank();

        try (Connection connection = connections.open();
                PreparedStatement statement = connection.prepareStatement(query)) {
            int placeholders = statement.getParameterMetaData().getParameterCount();
            if (placeholders > 1) {
                throw new IllegalArgumentException("query carries " + placeholders
                        + " placeholders; a pull query binds at most one (the watermark)");
            }
            if (placeholders == 1 && !hasWatermark) {
                throw new IllegalArgumentException("query has a watermark placeholder but no"
                        + " watermark was given; run the first pull with a placeholder-free"
                        + " query");
            }
            if (placeholders == 0 && hasWatermark) {
                throw new IllegalArgumentException("a watermark was given but the query has no"
                        + " placeholder to bind it");
            }
            if (placeholders == 1) {
                statement.setString(1, watermark);
            }
            if (maxRows > 0) {
                statement.setMaxRows(maxRows);
            }
            try (ResultSet rows = statement.executeQuery()) {
                return process(rows, idColumn, watermarkColumn, datasourceId, drive, watermark);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("pull query failed: " + e.getMessage(), e);
        }
    }

    private PullReport process(ResultSet rows, String idColumn, String watermarkColumn,
                               String datasourceId, String drive, String watermark)
            throws SQLException {
        ResultSetMetaData meta = rows.getMetaData();
        requireColumn(meta, idColumn);
        requireColumn(meta, watermarkColumn);

        PullReport.Accumulator report = new PullReport.Accumulator(watermark);
        String previousMark = null;
        while (rows.next()) {
            String id = rows.getString(idColumn);
            String mark = rows.getString(watermarkColumn);
            if (id == null || id.isBlank()) {
                report.failure("a row with a null " + idColumn + " cannot carry identity");
                continue;
            }
            if (mark == null || mark.isBlank()) {
                report.failure(idColumn + "=" + id + ": null " + watermarkColumn
                        + " cannot advance the watermark");
                continue;
            }
            if (previousMark != null && compareMarks(mark, previousMark) < 0) {
                throw new IllegalArgumentException("the query must order by " + watermarkColumn
                        + " ascending: row " + idColumn + "=" + id + " came out of order ('"
                        + mark + "' after '" + previousMark + "'); a wrong watermark silently"
                        + " loses rows");
            }
            previousMark = mark;

            String sourceKey = idColumn + "=" + id;
            try {
                byte[] payload = MAPPER.writeValueAsBytes(rowJson(rows, meta));
                Map<String, String> metadata = new LinkedHashMap<>();
                metadata.put("source", "jdbc");
                metadata.put("id", id);
                IngestDocumentResponse receipt = feed.submit(
                        PullDocuments.document(CONNECTOR_ID, datasourceId, sourceKey,
                                ByteString.copyFrom(payload), id + ".json", "application/json"),
                        datasourceId, drive, metadata);
                report.success(receipt.getDeduplicated(), mark);
            } catch (RuntimeException | com.fasterxml.jackson.core.JsonProcessingException e) {
                report.failure(sourceKey + ": " + e.getMessage());
            }
        }
        return report.report();
    }

    /** All columns of the current row as one JSON object; types the wire can carry survive. */
    private static ObjectNode rowJson(ResultSet rows, ResultSetMetaData meta)
            throws SQLException {
        ObjectNode row = MAPPER.createObjectNode();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String column = meta.getColumnLabel(i);
            Object value = rows.getObject(i);
            switch (value) {
                case null -> row.putNull(column);
                case Boolean b -> row.put(column, b);
                case Integer n -> row.put(column, n);
                case Long n -> row.put(column, n);
                case Double n -> row.put(column, n);
                case Float n -> row.put(column, n);
                case BigDecimal n -> row.put(column, n);
                case String s -> row.put(column, s);
                case byte[] bytes -> row.put(column,
                        Base64.getEncoder().encodeToString(bytes));
                default -> row.put(column, rows.getString(i));
            }
        }
        return row;
    }

    private static void requireColumn(ResultSetMetaData meta, String column)
            throws SQLException {
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (meta.getColumnLabel(i).equalsIgnoreCase(column)) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "column '" + column + "' is not in the query's result set");
    }

    /** Numeric when both sides parse as numbers, lexical otherwise. */
    static int compareMarks(String a, String b) {
        try {
            return new BigDecimal(a).compareTo(new BigDecimal(b));
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
