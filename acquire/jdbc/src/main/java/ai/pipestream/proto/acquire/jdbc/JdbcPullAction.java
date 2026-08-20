package ai.pipestream.proto.acquire.jdbc;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.acquire.pull.PullReport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@code pull-jdbc} verb: one {@link JdbcPull} pass as an action. The caller owns the
 * watermark and the query text; the source database connection is module configuration.
 */
public final class JdbcPullAction implements ProtoAction {

    /** The action name: {@value}. */
    public static final String NAME = "pull-jdbc";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final JdbcPull pull;

    /**
     * Creates the action.
     *
     * @param pull the pull core
     */
    public JdbcPullAction(JdbcPull pull) {
        if (pull == null) {
            throw new IllegalArgumentException("pull must not be null");
        }
        this.pull = pull;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Runs a watermark query against the configured source database and feeds each"
                + " row through the intake door as a stable-identity JSON document (an updated"
                + " row replaces its own document); incremental queries bind the watermark to"
                + " their single ? placeholder and must order by the watermark column"
                + " ascending.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("query")
                .put("type", "string")
                .put("description", "Source query; one ? placeholder for incremental pulls,"
                        + " none for a first pull; must order by the watermark column");
        properties.putObject("idColumn")
                .put("type", "string")
                .put("description", "Result-set column carrying the row's stable identity");
        properties.putObject("watermarkColumn")
                .put("type", "string")
                .put("description", "Result-set column the watermark advances along");
        properties.putObject("datasourceId")
                .put("type", "string")
                .put("description", "Datasource pulled documents belong to");
        properties.putObject("drive")
                .put("type", "string")
                .put("description", "Target drive; omit for intake's default");
        properties.putObject("watermark")
                .put("type", "string")
                .put("description", "The previous pull's watermark; omit for a first pull");
        properties.putObject("maxRows")
                .put("type", "integer")
                .put("description", "Cap on rows processed this pass; omit for no cap");
        schema.putArray("required")
                .add("query").add("idColumn").add("watermarkColumn").add("datasourceId");
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        PullReport report;
        try {
            report = pull.pull(
                    input.path("query").asText(""),
                    input.path("idColumn").asText(""),
                    input.path("watermarkColumn").asText(""),
                    input.path("datasourceId").asText(""),
                    input.path("drive").asText(""),
                    input.path("watermark").asText(""),
                    input.path("maxRows").asInt(0));
        } catch (IllegalArgumentException e) {
            throw new ActionException("invalid-input", e.getMessage());
        } catch (RuntimeException e) {
            throw new ActionException("pull-failed", e.getMessage());
        }
        ObjectNode output = MAPPER.createObjectNode();
        output.put("submitted", report.submitted());
        output.put("deduplicated", report.deduplicated());
        output.put("failed", report.failed());
        ArrayNode errors = output.putArray("errors");
        report.errors().forEach(errors::add);
        output.put("watermark", report.watermark());
        return output;
    }
}
