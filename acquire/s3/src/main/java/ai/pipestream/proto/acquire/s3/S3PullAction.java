package ai.pipestream.proto.acquire.s3;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.acquire.pull.PullReport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The {@code pull-s3} verb: one {@link S3Pull} pass as an action. The caller owns the
 * watermark — hand back the report's {@code watermark} on the next call for an incremental
 * pull, or persist it wherever operations state lives.
 */
public final class S3PullAction implements ProtoAction {

    /** The action name: {@value}. */
    public static final String NAME = "pull-s3";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final S3Pull pull;

    /**
     * Creates the action.
     *
     * @param pull the pull core
     */
    public S3PullAction(S3Pull pull) {
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
        return "Pulls new and changed objects from an S3 bucket past a watermark and feeds"
                + " them through the intake door with stable identity (a changed object"
                + " replaces its own document); returns counts, per-object errors, and the"
                + " watermark for the next pull.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("bucket")
                .put("type", "string")
                .put("description", "Source bucket");
        properties.putObject("prefix")
                .put("type", "string")
                .put("description", "Key prefix to restrict the pull; omit for the whole bucket");
        properties.putObject("datasourceId")
                .put("type", "string")
                .put("description", "Datasource pulled documents belong to");
        properties.putObject("drive")
                .put("type", "string")
                .put("description", "Target drive; omit for intake's default");
        properties.putObject("watermark")
                .put("type", "string")
                .put("description", "The previous pull's watermark; omit for a first pull");
        properties.putObject("maxObjects")
                .put("type", "integer")
                .put("description", "Cap on objects processed this pass; omit for no cap");
        schema.putArray("required").add("bucket").add("datasourceId");
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        PullReport report;
        try {
            report = pull.pull(
                    input.path("bucket").asText(""),
                    input.path("prefix").asText(""),
                    input.path("datasourceId").asText(""),
                    input.path("drive").asText(""),
                    input.path("watermark").asText(""),
                    input.path("maxObjects").asInt(0));
        } catch (IllegalArgumentException e) {
            throw new ActionException("invalid-input", e.getMessage());
        } catch (RuntimeException e) {
            throw new ActionException("pull-failed", e.getMessage());
        }
        return toJson(report);
    }

    static ObjectNode toJson(PullReport report) {
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
