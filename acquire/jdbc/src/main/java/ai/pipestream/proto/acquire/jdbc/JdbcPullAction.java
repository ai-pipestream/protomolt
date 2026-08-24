package ai.pipestream.proto.acquire.jdbc;

import ai.pipestream.proto.acquire.pull.PullReport;
import ai.pipestream.proto.acquire.pull.v1.PullFromJdbcRequest;
import ai.pipestream.proto.acquire.pull.v1.PullFromJdbcResponse;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

/**
 * The {@code pull-jdbc} verb: one {@link JdbcPull} pass as an action. The caller owns the
 * watermark and the query text; the source database connection is module configuration.
 */
public final class JdbcPullAction implements JsonAction {

    /** The action name: {@value}. */
    public static final String NAME = "pull-jdbc";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Enforces the request contract on the catalog path. */
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

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
                + " row through the intake service as a stable-identity JSON document (an updated"
                + " row replaces its own document); incremental queries bind the watermark to"
                + " their single ? placeholder and must order by the watermark column"
                + " ascending.";
    }

    @Override
    public Descriptor requestType() {
        return PullFromJdbcRequest.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return PullFromJdbcResponse.getDescriptor();
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        PullFromJdbcRequest request = parse(input);
        PullReport report;
        try {
            report = pull.pull(
                    request.getQuery(),
                    request.getIdColumn(),
                    request.getWatermarkColumn(),
                    request.getDatasourceId(),
                    request.getDrive(),
                    request.getWatermark(),
                    request.getMaxRows());
        } catch (IllegalArgumentException e) {
            throw new ActionException("invalid-input", e.getMessage());
        } catch (RuntimeException e) {
            throw new ActionException("pull-failed", e.getMessage());
        }
        // A PullReport under 'report', which is the one field the response message declares.
        ObjectNode output = MAPPER.createObjectNode();
        ObjectNode node = output.putObject("report");
        node.put("submitted", report.submitted());
        node.put("deduplicated", report.deduplicated());
        node.put("failed", report.failed());
        ArrayNode errors = node.putArray("errors");
        report.errors().forEach(errors::add);
        node.put("watermark", report.watermark());
        return output;
    }

    /**
     * Reads the envelope into a request and holds it to the message's declared rules.
     *
     * <p>Calls arriving through the catalog do not pass the validating interceptor the gRPC
     * surface uses, so the rules are applied here rather than left to the pass to discover
     * after it has opened a connection.
     */
    private static PullFromJdbcRequest parse(ObjectNode input) throws ActionException {
        PullFromJdbcRequest.Builder request = PullFromJdbcRequest.newBuilder();
        try {
            JsonFormat.parser().merge(input.toString(), request);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    "the pull is not a valid PullFromJdbcRequest: " + e.getMessage());
        }
        PullFromJdbcRequest built = request.build();
        ValidationResult result = VALIDATOR.validate(built);
        if (!result.valid()) {
            StringBuilder prose = new StringBuilder();
            for (ValidationResult.Violation violation : result.violations()) {
                if (prose.length() > 0) {
                    prose.append("; ");
                }
                prose.append(violation.path()).append(' ').append(violation.message());
            }
            throw new ActionException("invalid-input",
                    "The pull does not satisfy its contract: " + prose);
        }
        return built;
    }
}
