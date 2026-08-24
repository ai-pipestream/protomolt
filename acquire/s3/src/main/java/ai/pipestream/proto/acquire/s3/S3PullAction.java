package ai.pipestream.proto.acquire.s3;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.acquire.pull.PullReport;
import ai.pipestream.proto.acquire.pull.v1.PullFromS3Request;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
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

    /** Enforces the request contract on the catalog path. */
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

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
                + " them through the intake service with stable identity (a changed object"
                + " replaces its own document); returns counts, per-object errors, and the"
                + " watermark for the next pull.";
    }

    @Override
    public ObjectNode inputSchema() {
        // Derived from the request message, so the bounds and the required fields the pass
        // enforces are visible to a caller before it starts one.
        return MAPPER.valueToTree(ProtoJsonSchemaGenerator.create()
                .generate(PullFromS3Request.getDescriptor()));
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        PullFromS3Request request = parse(input);
        PullReport report;
        try {
            report = pull.pull(
                    request.getBucket(),
                    request.getPrefix(),
                    request.getDatasourceId(),
                    request.getDrive(),
                    request.getWatermark(),
                    request.getMaxObjects());
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

    /**
     * Reads the envelope into a request and holds it to the message's declared rules.
     *
     * <p>Calls arriving through the catalog do not pass the validating interceptor the gRPC
     * surface uses, so the rules are applied here rather than left to the pass to discover
     * after it has already started reading.
     */
    private static PullFromS3Request parse(ObjectNode input) throws ActionException {
        PullFromS3Request.Builder request = PullFromS3Request.newBuilder();
        try {
            JsonFormat.parser().merge(input.toString(), request);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    "the pull is not a valid PullFromS3Request: " + e.getMessage());
        }
        PullFromS3Request built = request.build();
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
