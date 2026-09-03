package ai.protomolt.proto.acquire.s3;

import ai.protomolt.proto.acquire.pull.PullReport;
import ai.protomolt.proto.acquire.pull.v1.PullFromS3Request;
import ai.protomolt.proto.acquire.pull.v1.PullFromS3Response;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.validate.ProtoValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

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
    public Descriptor requestType() {
        return PullFromS3Request.getDescriptor();
    }

    @Override
    public Descriptor responseType() {
        return PullFromS3Response.getDescriptor();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        PullFromS3Request request = CatalogContract.as(
                input, PullFromS3Request.getDefaultInstance(), name());
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
        return PullFromS3Response.newBuilder().setReport(report.toProto()).build();
    }
}
