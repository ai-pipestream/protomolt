package ai.protomolt.proto.acquire.jdbc;

import ai.protomolt.proto.acquire.pull.PullReport;
import ai.protomolt.proto.acquire.pull.v1.PullFromJdbcRequest;
import ai.protomolt.proto.acquire.pull.v1.PullFromJdbcResponse;
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
 * The {@code pull-jdbc} verb: one {@link JdbcPull} pass as an action. The caller owns the
 * watermark and the query text; the source database connection is module configuration.
 */
public final class JdbcPullAction implements ProtoAction {

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
    public Message execute(Message input, ActionContext context) throws ActionException {
        PullFromJdbcRequest request = CatalogContract.as(
                input, PullFromJdbcRequest.getDefaultInstance(), name());
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
        return PullFromJdbcResponse.newBuilder()
                .setReport(report.toProto())
                .build();
    }
}
