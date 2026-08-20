package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.receipt.SignedWorkRecord;
import ai.pipestream.proto.receipt.WorkRecord;
import ai.pipestream.proto.receipt.WorkRecords;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;

/** Projects a stored run's evidence into a canonical signed work record. */
final class ExportWorkRecordAction implements ProtoAction {

    private final RunEvidenceRepository runs;
    private final RecordSigning signing;
    private final Clock clock;

    ExportWorkRecordAction(RunEvidenceRepository runs, RecordSigning signing) {
        this(runs, signing, Clock.systemUTC());
    }

    ExportWorkRecordAction(RunEvidenceRepository runs, RecordSigning signing, Clock clock) {
        this.runs = runs;
        this.signing = signing;
        this.clock = clock;
    }

    @Override
    public String name() {
        return "export-work-record";
    }

    @Override
    public String description() {
        return "Projects a recorded run's evidence into a canonical signed work record that "
                + "verifies offline: deterministic manifest bytes, a detached Ed25519 issuer "
                + "signature, artifacts by digest, and a signed completeness claim.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = WorkflowActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        WorkflowActionJson.identitySchema(properties, "runId")
                .put("description", "Stored run-evidence identity to project.");
        WorkflowActionJson.identitySchema(properties, "recordId")
                .put("description", "Record identity; 'record-<runId>' when omitted.");
        properties.putObject("priorManifestSha256").put("type", "string")
                .put("pattern", "^[0-9a-f]{64}$")
                .put("description", "Manifest digest of the record this one re-issues.");
        schema.putArray("required").add("runId");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        if (runs == null) {
            throw WorkflowActionJson.unavailable("work-record export",
                    "start protomolt-serve with --workflow-workspace");
        }
        if (signing == null) {
            throw WorkflowActionJson.unavailable("work-record signing",
                    "set " + RecordSigning.ENV_KEY_FILE + ", " + RecordSigning.ENV_KEY_ID
                            + ", and " + RecordSigning.ENV_ISSUER);
        }
        String runId = WorkflowActionJson.identity(input, "runId");
        String recordId = WorkflowActionJson.optionalIdentity(input, "recordId");
        if (recordId == null) {
            recordId = "record-" + runId;
        }
        String prior = WorkflowActionJson.optionalText(input, "priorManifestSha256");
        if (prior != null && !prior.matches("[0-9a-f]{64}")) {
            throw WorkflowActionJson.invalid(
                    "'priorManifestSha256' must be a lowercase SHA-256 digest",
                    "/priorManifestSha256");
        }
        RunEvidence evidence;
        try {
            evidence = runs.find(runId).orElseThrow(() ->
                    WorkflowActionJson.invalid("No run evidence named '" + runId + "'",
                            "/runId"));
        } catch (ActionException e) {
            throw e;
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Run evidence read failed: " + e.getMessage());
        }
        Instant now = clock.instant();
        WorkRecord manifest;
        try {
            manifest = WorkRecordProjector.project(evidence, new WorkRecordProjector.Issuance(
                    recordId, signing.issuer(), signing.signer().keyId(),
                    Timestamp.newBuilder().setSeconds(now.getEpochSecond())
                            .setNanos(now.getNano()).build(),
                    prior == null ? "" : prior));
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/runId");
        }
        SignedWorkRecord record = signing.signer().sign(manifest);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("recordBase64",
                Base64.getEncoder().encodeToString(record.toByteArray()));
        output.put("manifestDigest",
                WorkRecords.sha256Hex(record.getManifest().toByteArray()));
        output.put("recordId", recordId);
        return output;
    }
}
