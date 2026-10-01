package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.receipt.RecordArtifact;
import ai.protomolt.proto.receipt.RecordVerifier;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.Verification;
import ai.protomolt.proto.receipt.WorkRecord;
import ai.protomolt.proto.receipt.WorkRecords;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;

/** Verifies an execution receipt against independently loaded authoring run evidence. */
public final class WorkflowAuthoringReceipt {
    private WorkflowAuthoringReceipt() { }

    /**
     * Verifies signature, complete artifact custody, and exact run projection.
     * The caller must load run evidence by the candidate's run ID from its own
     * repository, and supply the fingerprint of the independently compiled source.
     * This does not replace independent fixture execution or caller policy checks.
     *
     * @return the verified manifest digest
     */
    public static String verify(ArtifactReference receipt, RunEvidence run,
            String compiledFingerprint, TrustSnapshot trust, ArtifactRepository artifacts)
            throws IOException {
        WorkflowValidation.validate(run);
        if (run.getStatus() != RunStatus.RUN_STATUS_SUCCEEDED || !run.hasOutputArtifact()
                || !run.getWorkflowFingerprint().equals(compiledFingerprint)) {
            throw new IllegalArgumentException("run did not succeed with output for the compiled workflow");
        }
        if (Long.compareUnsigned(receipt.getSizeBytes(), WorkRecords.MAX_RECORD_BYTES) > 0) {
            throw new IllegalArgumentException("receipt exceeds record bounds");
        }
        byte[] signed = required(artifacts, receipt).content();
        // Authenticate before following references in a worker-supplied record.
        Verification authenticated = RecordVerifier.verify(signed, trust);
        if (!authenticated.verified()) {
            throw new IllegalArgumentException("receipt: " + authenticated.refusal().detail());
        }
        WorkRecord manifest = authenticated.manifest();
        var issuance = new WorkRecordProjector.Issuance(manifest.getRecordId(), manifest.getIssuer(),
                manifest.getKeyId(), manifest.getIssuedAt(), manifest.getPriorManifestSha256());
        if (!Arrays.equals(WorkRecords.canonicalBytes(manifest),
                WorkRecords.canonicalBytes(WorkRecordProjector.project(run, issuance)))) {
            throw new IllegalArgumentException("receipt does not describe the stored run");
        }
        var references = new ArrayList<>(manifest.getArtifactsList());
        for (var step : manifest.getStepsList()) {
            if (step.hasRequestArtifact()) references.add(step.getRequestArtifact());
            if (step.hasResponseArtifact()) references.add(step.getResponseArtifact());
        }
        var bytes = new LinkedHashMap<String, byte[]>();
        long total = 0;
        for (RecordArtifact reference : references) {
            // Bound bytes read even for an authenticated but unexpectedly large run.
            if (reference.getSizeBytes() < 0 || reference.getSizeBytes() > 4 * 1024 * 1024) {
                throw new IllegalArgumentException("receipt artifact exceeds 4 MiB");
            }
            if (!bytes.containsKey(reference.getSha256())) total += reference.getSizeBytes();
            if (total > 64L * 1024 * 1024) {
                throw new IllegalArgumentException("receipt artifacts exceed 64 MiB");
            }
            var expected = ArtifactReference.newBuilder().setSha256(reference.getSha256())
                    .setSizeBytes(reference.getSizeBytes()).setMediaType(reference.getMediaType())
                    .setRedacted(reference.getRedacted()).build();
            bytes.put(reference.getSha256(), required(artifacts, expected).content());
        }
        Verification verified = RecordVerifier.verify(signed, trust, bytes);
        if (!verified.verified() || verified.checks().stream()
                .anyMatch(check -> check.status() != Verification.Check.Status.PASSED)) {
            throw new IllegalArgumentException("receipt artifact verification failed");
        }
        return verified.manifestDigest();
    }

    private static ArtifactRepository.StoredArtifact required(ArtifactRepository artifacts,
            ArtifactReference reference) throws IOException {
        WorkflowValidation.validate(reference);
        var stored = artifacts.find(reference.getSha256()).orElseThrow(() ->
                new IllegalArgumentException("missing artifact " + reference.getSha256()));
        if (!stored.reference().equals(reference)) {
            throw new IllegalArgumentException("artifact reference differs from storage");
        }
        return stored;
    }
}
