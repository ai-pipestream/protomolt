package ai.protomolt.proto.workflow;

import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.RunStatus;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.receipt.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthoringReceiptTest {
    @TempDir Path temp;

    @Test
    void verifiesStoredRunAndRefusesAValidReceiptFromAnotherRun() throws Exception {
        var fixture = fixture();
        assertThat(WorkflowAuthoringReceipt.verify(fixture.receipt, fixture.run,
                fixture.run.getWorkflowFingerprint(), fixture.trust, fixture.artifacts)).hasSize(64);
        var other = fixture.run.toBuilder().setRunId("another-run").build();
        assertThatThrownBy(() -> WorkflowAuthoringReceipt.verify(fixture.receipt, other,
                other.getWorkflowFingerprint(), fixture.trust, fixture.artifacts))
                .hasMessageContaining("does not describe the stored run");
        assertThatThrownBy(() -> WorkflowAuthoringReceipt.verify(fixture.receipt, fixture.run,
                "b".repeat(64), fixture.trust, fixture.artifacts))
                .hasMessageContaining("compiled workflow");
        assertThatThrownBy(() -> WorkflowAuthoringReceipt.verify(fixture.receipt,
                fixture.run.toBuilder().clearOutputArtifact().build(),
                fixture.run.getWorkflowFingerprint(), fixture.trust, fixture.artifacts))
                .hasMessageContaining("with output");
    }

    @Test
    void requiresEveryArtifactAndItsExactMetadata() throws Exception {
        var fixture = fixture();
        var missing = new ArtifactRepository() {
            public ArtifactReference save(byte[] bytes, String media, boolean redacted) {
                throw new UnsupportedOperationException();
            }
            public Optional<StoredArtifact> find(String sha) throws java.io.IOException {
                return sha.equals(fixture.run.getOutputArtifact().getSha256())
                        ? Optional.empty() : fixture.artifacts.find(sha);
            }
        };
        assertThatThrownBy(() -> WorkflowAuthoringReceipt.verify(fixture.receipt, fixture.run,
                fixture.run.getWorkflowFingerprint(), fixture.trust, missing))
                .hasMessageContaining("missing artifact");
        assertThatThrownBy(() -> WorkflowAuthoringReceipt.verify(
                fixture.receipt.toBuilder().setRedacted(true).build(), fixture.run,
                fixture.run.getWorkflowFingerprint(), fixture.trust, fixture.artifacts))
                .hasMessageContaining("reference differs");
    }

    @Test
    void refusesUntrustedSignaturesAndChangedRunOutput() throws Exception {
        var fixture = fixture();
        assertThatThrownBy(() -> WorkflowAuthoringReceipt.verify(fixture.receipt, fixture.run,
                fixture.run.getWorkflowFingerprint(), TrustSnapshot.getDefaultInstance(), fixture.artifacts))
                .isInstanceOf(IllegalArgumentException.class);
        var changed = fixture.run.toBuilder().setOutputArtifact(fixture.run.getInputArtifact()).build();
        assertThatThrownBy(() -> WorkflowAuthoringReceipt.verify(fixture.receipt, changed,
                changed.getWorkflowFingerprint(), fixture.trust, fixture.artifacts))
                .hasMessageContaining("does not describe the stored run");
    }

    private Fixture fixture() throws Exception {
        var artifacts = new FileSystemArtifactRepository(temp);
        var input = artifacts.save(new byte[]{1}, "application/x-protobuf", false);
        var output = artifacts.save(new byte[]{2}, "application/x-protobuf", false);
        var run = RunEvidence.newBuilder().setRunId("tested-run").setWorkflowName("tested-workflow")
                .setWorkflowFingerprint("a".repeat(64)).setStatus(RunStatus.RUN_STATUS_SUCCEEDED)
                .setStartedAt(Timestamp.newBuilder().setSeconds(100))
                .setCompletedAt(Timestamp.newBuilder().setSeconds(101))
                .setInputArtifact(input).setOutputArtifact(output)
                .addDependencies(ServiceDependency.newBuilder().setAlias("fixture")
                        .setServiceProfile("fixture").setEndpoint("local")
                        .setDescriptorFingerprint("c".repeat(64))).build();
        var keys = RecordKeys.generate();
        var trust = TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer("authoring-test").addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId("test-key")
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setState(KeyState.KEY_STATE_ACTIVE)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(keys.getPublic()))))).build();
        var issuance = new WorkRecordProjector.Issuance("tested-record", "authoring-test", "test-key",
                Timestamp.newBuilder().setSeconds(102).build(), "");
        var signed = new RecordSigner("test-key", keys.getPrivate())
                .sign(WorkRecordProjector.project(run, issuance));
        var receipt = artifacts.save(signed.toByteArray(), "application/x-protobuf", false);
        return new Fixture(artifacts, run, receipt, trust);
    }

    private record Fixture(ArtifactRepository artifacts, RunEvidence run,
                           ArtifactReference receipt, TrustSnapshot trust) { }
}
