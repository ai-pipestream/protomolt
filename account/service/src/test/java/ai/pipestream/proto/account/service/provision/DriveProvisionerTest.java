package ai.pipestream.proto.account.service.provision;

import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.CreateDriveResponse;
import ai.pipestream.proto.repo.v1.Drive;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DriveProvisioner} against a fake {@code DriveService} on the gRPC
 * in-process transport — no database, no containers: the contract under test
 * is WHICH CreateDrive calls the provisioner issues (intake then pipeline,
 * typed, account-scoped) and how a repo failure surfaces (always UNAVAILABLE,
 * naming the account and the drive it died on).
 */
class DriveProvisionerTest {

    private Server fakeRepoServer;
    private ManagedChannel channel;

    /** A DriveService that records calls and answers idempotently. */
    private static final class RecordingDriveService extends DriveServiceGrpc.DriveServiceImplBase {
        final List<CreateDriveRequest> calls = new CopyOnWriteArrayList<>();

        @Override
        public void createDrive(CreateDriveRequest request,
                StreamObserver<CreateDriveResponse> responseObserver) {
            calls.add(request);
            responseObserver.onNext(CreateDriveResponse.newBuilder()
                    .setDrive(Drive.newBuilder()
                            .setDriveId("drive-" + request.getAccountId() + "-" + request.getName())
                            .setName(request.getName())
                            .setAccountId(request.getAccountId())
                            .setDriveType(request.getDriveType())
                            .setCreatedAt(Timestamp.getDefaultInstance()))
                    .build());
            responseObserver.onCompleted();
        }
    }

    /** A DriveService that fails when the failingDrive is requested. */
    private static final class FailingDriveService extends DriveServiceGrpc.DriveServiceImplBase {
        final List<String> attempted = new CopyOnWriteArrayList<>();
        private final String failingDrive;
        private final Status failure;

        FailingDriveService(String failingDrive, Status failure) {
            this.failingDrive = failingDrive;
            this.failure = failure;
        }

        @Override
        public void createDrive(CreateDriveRequest request,
                StreamObserver<CreateDriveResponse> responseObserver) {
            attempted.add(request.getName());
            if (request.getName().equals(failingDrive)) {
                responseObserver.onError(failure.asRuntimeException());
                return;
            }
            responseObserver.onNext(CreateDriveResponse.newBuilder()
                    .setDrive(Drive.newBuilder()
                            .setDriveId("drive-" + request.getName())
                            .setName(request.getName())
                            .setAccountId(request.getAccountId())
                            .setDriveType(request.getDriveType())
                            .setCreatedAt(Timestamp.getDefaultInstance()))
                    .build());
            responseObserver.onCompleted();
        }
    }

    private DriveProvisioner provisionerFor(DriveServiceGrpc.DriveServiceImplBase fake,
            String name) throws Exception {
        fakeRepoServer = InProcessServerBuilder.forName(name)
                .addService(fake)
                .directExecutor()
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).build();
        return new DriveProvisioner(DriveServiceGrpc.newBlockingStub(channel));
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (fakeRepoServer != null) {
            fakeRepoServer.shutdownNow();
        }
    }

    @Test
    void ensureProvisionsIntakeThenPipeline() throws Exception {
        RecordingDriveService fake = new RecordingDriveService();
        DriveProvisioner provisioner = provisionerFor(fake, "prov-ok");

        provisioner.ensureAccountDrives("acct-1");

        assertThat(fake.calls).hasSize(2);
        // Intake first, pipeline second: the failure-fast order matters when a
        // repo dies midway (the caller must know what did NOT run).
        assertThat(fake.calls.stream().map(CreateDriveRequest::getName).toList())
                .containsExactly(DriveProvisioner.INTAKE_DRIVE, DriveProvisioner.PIPELINE_DRIVE);
        assertThat(fake.calls).allSatisfy(r -> {
            assertThat(r.getAccountId()).isEqualTo("acct-1");
        });
        assertThat(fake.calls.stream().map(CreateDriveRequest::getDriveType).toList())
                .containsExactly(DriveType.DRIVE_TYPE_INTAKE, DriveType.DRIVE_TYPE_PIPELINE);
    }

    @Test
    void repoFailureOnIntakeStopsBeforePipeline() throws Exception {
        FailingDriveService fake = new FailingDriveService(DriveProvisioner.INTAKE_DRIVE,
                Status.INTERNAL.withDescription("intake store wedged"));
        DriveProvisioner provisioner = provisionerFor(fake, "prov-intake-down");

        assertThatThrownBy(() -> provisioner.ensureAccountDrives("acct-2"))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    // Any repo error becomes UNAVAILABLE: the caller fails the
                    // RPC without committing, whatever repo's own status was.
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
                    assertThat(e.getStatus().getDescription())
                            .contains("acct-2").contains(DriveProvisioner.INTAKE_DRIVE);
                    assertThat(e.getStatus().getCause()).isNotNull();
                });
        assertThat(fake.attempted).containsExactly(DriveProvisioner.INTAKE_DRIVE);
    }

    @Test
    void repoFailureOnPipelineNamesThePipelineDrive() throws Exception {
        FailingDriveService fake = new FailingDriveService(DriveProvisioner.PIPELINE_DRIVE,
                Status.INTERNAL.withDescription("pipeline store wedged"));
        DriveProvisioner provisioner = provisionerFor(fake, "prov-pipeline-down");

        assertThatThrownBy(() -> provisioner.ensureAccountDrives("acct-3"))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
                    assertThat(e.getStatus().getDescription())
                            .contains("acct-3").contains(DriveProvisioner.PIPELINE_DRIVE);
                });
        // Intake landed on repo's side; the failure surfaced only at pipeline.
        assertThat(fake.attempted)
                .containsExactly(DriveProvisioner.INTAKE_DRIVE, DriveProvisioner.PIPELINE_DRIVE);
    }

    @Test
    void evenNotFoundFromRepoBecomesUnavailable() throws Exception {
        // A semantic error (not a transport failure) is still a provisioning
        // failure the caller must not commit past: the status is normalized.
        FailingDriveService fake = new FailingDriveService(DriveProvisioner.INTAKE_DRIVE,
                Status.NOT_FOUND.withDescription("no such drive collection"));
        DriveProvisioner provisioner = provisionerFor(fake, "prov-not-found");

        assertThatThrownBy(() -> provisioner.ensureAccountDrives("acct-4"))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
    }

    @Test
    void unreachableRepoIsUnavailable() throws Exception {
        // No server behind the in-process name at all.
        channel = InProcessChannelBuilder.forName("prov-no-server").build();
        DriveProvisioner provisioner =
                new DriveProvisioner(DriveServiceGrpc.newBlockingStub(channel));

        assertThatThrownBy(() -> provisioner.ensureAccountDrives("acct-5"))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
    }
}
