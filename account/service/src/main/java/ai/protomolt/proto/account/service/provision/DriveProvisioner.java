package ai.protomolt.proto.account.service.provision;

import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drive provisioning against repo-service: every account gets its two
 * platform drives, {@code intake} ({@code INTAKE}) and {@code pipeline}
 * ({@code PIPELINE}), created through the {@code DriveService.CreateDrive}
 * RPC.
 * <p>
 * This is the ONLY coupling account-service has to repo-service, and it goes
 * through the wire contract (a blocking stub from {@code protomolt-repo-proto})
 * — never repo container/service internals. CreateDrive is idempotent by
 * deterministic drive id, so calling it on account creation AND again on
 * every (re-)activation converges: a drive that already exists is found, not
 * duplicated, and a repo-service that was down at creation heals on the next
 * activation.
 * <p>
 * Provisioning runs OUTSIDE the account store's transactions (never hold a
 * database transaction open across an RPC) and BEFORE the account mutation
 * commits: if repo-service is unreachable the account write never lands, so
 * a retried CreateAccount/ActivateAccount simply provisions again.
 */
public final class DriveProvisioner {

    /** The name of every account's intake drive. */
    public static final String INTAKE_DRIVE = "intake";
    /** The name of every account's pipeline drive. */
    public static final String PIPELINE_DRIVE = "pipeline";

    private static final Logger LOG = LoggerFactory.getLogger(DriveProvisioner.class);

    private final DriveServiceGrpc.DriveServiceBlockingStub drives;

    /**
     * @param drives the repo-service drive stub (blocking; callers run on
     *        virtual threads)
     */
    public DriveProvisioner(DriveServiceGrpc.DriveServiceBlockingStub drives) {
        this.drives = drives;
    }

    /**
     * Ensure the account's intake and pipeline drives exist on repo-service.
     *
     * @param accountId the account to provision for
     * @throws StatusRuntimeException UNAVAILABLE when repo-service cannot be
     *         reached — the caller fails the RPC without committing
     */
    public void ensureAccountDrives(String accountId) {
        createDrive(accountId, INTAKE_DRIVE, DriveType.DRIVE_TYPE_INTAKE);
        createDrive(accountId, PIPELINE_DRIVE, DriveType.DRIVE_TYPE_PIPELINE);
        LOG.debug("account '{}' drives ensured (intake, pipeline)", accountId);
    }

    private void createDrive(String accountId, String name, DriveType type) {
        try {
            drives.createDrive(CreateDriveRequest.newBuilder()
                    .setName(name)
                    .setAccountId(accountId)
                    .setDriveType(type)
                    .build());
        } catch (StatusRuntimeException e) {
            // Repo answers CreateDrive idempotently; anything it returns as an
            // error is a provisioning failure the caller must not commit past.
            throw Status.UNAVAILABLE
                    .withDescription("drive provisioning failed for account '" + accountId
                            + "' (drive '" + name + "'): " + e.getStatus())
                    .withCause(e)
                    .asRuntimeException();
        }
    }
}
