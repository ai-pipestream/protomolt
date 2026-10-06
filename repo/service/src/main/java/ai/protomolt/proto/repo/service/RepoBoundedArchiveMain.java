package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveStatus;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Standalone authenticated archive-only host with explicit managed storage. */
public final class RepoBoundedArchiveMain {
    private static final Logger LOG = LoggerFactory.getLogger(RepoBoundedArchiveMain.class);
    private RepoBoundedArchiveMain() {}

    public static void main(String[] args) throws Exception {
        var settings = Settings.parse(System.getenv());
        var services = RepoServices.buildBoundedArchive(settings.config, settings.limits);
        Thread hook = null;
        boolean listenerMayHaveAccepted = false;
        try {
            var drive = services.driveRepository().createDrive(new RepositoryCaller("archive-bootstrap", true),
                    CreateDriveRequest.newBuilder().setAccountId(settings.account).setName(settings.drive).build()).getDrive();
            if (drive.getStatus() != DriveStatus.DRIVE_STATUS_ACTIVE)
                throw new IllegalStateException("Archive bootstrap drive must be active");
            if (!drive.getProvider().equals(settings.config.blobStore()))
                throw new IllegalStateException("Archive bootstrap drive does not match selected provider");
            hook = new Thread(() -> drain(() -> services.close(Duration.ofSeconds(10))), "bounded-archive-shutdown");
            Runtime.getRuntime().addShutdownHook(hook);
            listenerMayHaveAccepted = true;
            var server = services.startBoundedArchiveNetty(settings.config.grpcPort(), settings.token);
            // Machine-readable readiness contains no credential or physical storage location.
            System.out.println("PROTOMOLT_ARCHIVE_READY port=" + server.getPort());
            server.awaitTermination();
            drain(() -> services.close(Duration.ofSeconds(10)));
        } catch (Exception | Error failure) {
            if (listenerMayHaveAccepted) cleanupAfterServing(services::close, failure);
            else try { services.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        } finally {
            if (hook != null) {
                try { Runtime.getRuntime().removeShutdownHook(hook); }
                catch (IllegalStateException shutdownInProgress) {
                    // The registered hook owns draining once JVM shutdown has begun.
                    LOG.debug("JVM shutdown owns archive cleanup");
                }
            }
        }
    }

    static void cleanupAfterServing(Runnable close, Throwable failure) {
        // awaitTermination may have been interrupted. Drain accepted work before
        // restoring that signal and propagating the original exceptional exit.
        boolean interrupted = Thread.interrupted() || failure instanceof InterruptedException;
        try { drain(close); }
        catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
        finally { if (interrupted) Thread.currentThread().interrupt(); }
    }

    static void drain(Runnable close) {
        for (;;) {
            try { close.run(); return; }
            catch (RepositoryDrainTimeoutException timeout) {
                LOG.warn("Archive shutdown waiting for {}", timeout.phase());
            }
        }
    }

    static final class Settings {
        final RepoServiceConfig config;
        final BoundedArchiveOptions limits;
        final String token, account, drive;
        private Settings(RepoServiceConfig config, BoundedArchiveOptions limits, String token, String account, String drive) {
            this.config = config; this.limits = limits; this.token = token; this.account = account; this.drive = drive;
        }
        static Settings parse(Map<String, String> snapshot) {
            var env = new RepositoryEnvironment(snapshot);
            String token = required(env, "PROTOMOLT_API_TOKEN");
            String account = required(env, "DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT");
            String drive = required(env, "DOCUMENT_PLATFORM_ARCHIVE_DRIVE");
            int requestLimit = (int) env.number("DOCUMENT_PLATFORM_ARCHIVE_MAX_REQUEST_BYTES", 2_097_152, 1, Integer.MAX_VALUE);
            var limits = new BoundedArchiveOptions(
                    (int) env.number("DOCUMENT_PLATFORM_ARCHIVE_MAX_OBJECT_BYTES", 1_048_576, 1, 9 * 1024 * 1024),
                    requestLimit,
                    (int) env.number("DOCUMENT_PLATFORM_ARCHIVE_MAX_RENDITIONS", 16, 1, Integer.MAX_VALUE),
                    env.number("DOCUMENT_PLATFORM_ARCHIVE_PAYLOAD_BUDGET_BYTES", 14_680_064, 1, Long.MAX_VALUE),
                    (int) env.number("DOCUMENT_PLATFORM_ARCHIVE_MAX_CONCURRENT_REQUESTS", 4, 1, 1024),
                    (int) env.number("DOCUMENT_PLATFORM_ARCHIVE_MAX_RESPONSE_BYTES", requestLimit, 1, Integer.MAX_VALUE),
                    (int) env.number("DOCUMENT_PLATFORM_ARCHIVE_MAX_MANIFEST_BYTES", requestLimit, 1, Integer.MAX_VALUE));
            var config = RepoServiceConfig.fromEnvironment(snapshot);
            // Pure qualification before SQL, provider discovery or client construction.
            limits.profile().openAdmission(config).close();
            return new Settings(config, limits, token, account, drive);
        }
        private static String required(RepositoryEnvironment env, String name) {
            String value = env.text(name, null);
            if (value == null) throw new IllegalArgumentException(name + " is required");
            return value;
        }
    }
}
