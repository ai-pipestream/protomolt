package ai.protomolt.proto.repo.blob.s3;

import java.time.Duration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Test-fixture startup only: RustFS can answer /health before its S3 subsystems are ready. */
final class RustFsReadiness {
    private RustFsReadiness() {}

    static void awaitServing(S3Client client) throws InterruptedException {
        awaitServing(client, Duration.ofSeconds(60));
    }

    static void awaitServing(S3Client client, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            try {
                client.listBuckets();
                return;
            } catch (S3Exception pending) {
                // Match the existing restart wait in RustFsDamagedObjectReadIT.
                // Authentication and other failures remain immediate failures;
                // persistent unavailability retains the provider exception.
                if (pending.statusCode() != 503 || System.nanoTime() >= deadline) throw pending;
                Thread.sleep(250);
            }
        }
    }
}
