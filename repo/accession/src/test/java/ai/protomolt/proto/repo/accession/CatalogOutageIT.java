package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.stack.BlobStoreStack;
import ai.protomolt.proto.repo.v2.GetOperationOutcomeRequest;
import ai.protomolt.proto.repo.v2.StackRecord;
import io.grpc.Status;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** §15.18: an unreachable grant and catalog authority is UNAVAILABLE, never a recorded or denied outcome. */
class CatalogOutageIT {
    @Test void anUnreachableCatalogIsUnavailableForSavesAndOutcomeLookups() {
        AccessionFixture.start();
        var postgres = new PostgreSQLContainer("postgres:18-alpine");
        postgres.start();
        try (var catalog = new PostgresCatalog(new CatalogConfig(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword(), 2))) {
            var stackId = catalog.registerStack(StackRecord.newBuilder().setStackId("outage").setProvider("s3")
                    .setEndpoint("test").setNamespace(BUCKET + "/outage").build());
            var stack = new BlobStoreStack(stackId, new S3BlobStore(AccessionFixture.s3()), BUCKET, "outage");
            var drives = new DocumentSaves.Drives("default", Map.of("default", stack));
            var saves = new DocumentSaves(catalog, drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC());
            var reads = new DocumentReads(catalog, drives, saves.containerSchema());
            var account = "acct-" + UUID.randomUUID();
            var credential = UUID.randomUUID();
            catalog.registerCredential(ISSUER, credential, "crawler");
            var caller = writer(account, "crawler", credential, 1);
            var request = create("op-1", address(account, "doc-1"), document(account, "doc-1", "v1", "crawler")).build();

            postgres.stop();

            assertThatThrownBy(() -> saves.save(caller, request)).isInstanceOfSatisfying(AccessionException.class,
                    failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.UNAVAILABLE);
                        assertThat(failure.detail()).isEmpty();
                    });
            assertThatThrownBy(() -> reads.outcome(caller, GetOperationOutcomeRequest.newBuilder().setAccountId(account)
                    .setOperationId("op-1").build())).isInstanceOfSatisfying(AccessionException.class,
                    failure -> assertThat(failure.code()).isEqualTo(Status.Code.UNAVAILABLE));
        }
    }
}
