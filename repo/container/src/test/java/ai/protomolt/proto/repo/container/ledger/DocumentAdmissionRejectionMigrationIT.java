package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationRejectionCodec;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import java.util.HexFormat;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentAdmissionRejectionMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void preservesPriorReceiptsButRefusesUnboundAdmissionRows(boolean unboundAdmission) {
        try (var c = context(POSTGRES, "75")) {
            var p = prepare(c, 1);
            var receipt = c.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, p.owner());
                long now = ((Number) em.createNativeQuery("SELECT floor(extract(epoch FROM clock_timestamp())*1000000)")
                        .getSingleResult()).longValue();
                var value = DocumentPublicationRejection.newBuilder().setOperationId(p.command().operationId().toString())
                        .setAccountId("account").setPrincipal("principal").setOwnerGeneration(p.owner().generation())
                        .setCommandCodec("document-publication").setCommandEncodingVersion(1).setCommandSha256(p.command().sha256())
                        .setRecordedAtEpochMicros(now).setDispositionValue(2).setReasonValue(3).build();
                var encoded = DocumentPublicationRejectionCodec.encode(p.command(), value, "principal", p.owner().generation());
                // The negative case deliberately inserts an old SQL row with no
                // evidence binding. It is not a legitimate admission decision.
                em.createNativeQuery("""
                        INSERT INTO repository_operation_rejection(account_id,principal,operation_id,owner_generation,
                            command_codec,command_version,command_sha256,result_codec,result_version,result_bytes,result_sha256,
                            recorded_at_epoch_micros,disposition,reason)
                        VALUES('account','principal',:op,:gen,'document-publication',1,decode(:command,'hex'),
                            'document-publication-rejection',1,:bytes,:sha,:now,:disposition,:reason)
                        """).setParameter("op", p.command().operationId()).setParameter("gen", p.owner().generation())
                        .setParameter("command", p.command().sha256()).setParameter("bytes", encoded.bytes().toByteArray())
                        .setParameter("sha", HexFormat.of().parseHex(encoded.sha256())).setParameter("now", now)
                        .setParameter("disposition", unboundAdmission ? 1 : 2).setParameter("reason", unboundAdmission ? 2 : 3).executeUpdate();
                return value;
            });
            Runnable migrate = () -> org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            if (unboundAdmission) {
                assertThatThrownBy(migrate::run).hasStackTraceContaining("repository_rejection_assessment_binding");
                int reason = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT reason FROM repository_operation_rejection")
                        .getSingleResult()).intValue());
                assertThat(reason).isEqualTo(2);
            } else {
                migrate.run();
                assertThat(new DocumentPublicationReplay(c.tx()).observe(new RepositoryCaller("principal", true), p.command()).rejection())
                        .contains(receipt);
            }
        }
    }
}
