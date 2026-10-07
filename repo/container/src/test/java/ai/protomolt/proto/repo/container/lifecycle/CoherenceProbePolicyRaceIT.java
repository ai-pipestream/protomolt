package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.PartState;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class CoherenceProbePolicyRaceIT extends AbstractLifecycleIT {

    @Test
    void policyRevocationDuringHeadSurvivesRepairAndNextProbeRepairsFreshRevision() {
        var drive = createDrive("probe-race", "docs", "probe-policy-race", "pfx");
        String key = "pfx/missing-part";
        putObject(drive.bucket, key);
        var row = intakeRow(UUID.randomUUID(), drive.accountId, "race", "ds", drive.name, List.of(key));
        row.writeSecurity(policy(Access.ACCESS_READ));
        documents.save(row);
        var original = documents.findByNodeId(row.nodeId).orElseThrow();
        assertThat(store.delete(drive.bucket, key)).isTrue();

        var injected = new AtomicBoolean();
        // All HEADs reach S3. Change the SQL policy after the probe sampled its
        // row, before the real adapter confirms the missing object.
        BlobStore racingStore = (BlobStore) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] {BlobStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("headObject") && injected.compareAndSet(false, true)) {
                        var latest = documents.findByNodeId(row.nodeId).orElseThrow();
                        latest.writeSecurity(policy(Access.ACCESS_DENY));
                        documents.save(latest);
                    }
                    try {
                        return method.invoke(store, args);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                });

        var report = probe.probe(racingStore, 100);
        assertThat(injected).isTrue();
        assertThat(report.totalMissing()).isEqualTo(1);
        assertThat(report.repairsSkipped()).isEqualTo(1);
        var after = documents.findByNodeId(row.nodeId).orElseThrow();
        assertThat(after.readSecurity()).isEqualTo(policy(Access.ACCESS_DENY));
        assertThat(after.readManifest()).isEqualTo(original.readManifest());
        assertThat(after.mutationRevision).isGreaterThan(original.mutationRevision);

        var retry = probe.probe(store, 100);
        assertThat(retry.totalMissing()).isEqualTo(1);
        assertThat(retry.repairsSkipped()).isZero();
        var repaired = documents.findByNodeId(row.nodeId).orElseThrow();
        assertThat(repaired.readSecurity()).isEqualTo(policy(Access.ACCESS_DENY));
        assertThat(repaired.readManifest().getParts(0).getState()).isEqualTo(PartState.PART_STATE_DELETED);
        assertThat(repaired.mutationRevision).isGreaterThan(after.mutationRevision);
        assertThat(repaired.updatedAt).isEqualTo(original.updatedAt);
    }

    private static DocumentSecurity policy(Access access) {
        return DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                .setIdentityType("public").setIdentity("public").setAccess(access)).build();
    }
}
