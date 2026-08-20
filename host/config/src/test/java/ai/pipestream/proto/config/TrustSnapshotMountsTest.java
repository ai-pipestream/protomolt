package ai.pipestream.proto.config;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.receipt.KeyState;
import ai.pipestream.proto.receipt.SignatureAlgorithm;
import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.TrustedIssuer;
import ai.pipestream.proto.receipt.TrustedKey;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The trust-snapshot mount contract: one subject, the source's version as
 * the mounted version, atomic swaps, a snapshot duplicating an issuer
 * refused without unmounting the previous one, and a document failing the
 * snapshot's own declared rules never mounting.
 */
class TrustSnapshotMountsTest {

    static final class FakeSource implements ConfigSource {
        final Map<String, Fetched> documents = new HashMap<>();

        @Override
        public Optional<Fetched> fetch(String subject) {
            return Optional.ofNullable(documents.get(subject));
        }
    }

    private static TrustSnapshot snapshot(String issuer, String... keyIds) {
        TrustedIssuer.Builder trusted = TrustedIssuer.newBuilder()
                .setIssuer(issuer)
                .addSubjectKinds("workflow-run");
        for (String keyId : keyIds) {
            trusted.addKeys(TrustedKey.newBuilder()
                    .setKeyId(keyId)
                    .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                    .setPublicKey(ByteString.copyFrom(new byte[32]))
                    .setState(KeyState.KEY_STATE_ACTIVE));
        }
        return TrustSnapshot.newBuilder().addIssuers(trusted).build();
    }

    @Test
    void theSnapshotMountsAndSwapsAtomically() {
        FakeSource source = new FakeSource();
        source.documents.put(TrustSnapshotMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", snapshot("records.protomolt.dev", "key-2026").toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            TrustSnapshotMounts mounts = TrustSnapshotMounts.follow(config);
            assertThat(mounts.current()).as("unmounted until a document applies").isEmpty();

            config.refresh();
            TrustSnapshotMounts.Mounted mounted = mounts.current().orElseThrow();
            assertThat(mounted.version()).isEqualTo("v1");
            assertThat(mounted.snapshot().getIssuers(0).getIssuer())
                    .isEqualTo("records.protomolt.dev");

            source.documents.put(TrustSnapshotMounts.SUBJECT, new ConfigSource.Fetched(
                    "v2", snapshot("records.protomolt.dev", "key-2026", "key-2027")
                            .toByteArray()));
            config.refresh();
            assertThat(mounts.current().orElseThrow().version()).isEqualTo("v2");
            assertThat(mounts.current().orElseThrow().snapshot()
                    .getIssuers(0).getKeysCount()).isEqualTo(2);
        }
    }

    @Test
    void aDuplicateIssuerRefusesWithoutUnmountingThePrevious() {
        FakeSource source = new FakeSource();
        source.documents.put(TrustSnapshotMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", snapshot("records.protomolt.dev", "key-2026").toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            TrustSnapshotMounts mounts = TrustSnapshotMounts.follow(config);
            config.refresh();

            TrustSnapshot duplicate = TrustSnapshot.newBuilder()
                    .addAllIssuers(snapshot("records.protomolt.dev", "key-a")
                            .getIssuersList())
                    .addAllIssuers(snapshot("records.protomolt.dev", "key-b")
                            .getIssuersList())
                    .build();
            source.documents.put(TrustSnapshotMounts.SUBJECT,
                    new ConfigSource.Fetched("v2", duplicate.toByteArray()));
            config.refresh();

            assertThat(mounts.current().orElseThrow().version())
                    .as("the previous snapshot stays live")
                    .isEqualTo("v1");
        }
    }

    @Test
    void aDocumentFailingItsOwnRulesNeverMounts() {
        FakeSource source = new FakeSource();
        source.documents.put(TrustSnapshotMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", TrustSnapshot.getDefaultInstance().toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            TrustSnapshotMounts mounts = TrustSnapshotMounts.follow(config);
            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(mounts.current()).isEmpty();
            assertThat(outcome.refused()).hasSize(1);
        }
    }
}
