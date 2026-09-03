package ai.protomolt.proto.config;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The access-policy mount contract: one subject, the source's version as the mounted
 * version, atomic swaps, and a document duplicating a principal refused without
 * unmounting the previous policy.
 */
class AccessPolicyMountsTest {

    static final class FakeSource implements ConfigSource {
        final Map<String, Fetched> documents = new HashMap<>();

        @Override
        public Optional<Fetched> fetch(String subject) {
            return Optional.ofNullable(documents.get(subject));
        }
    }

    private static AccessPolicy policy(String name, String credential, String... scopes) {
        Principal.Builder principal = Principal.newBuilder()
                .setName(name)
                .addCredentialSha256(AccessPolicyCallers.sha256Hex(credential));
        for (String scope : scopes) {
            principal.addScopes(scope);
        }
        return AccessPolicy.newBuilder().addPrincipals(principal).build();
    }

    @Test
    void thePolicyMountsAndSwapsAtomically() {
        FakeSource source = new FakeSource();
        source.documents.put(AccessPolicyMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", policy("ci-reader", "reader-token", "schema-read").toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            AccessPolicyMounts mounts = AccessPolicyMounts.follow(config);
            assertThat(mounts.current()).as("unmounted until a document applies").isEmpty();

            config.refresh();
            AccessPolicyMounts.Mounted mounted = mounts.current().orElseThrow();
            assertThat(mounted.version()).isEqualTo("v1");
            assertThat(mounted.policy().getPrincipals(0).getName()).isEqualTo("ci-reader");

            source.documents.put(AccessPolicyMounts.SUBJECT, new ConfigSource.Fetched(
                    "v2", policy("ci-reader", "reader-token", "schema-read", "schema-write")
                            .toByteArray()));
            config.refresh();
            assertThat(mounts.current().orElseThrow().version()).isEqualTo("v2");
            assertThat(mounts.current().orElseThrow().policy()
                    .getPrincipals(0).getScopesCount()).isEqualTo(2);
        }
    }

    @Test
    void aRefusedDocumentKeepsThePreviousPolicyLive() {
        FakeSource source = new FakeSource();
        source.documents.put(AccessPolicyMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", policy("ci-reader", "reader-token", "schema-read").toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            AccessPolicyMounts mounts = AccessPolicyMounts.follow(config);
            config.refresh();

            AccessPolicy duplicate = AccessPolicy.newBuilder()
                    .addAllPrincipals(policy("x", "a", "schema-read").getPrincipalsList())
                    .addAllPrincipals(policy("x", "b", "schema-write").getPrincipalsList())
                    .build();
            source.documents.put(AccessPolicyMounts.SUBJECT,
                    new ConfigSource.Fetched("v2", duplicate.toByteArray()));
            config.refresh();

            assertThat(mounts.current().orElseThrow().version())
                    .as("the previous policy stays live")
                    .isEqualTo("v1");
        }
    }

    @Test
    void aDocumentFailingItsOwnRulesNeverMounts() {
        FakeSource source = new FakeSource();
        source.documents.put(AccessPolicyMounts.SUBJECT, new ConfigSource.Fetched(
                "v1", AccessPolicy.getDefaultInstance().toByteArray()));
        try (DistributedConfig config = DistributedConfig.over(source)) {
            AccessPolicyMounts mounts = AccessPolicyMounts.follow(config);
            DistributedConfig.RefreshOutcome outcome = config.refresh();
            assertThat(mounts.current()).isEmpty();
            assertThat(outcome.refused()).hasSize(1);
        }
    }
}
