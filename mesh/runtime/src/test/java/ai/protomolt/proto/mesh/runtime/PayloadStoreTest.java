package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.test.RawInput;
import ai.protomolt.proto.mesh.runtime.v1.ChannelPolicy;
import ai.protomolt.proto.mesh.v1.CompletionPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PayloadStoreTest {

    private static final String NAMESPACE = "a2afcaf1-e96f-4519-a20d-49530ac905a5";
    private static final String PROFILE = "payload-test";

    private Clock clock;
    private InMemoryPayloadStore store;
    private ChannelPolicy policy;
    private ai.protomolt.proto.descriptors.DescriptorRegistry descriptors;
    private ai.protomolt.proto.mesh.v1.EntityEnvelope inline;

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(ProcessorChannelFixtures.NOW, ZoneOffset.UTC);
        store = new InMemoryPayloadStore(clock, 10, 1_000_000);
        descriptors = ProcessorChannelFixtures.descriptors();
        policy = ChannelPolicies.localDurable().getPolicy().toBuilder()
                .setInlineByteLimit(1)
                .setPayloadStoreProfile(PROFILE)
                .build();
        inline = EntityEnvelopes.root(UUID.randomUUID().toString(), NAMESPACE,
                RawInput.newBuilder().setText("external payload").build(),
                ProcessorChannelFixtures.NOW,
                ProcessorChannelFixtures.NOW.plusSeconds(600),
                CompletionPolicy.COMPLETION_POLICY_STRICT);
    }

    @Test
    void externalizationAndHydrationPreserveExactProtobufInput() {
        var externalized = new PayloadExternalizer(store).externalize(
                inline, policy, NAMESPACE, "run-owner",
                ProcessorChannelFixtures.NOW.plusSeconds(300));

        assertThat(externalized.envelope().hasClaimCheck()).isTrue();
        assertThat(externalized.envelope().hasPayload()).isFalse();
        assertThat(store.head(externalized.lease().getIdentity()).getActiveLeases())
                .isEqualTo(1);

        var resolved = new PayloadStoreResolver(
                descriptors, store, ignored -> NAMESPACE, PROFILE)
                .resolve(externalized.envelope());
        assertThat(resolved).isEqualTo(RawInput.newBuilder()
                .setText("external payload").build());
    }

    @Test
    void hydrationNamesFourIndependentIdentityAndByteRefusals() {
        var externalized = new PayloadExternalizer(store).externalize(
                inline, policy, NAMESPACE, "run-owner",
                ProcessorChannelFixtures.NOW.plusSeconds(300));
        var envelope = externalized.envelope();
        var resolver = new PayloadStoreResolver(
                descriptors, store, ignored -> NAMESPACE, PROFILE);

        assertThatThrownBy(() -> resolver.resolve(envelope.toBuilder()
                .setClaimCheck(envelope.getClaimCheck().toBuilder()
                        .setPayloadTypeName("other.Message"))
                .build())).hasMessageContaining("payload-type-name-mismatch");
        assertThatThrownBy(() -> resolver.resolve(envelope.toBuilder()
                .setClaimCheck(envelope.getClaimCheck().toBuilder()
                        .setDescriptorFingerprint("0".repeat(64)))
                .build())).hasMessageContaining("payload-descriptor-fingerprint-mismatch");
        assertThatThrownBy(() -> resolver.resolve(envelope.toBuilder()
                .setHeader(envelope.getHeader().toBuilder()
                        .setPayloadLength(envelope.getHeader().getPayloadLength() + 1))
                .build())).hasMessageContaining("payload-length-mismatch");
        assertThatThrownBy(() -> resolver.resolve(envelope.toBuilder()
                .setHeader(envelope.getHeader().toBuilder()
                        .setPayloadDigest("0".repeat(64)))
                .build())).hasMessageContaining("payload-digest-mismatch");
    }

    @Test
    void retentionHoldAndLiveDescendantsBothFencePhysicalPurge() {
        var externalized = new PayloadExternalizer(store).externalize(
                inline, policy, NAMESPACE, "run-owner",
                ProcessorChannelFixtures.NOW.plusSeconds(300));
        var identity = externalized.lease().getIdentity();
        store.markEligible(identity, ProcessorChannelFixtures.NOW, "", "");

        assertThatThrownBy(() -> store.purge(identity, "cleanup",
                ProcessorChannelFixtures.NOW.plusSeconds(1)))
                .hasMessageContaining("payload-live-descendants");
        store.hold(identity, true);
        store.release(identity, externalized.lease().getOwnerId(),
                externalized.lease().getLeaseId());
        assertThatThrownBy(() -> store.purge(identity, "cleanup",
                ProcessorChannelFixtures.NOW.plusSeconds(1)))
                .hasMessageContaining("payload-retention-hold");
        store.hold(identity, false);
        assertThat(store.purge(identity, "cleanup",
                ProcessorChannelFixtures.NOW.plusSeconds(1)).getPurged()).isTrue();
    }

    @Test
    void expiredLeaseCannotBeAcquired() {
        byte[] bytes = RawInput.newBuilder().setText("expired").build().toByteArray();
        var identity = store.put(new PayloadStore.Put(NAMESPACE, PROFILE,
                RawInput.getDescriptor().getFullName(),
                EntityEnvelopes.schemaOf(RawInput.getDefaultInstance())
                        .getDescriptorFingerprint(), bytes, "")).getIdentity();
        assertThatThrownBy(() -> store.acquire(identity, "owner",
                UUID.randomUUID().toString(), Instant.parse("2026-09-03T11:59:59Z")))
                .hasMessageContaining("payload-lease-expiry-invalid");
    }
}
