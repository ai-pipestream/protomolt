package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordSigner;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import ai.protomolt.proto.workflow.RecordSigning;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.security.KeyPair;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowPreparationSigningTest {
    private static final Instant NOW = Instant.parse("2026-10-01T12:00:00.123456789Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private final KeyPair pair = RecordKeys.generate();
    private final RecordSigning signing = new RecordSigning("issuer", new RecordSigner("key", pair.getPrivate()));

    @Test void configuredPrivateKeyMustMatchTrustedPublicKeyNotJustItsName() {
        assertThatCode(() -> WorkflowPreparationSigning.verify(signing, trust(key()), CLOCK))
                .doesNotThrowAnyException();
        rejected(signing, trust(key().setPublicKey(ByteString.copyFrom(
                RecordKeys.rawPublicKey(RecordKeys.generate().getPublic())))));
    }

    @Test void issuerScopeAndActiveStateAreRequired() {
        rejected(new RecordSigning("other", signing.signer()), trust(key()));
        var wrongKind = trust(key()).toBuilder();
        wrongKind.setIssuers(0, wrongKind.getIssuers(0).toBuilder()
                .clearSubjectKinds().addSubjectKinds("delegation-task"));
        rejected(signing, wrongKind.build());
        rejected(signing, trust(key().setState(KeyState.KEY_STATE_REVOKED)));
        rejected(signing, trust(key().setState(KeyState.KEY_STATE_RETIRED)));
        rejected(null, trust(key()));
        rejected(signing, null);
    }

    @Test void keyWindowUsesTheInjectedTimeWithNanosecondPrecision() {
        rejected(signing, trust(key().setNotBefore(timestamp(NOW.plusNanos(1)))));
        rejected(signing, trust(key().setNotAfter(timestamp(NOW.minusNanos(1)))));
        assertThatCode(() -> WorkflowPreparationSigning.verify(signing,
                trust(key().setNotBefore(timestamp(NOW)).setNotAfter(timestamp(NOW))), CLOCK))
                .doesNotThrowAnyException();
    }

    private TrustedKey.Builder key() {
        return TrustedKey.newBuilder().setKeyId("key")
                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                .setState(KeyState.KEY_STATE_ACTIVE)
                .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(pair.getPublic())));
    }

    private static TrustSnapshot trust(TrustedKey.Builder key) {
        return TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer("issuer").addKeys(key).addSubjectKinds("workflow-run")).build();
    }

    private static Timestamp timestamp(Instant time) {
        return Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
    }

    private static void rejected(RecordSigning signing, TrustSnapshot trust) {
        assertThatThrownBy(() -> WorkflowPreparationSigning.verify(signing, trust, CLOCK))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("workflow preparation signing identity is not trusted");
    }
}
