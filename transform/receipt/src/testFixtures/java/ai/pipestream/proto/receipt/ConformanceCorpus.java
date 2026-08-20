package ai.pipestream.proto.receipt;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.HexFormat;
import java.util.List;

/**
 * The conformance corpus: deterministically generated valid and invalid
 * records, each invalid fixture failing exactly one named check. The
 * fixtures are byte-stable — keys are fixed RFC 8032 seeds, timestamps
 * are constants, Ed25519 is deterministic — and the freeze test pins
 * their digests. An external verifier claims conformance by passing this
 * corpus against {@link #trust()}.
 */
public final class ConformanceCorpus {

    /** RFC 8032 section 7.1 TEST 1 seed; the corpus issuer key. */
    public static final byte[] SEED = HexFormat.of().parseHex(
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
    /** RFC 8032 section 7.1 TEST 1 public key. */
    public static final byte[] PUBLIC_KEY = HexFormat.of().parseHex(
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
    /** RFC 8032 section 7.1 TEST 2 seed; the second key. */
    public static final byte[] SECOND_SEED = HexFormat.of().parseHex(
            "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb");
    /** RFC 8032 section 7.1 TEST 2 public key. */
    public static final byte[] SECOND_PUBLIC_KEY = HexFormat.of().parseHex(
            "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c");

    public static final String ISSUER = "records.protomolt.dev";
    public static final String KEY_ID = "key-2026";
    public static final String SECOND_KEY_ID = "key-second";
    public static final String REVOKED_KEY_ID = "key-revoked";
    public static final String EXPIRED_KEY_ID = "key-expired";

    private static final Timestamp ISSUED_AT = seconds(1750000000);
    private static final PrivateKey PRIVATE_KEY = RecordKeys.privateKey(SEED);
    private static final PrivateKey SECOND_PRIVATE_KEY = RecordKeys.privateKey(SECOND_SEED);

    private ConformanceCorpus() {
    }

    /** One corpus record: a name, the record bytes, and the expected refusal. */
    public record Fixture(String name, byte[] record, String failsAt) {
        public boolean valid() {
            return failsAt == null;
        }
    }

    /** The snapshot every corpus fixture verifies against. */
    public static TrustSnapshot trust() {
        return TrustSnapshot.newBuilder()
                .addIssuers(TrustedIssuer.newBuilder()
                        .setIssuer(ISSUER)
                        .addKeys(key(KEY_ID, PUBLIC_KEY, KeyState.KEY_STATE_ACTIVE, null))
                        .addKeys(key(SECOND_KEY_ID, SECOND_PUBLIC_KEY,
                                KeyState.KEY_STATE_ACTIVE, null))
                        .addKeys(key(REVOKED_KEY_ID, PUBLIC_KEY,
                                KeyState.KEY_STATE_REVOKED, null))
                        .addKeys(key(EXPIRED_KEY_ID, PUBLIC_KEY,
                                KeyState.KEY_STATE_RETIRED, seconds(1700000000)))
                        .addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN))
                .build();
    }

    public static List<Fixture> fixtures() {
        WorkRecord complete = manifest().build();
        byte[] canonical = WorkRecords.canonicalBytes(complete);
        return List.of(
                new Fixture("valid-complete", sign(complete, KEY_ID, PRIVATE_KEY), null),
                new Fixture("valid-partial", sign(manifest()
                        .setRecordId("record-partial")
                        .setCompleteness(Completeness.newBuilder()
                                .setStatus(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL)
                                .addMissingReasons("the run predates response recording")
                                .setPolicyId("evidence-baseline")
                                .setPolicyVersion("1")
                                .setPolicySha256(WorkRecords.sha256Hex("policy".getBytes())))
                        .build(), KEY_ID, PRIVATE_KEY), null),
                new Fixture("valid-revision-disclosure", sign(manifest()
                        .setRecordId("record-revised")
                        .setPriorManifestSha256(WorkRecords.sha256Hex(canonical))
                        .setDisclosure(Disclosure.newBuilder()
                                .setSourceManifestSha256(
                                        WorkRecords.sha256Hex("source".getBytes()))
                                .setPolicy("mask pii, secret"))
                        .build(), KEY_ID, PRIVATE_KEY), null),
                new Fixture("empty-record", new byte[0],
                        RecordVerifier.CHECK_CONTAINER_BOUNDS),
                new Fixture("garbage-record", new byte[] {(byte) 0xff, 0x01, 0x02},
                        RecordVerifier.CHECK_CONTAINER_BOUNDS),
                new Fixture("no-signatures", SignedWorkRecord.newBuilder()
                        .setManifest(ByteString.copyFrom(canonical))
                        .build().toByteArray(),
                        RecordVerifier.CHECK_CONTAINER_BOUNDS),
                new Fixture("foreign-algorithm", container(canonical)
                        .setSignatures(0, container(canonical).getSignatures(0).toBuilder()
                                .setAlgorithmValue(2))
                        .build().toByteArray(),
                        RecordVerifier.CHECK_CONTAINER_BOUNDS),
                new Fixture("duplicate-signature", container(canonical)
                        .addSignatures(container(canonical).getSignatures(0))
                        .build().toByteArray(),
                        RecordVerifier.CHECK_CONTAINER_BOUNDS),
                new Fixture("unknown-container-field", container(canonical)
                        .setUnknownFields(unknownField())
                        .build().toByteArray(),
                        RecordVerifier.CHECK_CONTAINER_BOUNDS),
                new Fixture("unknown-manifest-field", signBytes(
                        complete.toBuilder().setUnknownFields(unknownField())
                                .build().toByteArray(), KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_MANIFEST_PARSE),
                new Fixture("unknown-manifest-version", sign(
                        manifest().setManifestVersion(2).build(), KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_MANIFEST_PARSE),
                new Fixture("blank-issuer", sign(
                        manifest().clearIssuer().build(), KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_MANIFEST_PARSE),
                new Fixture("malformed-prior-link", sign(manifest()
                        .setPriorManifestSha256("not-a-digest").build(),
                        KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_MANIFEST_PARSE),
                new Fixture("non-canonical-manifest", signBytes(
                        appendDuplicateVersionField(canonical), KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_RESERIALIZATION_EQUALITY),
                new Fixture("unknown-issuer", sign(
                        manifest().setIssuer("nobody.example").build(), KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_KEY_TRUSTED),
                new Fixture("unknown-key", sign(
                        manifest().setKeyId("key-unknown").build(),
                        "key-unknown", PRIVATE_KEY),
                        RecordVerifier.CHECK_KEY_TRUSTED),
                new Fixture("revoked-key", sign(
                        manifest().setKeyId(REVOKED_KEY_ID).build(),
                        REVOKED_KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_KEY_TRUSTED),
                new Fixture("expired-key", sign(
                        manifest().setKeyId(EXPIRED_KEY_ID).build(),
                        EXPIRED_KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_KEY_TRUSTED),
                new Fixture("unsigned-manifest-key", signBytes(
                        canonical, SECOND_KEY_ID, SECOND_PRIVATE_KEY),
                        RecordVerifier.CHECK_KEY_TRUSTED),
                new Fixture("forged-signature", tamperSignature(canonical),
                        RecordVerifier.CHECK_SIGNATURE_VALID),
                new Fixture("unauthorized-kind", sign(manifest()
                        .setSubject(subject().setKind("delegation-run"))
                        .build(), KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_ISSUER_AUTHORIZED),
                new Fixture("complete-with-reasons", sign(manifest()
                        .setCompleteness(manifest().getCompleteness().toBuilder()
                                .addMissingReasons("a reason on a complete record"))
                        .build(), KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_COMPLETENESS_CONSISTENT),
                new Fixture("partial-without-reasons", sign(manifest()
                        .setCompleteness(manifest().getCompleteness().toBuilder()
                                .setStatus(CompletenessStatus.COMPLETENESS_STATUS_PARTIAL))
                        .build(), KEY_ID, PRIVATE_KEY),
                        RecordVerifier.CHECK_COMPLETENESS_CONSISTENT));
    }

    /** The baseline complete manifest every fixture derives from. */
    public static WorkRecord.Builder manifest() {
        return WorkRecord.newBuilder()
                .setManifestVersion(WorkRecords.MANIFEST_VERSION)
                .setRecordId("record-1")
                .setIssuer(ISSUER)
                .setKeyId(KEY_ID)
                .setIssuedAt(ISSUED_AT)
                .setSubject(subject())
                .addSteps(RecordStep.newBuilder()
                        .setName("tokenize")
                        .setMethod("workbench.test.Tokenizer/Tokenize")
                        .setOutcome(StepOutcome.STEP_OUTCOME_SUCCEEDED)
                        .setStartedAt(seconds(1749999990))
                        .setCompletedAt(seconds(1749999991))
                        .setRequestArtifact(artifact("request".getBytes()))
                        .setResponseArtifact(artifact("response".getBytes())))
                .addSteps(RecordStep.newBuilder()
                        .setName("summarize")
                        .setOutcome(StepOutcome.STEP_OUTCOME_SUCCEEDED)
                        .setStartedAt(seconds(1749999992))
                        .setCompletedAt(seconds(1749999995))
                        .setPromptTokens(120)
                        .setCompletionTokens(40)
                        .setModel("claude-sonnet-5")
                        .setModelVersion("20260501"))
                .addArtifacts(artifact("output".getBytes()))
                .setCompleteness(Completeness.newBuilder()
                        .setStatus(CompletenessStatus.COMPLETENESS_STATUS_COMPLETE)
                        .setPolicyId("evidence-baseline")
                        .setPolicyVersion("1")
                        .setPolicySha256(WorkRecords.sha256Hex("policy".getBytes())));
    }

    public static RecordSubject.Builder subject() {
        return RecordSubject.newBuilder()
                .setKind(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .setWorkflowName("embed-pipeline")
                .setWorkflowVersion("v1")
                .setWorkflowFingerprint(WorkRecords.sha256Hex("workflow".getBytes()))
                .setRunId("run-1");
    }

    public static RecordArtifact.Builder artifact(byte[] content) {
        return RecordArtifact.newBuilder()
                .setSha256(WorkRecords.sha256Hex(content))
                .setMediaType("application/x-protobuf")
                .setSizeBytes(content.length)
                .setRedacted(true);
    }

    public static byte[] sign(WorkRecord manifest, String keyId, PrivateKey key) {
        return signBytes(WorkRecords.canonicalBytes(manifest), keyId, key);
    }

    public static byte[] signBytes(byte[] manifestBytes, String keyId, PrivateKey key) {
        return SignedWorkRecord.newBuilder()
                .setManifest(ByteString.copyFrom(manifestBytes))
                .addSignatures(RecordSignature.newBuilder()
                        .setKeyId(keyId)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setSignature(ByteString.copyFrom(rawSign(manifestBytes, key))))
                .build().toByteArray();
    }

    private static SignedWorkRecord.Builder container(byte[] manifestBytes) {
        return SignedWorkRecord.newBuilder()
                .setManifest(ByteString.copyFrom(manifestBytes))
                .addSignatures(RecordSignature.newBuilder()
                        .setKeyId(KEY_ID)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setSignature(ByteString.copyFrom(
                                rawSign(manifestBytes, PRIVATE_KEY))));
    }

    private static byte[] tamperSignature(byte[] canonical) {
        byte[] signature = rawSign(canonical, PRIVATE_KEY);
        signature[0] ^= 0x01;
        return SignedWorkRecord.newBuilder()
                .setManifest(ByteString.copyFrom(canonical))
                .addSignatures(RecordSignature.newBuilder()
                        .setKeyId(KEY_ID)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setSignature(ByteString.copyFrom(signature)))
                .build().toByteArray();
    }

    private static byte[] rawSign(byte[] bytes, PrivateKey key) {
        try {
            Signature signature = Signature.getInstance("Ed25519");
            signature.initSign(key);
            signature.update(bytes);
            return signature.sign();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] appendDuplicateVersionField(byte[] canonical) {
        byte[] bytes = new byte[canonical.length + 2];
        System.arraycopy(canonical, 0, bytes, 0, canonical.length);
        bytes[canonical.length] = 0x08;
        bytes[canonical.length + 1] = 0x01;
        return bytes;
    }

    private static UnknownFieldSet unknownField() {
        return UnknownFieldSet.newBuilder()
                .addField(999, UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                .build();
    }

    private static TrustedKey.Builder key(String keyId, byte[] publicKey, KeyState state,
                                          Timestamp notAfter) {
        TrustedKey.Builder key = TrustedKey.newBuilder()
                .setKeyId(keyId)
                .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                .setPublicKey(ByteString.copyFrom(publicKey))
                .setState(state);
        if (notAfter != null) {
            key.setNotAfter(notAfter);
        }
        return key;
    }

    private static Timestamp seconds(long seconds) {
        return Timestamp.newBuilder().setSeconds(seconds).build();
    }
}
