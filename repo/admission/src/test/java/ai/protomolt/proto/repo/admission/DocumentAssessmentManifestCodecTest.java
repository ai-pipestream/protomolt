package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import static ai.protomolt.proto.repo.admission.DocumentSchemaPreparationTest.*;
import static org.assertj.core.api.Assertions.*;

class DocumentAssessmentManifestCodecTest {
    @Test void canonicalIdentityIgnoresSetInsertionOrderAndPreservesNanos() throws Exception {
        var input = manifest();
        var reverse = input.toBuilder().clearMembers().addMembers(input.getMembers(1)).addMembers(input.getMembers(0))
                .setRuntime(input.getRuntime().toBuilder().clearImplementationArtifacts()
                        .addImplementationArtifacts(input.getRuntime().getImplementationArtifacts(1))
                        .addImplementationArtifacts(input.getRuntime().getImplementationArtifacts(0))).build();
        var budget = new Reservations();
        try (var one = DocumentAssessmentManifestCodec.encode(input, budget, () -> {});
             var two = DocumentAssessmentManifestCodec.encode(reverse, budget, () -> {})) {
            assertThat(one.bytes()).isEqualTo(two.bytes()); assertThat(one.sha256()).isEqualTo(two.sha256());
            long live = budget.live;
            var decoded = decode(one.bytes(), one.sha256(), budget);
            assertThat(decoded.getMembersList()).extracting(DocumentMemberAssessment::getMemberId).containsExactly("a", "b");
            assertThat(decoded.getEvaluatedAt().getNanos()).isEqualTo(123456789);
            assertThat(budget.live).isEqualTo(live);
        }
        assertThat(budget.live).isZero();
    }
    @Test void duplicateMembersSchemasRootsAndRuntimeNamesAreRejected() throws Exception {
        var input = manifest(); var member = input.getMembers(0); var typed = member.getTyped();
        var malformed = List.of(input.toBuilder().addMembers(member).build(),
                input.toBuilder().setMembers(0, member.toBuilder().setTyped(typed.toBuilder().addPayloadSchemas(typed.getContainer()))).build(),
                input.toBuilder().setMembers(0, member.toBuilder().setTyped(typed.toBuilder()
                        .addPayloadSchemas(typed.getPayloadSchemas(0).toBuilder().setMetadataSha256("0".repeat(64))))).build(),
                input.toBuilder().setMembers(0, member.toBuilder().setTyped(typed.toBuilder().addRoots(typed.getRoots(0)))).build(),
                input.toBuilder().setRuntime(input.getRuntime().toBuilder().addImplementationArtifacts(input.getRuntime().getImplementationArtifacts(0))).build());
        for (var value : malformed) {
            var budget = new Reservations();
            assertThatThrownBy(() -> DocumentAssessmentManifestCodec.encode(value, budget, () -> {}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate");
            assertThat(budget.live).isZero();
        }
    }
    @Test void schemaAndRootSetOrderIsCanonicalAndDoesNotDropIdentities() throws Exception {
        var input = manifest(); var member = input.getMembers(0);
        var a = member.getTyped().getPayloadSchemas(0);
        var b = a.toBuilder().setTypeUrl("type.test/z.Payload").build();
        var root = member.getTyped().getRoots(0);
        var later = root.toBuilder().setOrdinal(1).build();
        var typed = member.getTyped().toBuilder().clearPayloadSchemas().addPayloadSchemas(b).addPayloadSchemas(a)
                .clearRoots().addRoots(later).addRoots(root);
        var budget = new Reservations();
        try (var encoded = DocumentAssessmentManifestCodec.encode(input.toBuilder().setMembers(0, member.toBuilder().setTyped(typed)).build(), budget, () -> {})) {
            var result = decode(encoded.bytes(), encoded.sha256(), budget).getMembers(0).getTyped();
            assertThat(result.getPayloadSchemasList()).containsExactly(a, b);
            assertThat(result.getRootsList()).containsExactly(root, later);
        }
        assertThat(budget.live).isZero();
    }
    @Test void rejectsUnknownFieldsAndNoncanonicalStoredSetOrder() throws Exception {
        var input = manifest(); var budget = new Reservations();
        var unknown = input.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        assertThatThrownBy(() -> DocumentAssessmentManifestCodec.encode(unknown, budget, () -> {})).hasMessageContaining("unknown");
        var nested = input.toBuilder().setRuntime(input.getRuntime().toBuilder().setJvm(input.getRuntime().getJvm().toBuilder()
                .setUnknownFields(unknown.getUnknownFields()))).build();
        assertThatThrownBy(() -> DocumentAssessmentManifestCodec.encode(nested, budget, () -> {})).hasMessageContaining("unknown");
        var reversed = input.toBuilder().clearMembers().addMembers(input.getMembers(1)).addMembers(input.getMembers(0)).build();
        var encoded = DocumentSchemaEvidenceCodec.encode(reversed, () -> {});
        assertThatThrownBy(() -> decode(encoded.bytes(), encoded.sha256(), budget)).hasMessageContaining("noncanonical assessment manifest ordering");
        assertThat(budget.live).isZero();
    }
    @Test void rejectsCorruptTruncatedAndUnsupportedInputWithoutLeakingScratch() throws Exception {
        var budget = new Reservations();
        try (var encoded = DocumentAssessmentManifestCodec.encode(manifest(), budget, () -> {})) {
            long retained = budget.live;
            assertThatThrownBy(() -> decode(encoded.bytes(), "0".repeat(64), budget)).hasMessageContaining("digest mismatch");
            var truncated = encoded.bytes().substring(0, encoded.bytes().size() - 1);
            var digest = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(truncated.toByteArray()));
            assertThatThrownBy(() -> decode(truncated, digest, budget))
                    .isInstanceOfAny(IllegalArgumentException.class, com.google.protobuf.InvalidProtocolBufferException.class);
            assertThatThrownBy(() -> DocumentAssessmentManifestCodec.decode("wrong", 1, encoded.bytes(), encoded.sha256(), budget, () -> {}))
                    .hasMessageContaining("unsupported");
            assertThatThrownBy(() -> DocumentAssessmentManifestCodec.decode(DocumentAssessmentManifestCodec.CODEC, 2, encoded.bytes(), encoded.sha256(), budget, () -> {}))
                    .hasMessageContaining("unsupported");
            assertThat(budget.live).isEqualTo(retained);
        }
        assertThat(budget.live).isZero();
    }
    @Test void reservationAndCancellationFailuresReleaseCanonicalBuffer() throws Exception {
        var input = manifest(); var budget = new Reservations();
        budget.refuseAt = 1; budget.refusal = new IllegalStateException("no capacity");
        assertThatThrownBy(() -> DocumentAssessmentManifestCodec.encode(input, budget, () -> {})).isSameAs(budget.refusal);
        assertThat(budget.live).isZero();
        var active = new Reservations(); var cancelled = new java.util.concurrent.CancellationException("encoding cancelled");
        assertThatThrownBy(() -> DocumentAssessmentManifestCodec.encode(input, active, () -> {
            if (active.live > 0) throw cancelled;
        })).isSameAs(cancelled);
        assertThat(active.live).isZero();
        var encoded = DocumentAssessmentManifestCodec.encode(input, active, () -> {});
        encoded.close(); encoded.close();
        assertThatThrownBy(encoded::bytes).hasMessageContaining("closed");
        assertThat(active.live).isZero();
    }
    @Test void rejectsOversizeWireInputBeforeDigestAndEnforcesAggregateArtifactBound() throws Exception {
        var budget = new Reservations();
        var oversized = ByteString.copyFrom(new byte[DocumentAssessmentManifestCodec.MAX_BYTES + 1]);
        assertThatThrownBy(() -> decode(oversized, "not-a-digest", budget)).hasMessageContaining("byte bound");
        assertThat(budget.calls).isZero();
        var input = manifest(); var member = input.getMembers(0); var typed = member.getTyped().toBuilder();
        for (int i = 0; i < 32; i++) typed.addPayloadSchemas(typed.getPayloadSchemas(0).toBuilder()
                .setDescriptorSha256(String.format("%064x", 2 * i)).setMetadataSha256(String.format("%064x", 2 * i + 1)));
        assertThatThrownBy(() -> DocumentAssessmentManifestCodec.encode(input.toBuilder()
                .setMembers(0, member.toBuilder().setTyped(typed)).build(), budget, () -> {}))
                .hasMessageContaining("schema artifact bound");
        assertThat(budget.calls).isZero();
        assertThat(budget.live).isZero();
    }
    @Test void decodeReservationRefusalAndCancellationLeaveOriginalEncodedOwnerAlive() throws Exception {
        var owner = new Reservations();
        try (var encoded = DocumentAssessmentManifestCodec.encode(manifest(), owner, () -> {})) {
            var refused = new Reservations(); refused.refuseAt = 1; refused.refusal = new IllegalStateException("decode capacity");
            assertThatThrownBy(() -> decode(encoded.bytes(), encoded.sha256(), refused)).isSameAs(refused.refusal);
            assertThat(refused.live).isZero();
            var scratch = new Reservations(); var cancelled = new java.util.concurrent.CancellationException("decode cancelled");
            assertThatThrownBy(() -> DocumentAssessmentManifestCodec.decode(DocumentAssessmentManifestCodec.CODEC, 1,
                    encoded.bytes(), encoded.sha256(), scratch, () -> { if (scratch.live > 0) throw cancelled; }))
                    .isSameAs(cancelled);
            assertThat(scratch.live).isZero();
            assertThat(owner.live).isEqualTo(encoded.bytes().size());
            assertThat(decode(encoded.bytes(), encoded.sha256(), scratch).getMembersCount()).isEqualTo(2);
        }
        assertThat(owner.live).isZero();
    }
    @Test void aggregateWireBoundAppliesBeforeLargeRootListsAreSorted() throws Exception {
        var input = manifest(); var typed = input.getMembers(0).getTyped().toBuilder().clearRoots();
        for (int i = 0; i < 1024; i++) typed.addRoots(input.getMembers(0).getTyped().getRoots(0).toBuilder().setOrdinal(i));
        var large = input.toBuilder().clearMembers();
        for (int i = 0; i < 5; i++) large.addMembers(DocumentMemberAssessment.newBuilder().setMemberId("member-" + i).setTyped(typed));
        var budget = new Reservations();
        assertThatThrownBy(() -> DocumentAssessmentManifestCodec.encode(large.build(), budget, () -> {}))
                .hasMessageContaining("wire value bound");
        assertThat(budget.calls).isZero();
    }
    private static DocumentPublicationAssessmentManifest decode(ByteString bytes, String digest, Reservations budget) throws Exception {
        return DocumentAssessmentManifestCodec.decode(DocumentAssessmentManifestCodec.CODEC, 1, bytes, digest, budget, () -> {});
    }
    private static DocumentPublicationAssessmentManifest manifest() throws Exception {
        var f = fixture(false);
        var typed = DocumentTypedAssessment.newBuilder().setContainer(f.container().reference().toProto())
                .addPayloadSchemas(f.string().reference().toProto())
                .addRoots(DocumentAssessmentRootReference.newBuilder().setOrdinal(0).setCodec("document-root-schema-evidence")
                        .setVersion(1).setSha256("c".repeat(64)));
        var tool = SchemaToolIdentity.newBuilder().setName("a-test-artifact").setVersion("fixture").setArtifactSha256("d".repeat(64));
        return DocumentPublicationAssessmentManifest.newBuilder().setEncodingVersion(1)
                .setOperationId("abcdefab-cdef-4abc-8def-abcdefabcdef").setAccountId("account").setPrincipal("principal").setOwnerGeneration(1)
                .setCommandCodec("document-publication").setCommandVersion(1).setCommandSha256("a".repeat(64))
                .setPolicyRevision(1).setPolicySha256("b".repeat(64))
                .setEvaluatedAt(DocumentAssessmentInstant.newBuilder().setEpochSeconds(946684800).setNanos(123456789))
                .setRuntime(DocumentAssessmentRuntime.newBuilder().setValidationProfile(DocumentSchemaAdmission.PROFILE)
                        .setCatalogConfiguration("empty-taxonomy-and-postal/v1").addImplementationArtifacts(tool)
                        .addImplementationArtifacts(tool.clone().setName("z-test-artifact"))
                        .setJvm(SchemaToolIdentity.newBuilder().setName("synthetic-jvm").setVersion("fixture")))
                .addMembers(DocumentMemberAssessment.newBuilder().setMemberId("a").setTyped(typed))
                .addMembers(DocumentMemberAssessment.newBuilder().setMemberId("b").setOpaque(true)).build();
    }
}
