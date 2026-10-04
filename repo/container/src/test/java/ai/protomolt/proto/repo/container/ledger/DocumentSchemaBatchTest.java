package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaBatchTest {
    @Test void requiresProofsForTypedMembersAndRejectsNullOrUnknownProofEntries() throws Exception {
        var f = command("account", List.of(member("member-a", "doc-a")));
        var policy = policy("account", false, 20);
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(f.command, selection(policy), Map.of(), () -> {}))
                .hasMessageContaining("Required member schema proof is absent");

        var proof = proof(f.command, f.data.get("member-a"), policy, f.assets);
        var permissive = policy("account", true, 20);
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(f.command, selection(permissive), Map.of("unknown", proof), () -> {}))
                .hasMessageContaining("Schema proof names an unknown command member");
        var nullProof = new HashMap<String, DocumentSchemaAdmission.Proof>();
        nullProof.put("member-a", null);
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(f.command, selection(permissive), nullProof, () -> {}))
                .hasMessageContaining("Required member schema proof is absent");
    }

    @Test void rejectsCommandMemberAndPolicyMismatches() throws Exception {
        var base = command("account", List.of(member("member-a", "doc-a")));
        var policy = policy("account", false, 20);
        var proof = proof(base.command, base.data.get("member-a"), policy, base.assets);

        // The proof's member remains byte-for-byte equal, while the complete canonical command gains a member.
        var larger = command("account", List.of(base.data.get("member-a").member, member("member-b", "doc-b")), base.assets);
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(larger.command, selection(policy), Map.of("member-a", proof), () -> {}))
                .hasMessageContaining("Schema proof differs from canonical command member");

        var changedMember = base.data.get("member-a").member.toBuilder().putMetadata("changed", "yes").build();
        var changed = command("account", List.of(changedMember), base.assets);
        var wrongMember = proof(changed.command, base.data.get("member-a"), policy, base.assets);
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(changed.command, selection(policy), Map.of("member-a", wrongMember), () -> {}))
                .hasMessageContaining("Schema proof differs from canonical command member");

        var otherPolicy = policy("account", false, 21);
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(base.command, selection(otherPolicy), Map.of("member-a", proof), () -> {}))
                .hasMessageContaining("admission proof differs from policy snapshot");
    }

    @Test void permitsProofOmissionOnlyForAnExplicitOpaquePolicy() throws Exception {
        var f = command("account", List.of(member("member-a", "doc-a"), member("member-b", "doc-b")));
        var policy = policy("account", true, 20);
        var batch = DocumentSchemaBatch.prepare(f.command, selection(policy), Map.of(), () -> {});
        assertThat(batch.proofs()).isEmpty();
        assertThat(batch.artifacts()).isEmpty();
        assertThat(batch.command()).isEqualTo(f.command);
        assertThat(batch.policy()).isEqualTo(selection(policy));
    }

    @Test void usesTheProofMapSnapshotEvenWhenTheCallerChangesItsMapDuringChecking() throws Exception {
        var f = command("account", List.of(member("member-a", "doc-a")));
        var supplied = new HashMap<String, DocumentSchemaAdmission.Proof>();
        supplied.put("member-a", null);
        var visits = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(f.command, selection(policy("account", true, 20)), supplied, () -> {
            if (visits.incrementAndGet() == 2) supplied.clear();
        })).hasMessageContaining("Required member schema proof is absent");
        assertThat(supplied).isEmpty();
    }

    @Test void rejectsOtherAccountPolicyAndPropagatesCancellation() throws Exception {
        var f = command("account", List.of(member("member-a", "doc-a")));
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(f.command, selection(policy("other", true, 20)), Map.of(), () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Policy account differs");
        var cancelled = new java.util.concurrent.CancellationException("cancel preparation");
        assertThatThrownBy(() -> DocumentSchemaBatch.prepare(f.command, selection(policy("account", true, 20)), Map.of(), () -> {
            throw cancelled;
        })).isSameAs(cancelled);
    }

    @Test void realMemberProofsShareTheCompleteArtifactUnionWithoutLosingProofs() throws Exception {
        var f = command("account", List.of(member("member-a", "doc-a"), member("member-b", "doc-b")));
        var policy = policy("account", false, 20);
        var first = proof(f.command, f.data.get("member-a"), policy, f.assets);
        var second = proof(f.command, f.data.get("member-b"), policy, f.assets);
        assertThat(first.artifacts()).isEqualTo(second.artifacts());
        var batch = DocumentSchemaBatch.prepare(f.command, selection(policy),
                Map.of("member-a", first, "member-b", second), () -> {});
        assertThat(batch.proofs()).containsOnlyKeys("member-a", "member-b");
        assertThat(batch.proofs()).containsEntry("member-a", first).containsEntry("member-b", second);
        assertThat(batch.artifacts()).isEqualTo(first.artifacts());
        assertThat(batch.artifacts()).hasSize(4); // one Document and one payload descriptor/metadata pair, shared by both proofs
        assertThat(batch.artifacts().keySet()).containsAll(first.artifacts().keySet());
    }

    private static DocumentSchemaAdmission.Proof proof(DocumentPublicationCommand command, MemberData member,
            DocumentAdmissionPolicy policy, Assets assets) throws Exception {
        return policy.prepareAndCheck(ByteString.copyFrom(HexFormat.of().parseHex(command.sha256())), member.member,
                member.fragments, assets.container.definition(), selection -> assets.payload.definition(), () -> {});
    }

    private static DocumentSchemaPolicies.Selection selection(DocumentAdmissionPolicy policy) {
        return new DocumentSchemaPolicies.Selection(policy.definition().getAccountId(), 1, policy);
    }

    private static DocumentAdmissionPolicy policy(String account, boolean opaque, int maxFragments) {
        var mode = opaque ? DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED
                : DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED;
        return DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId(account)
                .setValidationProfile("protomolt-retained-schema-admission/v1").setMode(mode)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder()
                        .setMaxFragments(maxFragments).setMaxFragmentBytes(4_000_000).setMaxRoots(100)
                        .setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000)
                        .setMaxDecodedBytes(1_000_000)).build(), () -> {});
    }

    private static CommandData command(String account, List<DocumentPublicationMember> members) throws Exception {
        return command(account, members, assets());
    }

    private static CommandData command(String account, List<DocumentPublicationMember> members, Assets assets) throws Exception {
        var data = new HashMap<String, MemberData>();
        for (var member : members) data.put(member.getMemberId(), memberData(member));
        var intent = DocumentPublicationIntent.newBuilder().setOperationId(UUID.randomUUID().toString())
                .setEncodingVersion(1).setAccountId(account).addAllMembers(members).build();
        return new CommandData(new DocumentPublicationCommand(intent), Map.copyOf(data), assets);
    }

    private static DocumentPublicationMember member(String memberId, String docId) {
        var ownership = OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                .setSecurity(DocumentSecurity.getDefaultInstance()).build();
        var document = Document.newBuilder().setDocId(docId).setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("payload"), "type.test")).build();
        var member = DocumentPublicationMember.newBuilder().setMemberId(memberId)
                .setDriveId(UUID.randomUUID().toString()).setOwnership(ownership)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setDocId(docId).setGraphId("graph").setGraphAddressId("node")));
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            var bytes = ByteString.copyFrom(part.bytes());
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                    .setPart(part.part()).setSubKey(part.subKey())).setUpload(PublicationUpload.newBuilder()
                    .setSizeBytes(bytes.size()).setSha256(sha(bytes)).setContentType("application/protobuf")));
        }
        return member.build();
    }

    private static MemberData memberData(DocumentPublicationMember member) throws Exception {
        var ownership = member.getOwnership();
        var docId = member.getDestination().getAddress().getDocId();
        var document = Document.newBuilder().setDocId(docId).setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("payload"), "type.test")).build();
        var parts = DocumentPartCodec.split(document, PartLayouts.document());
        var fragments = new HashMap<Integer, ByteString>();
        for (int i = 0; i < parts.size(); i++) fragments.put(i, ByteString.copyFrom(parts.get(i).bytes()));
        return new MemberData(member, Map.copyOf(fragments));
    }

    private static Assets assets() throws Exception {
        return new Assets(asset(Document.getDescriptor()), asset(StringValue.getDescriptor()));
    }

    private static Asset asset(com.google.protobuf.Descriptors.Descriptor type) throws Exception {
        var closure = DescriptorFingerprints.closure(type); var descriptors = closure.toByteString(); var descriptorHash = sha(descriptors);
        var schema = PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build();
        var metadata = RepositorySchemaAsset.newBuilder().setSchema(schema).setArtifactSha256(descriptorHash)
                .setTypeUrl("type.test/" + type.getFullName()).setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("synthetic test descriptor; compiler identity unknown")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"))).build();
        return new Asset(new DocumentSchemaAdmission.Definition(metadata, descriptors, Optional.empty()));
    }

    private static String sha(ByteString bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray())); }
        catch (Exception impossible) { throw new AssertionError(impossible); }
    }

    private record Asset(DocumentSchemaAdmission.Definition definition) {}
    private record Assets(Asset container, Asset payload) {}
    private record MemberData(DocumentPublicationMember member, Map<Integer, ByteString> fragments) {}
    private record CommandData(DocumentPublicationCommand command, Map<String, MemberData> data, Assets assets) {}
}
