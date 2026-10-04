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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaManifestTest {
    private static final String ACCOUNT = "account";
    @Test void typedManifestContainsExactPartsArtifactUnionAssetsAndEvidence() throws Exception {
        var f = fixture(false, true);
        var physicalParts = bound(f, null);
        var manifest = DocumentSchemaManifest.prepare(f.batch, "member", physicalParts, () -> {});
        assertThat(manifest.decision()).isEqualTo("TYPED");
        Struct json = parse(manifest.json());
        assertThat(text(json, "version")).isEqualTo("1");
        var serializedParts = list(json, "parts").getValuesList();
        assertThat(serializedParts).hasSize(f.member.getPartsCount());
        for (int ordinal = 0; ordinal < f.member.getPartsCount(); ordinal++) {
            Struct part = serializedParts.get(ordinal).getStructValue();
            var declaration = f.member.getParts(ordinal);
            var physical = physicalParts.parts().get(new DocumentCommitParts.Slot("member", ordinal));
            assertThat(text(part, "ordinal")).isEqualTo(Integer.toString(ordinal));
            assertThat(text(part, "part")).isEqualTo(Integer.toString(declaration.getSlot().getPartValue()));
            assertThat(text(part, "sub_key")).isEqualTo(declaration.getSlot().getSubKey());
            assertThat(text(part, "object_id")).isEqualTo(physical.id().toString());
            assertThat(text(part, "size")).isEqualTo(Long.toString(physical.size()));
            assertThat(text(part, "sha256")).isEqualTo(physical.sha256());
        }
        assertThat(list(json, "artifacts").getValuesCount()).isEqualTo(f.proof.artifacts().size());
        assertThat(list(json, "assets").getValuesCount()).isEqualTo(f.proof.references().size());
        assertThat(list(json, "roots").getValuesCount()).isEqualTo(f.proof.roots().size());
        var artifactDigests = list(json, "artifacts").getValuesList().stream().map(Value::getStringValue).toList();
        assertThat(artifactDigests).containsExactlyElementsOf(f.proof.artifacts().keySet().stream().sorted().toList());
        for (int i = 0; i < f.proof.roots().size(); i++) {
            var expected = f.proof.roots().get(i);
            Struct actual = list(json, "roots").getValues(i).getStructValue();
            assertThat(text(actual, "ordinal")).isEqualTo(Integer.toString(expected.ordinal()));
            assertThat(text(actual, "locator_sha256")).isEqualTo(expected.locatorSha256());
            assertThat(text(actual, "fragment_sha256")).isEqualTo(f.member.getParts(expected.ordinal()).getUpload().getSha256());
            assertThat(text(actual, "fragment_size")).isEqualTo(Long.toString(f.member.getParts(expected.ordinal()).getUpload().getSizeBytes()));
            assertThat(text(actual, "evidence_codec")).isEqualTo(expected.encoded().codec());
            assertThat(text(actual, "evidence_version")).isEqualTo(Integer.toString(expected.encoded().version()));
            assertThat(text(actual, "evidence_sha256")).isEqualTo(expected.encoded().sha256());
        }
    }

    @Test void opaqueManifestKeepsPhysicalPartsButHasNoSchemaClaims() throws Exception {
        var f = fixture(true, false);
        var manifest = DocumentSchemaManifest.prepare(f.batch, "member", bound(f, null), () -> {});
        assertThat(manifest.decision()).isEqualTo("OPAQUE");
        Struct json = parse(manifest.json());
        assertThat(list(json, "parts").getValuesCount()).isEqualTo(f.member.getPartsCount());
        assertThat(list(json, "artifacts").getValuesCount()).isZero();
        assertThat(list(json, "assets").getValuesCount()).isZero();
        assertThat(list(json, "roots").getValuesCount()).isZero();
    }

    @Test void rejectsMissingExtraAndMismatchedPhysicalSlotsAndReuseIdentity() throws Exception {
        var f = fixture(false, true);
        var complete = bound(f, null);
        var missing = new HashMap<>(complete.parts()); missing.remove(new DocumentCommitParts.Slot("member", 0));
        assertThatThrownBy(() -> DocumentSchemaManifest.prepare(f.batch, "member",
                new DocumentCommitParts.Bound(missing, complete.selections()), () -> {}))
                .hasMessageContaining("physical part differs");

        var changed = new HashMap<>(complete.parts());
        var key = new DocumentCommitParts.Slot("member", 0); var value = changed.get(key);
        changed.put(key, new DocumentCommitParts.Physical(value.id(), value.part(), value.subKey(), value.key(),
                value.size(), "f".repeat(64), value.contentType(), value.version(), value.etag(), value.generation(), value.realm(), value.namespace()));
        assertThatThrownBy(() -> DocumentSchemaManifest.prepare(f.batch, "member",
                new DocumentCommitParts.Bound(changed, complete.selections()), () -> {}))
                .hasMessageContaining("physical part differs");

        var withExtra = new HashMap<>(complete.parts());
        withExtra.put(new DocumentCommitParts.Slot("member", 99), value);
        assertThatThrownBy(() -> DocumentSchemaManifest.prepare(f.batch, "member",
                new DocumentCommitParts.Bound(withExtra, complete.selections()), () -> {}))
                .hasMessageContaining("physical part set differs");

        var reused = fixture(false, true, true);
        var validReuseManifest = DocumentSchemaManifest.prepare(reused.batch, "member", bound(reused, null), () -> {});
        assertThat(text(list(parse(validReuseManifest.json()), "parts").getValues(0).getStructValue(), "object_id"))
                .isEqualTo(REUSE_ID.toString());
        var wrongId = bound(reused, UUID.randomUUID());
        assertThatThrownBy(() -> DocumentSchemaManifest.prepare(reused.batch, "member", wrongId, () -> {}))
                .hasMessageContaining("physical part differs");
    }

    @Test void rejectsUnknownMembersAndPropagatesCancellation() throws Exception {
        var f = fixture(false, true);
        var bound = bound(f, null);
        assertThatThrownBy(() -> DocumentSchemaManifest.prepare(f.batch, "missing", bound, () -> {}))
                .hasMessageContaining("Unknown schema manifest member");
        var visits = new java.util.concurrent.atomic.AtomicInteger();
        var cancelled = new CancellationException("manifest cancelled");
        assertThatThrownBy(() -> DocumentSchemaManifest.prepare(f.batch, "member", bound, () -> {
            if (visits.incrementAndGet() == 3) throw cancelled;
        })).isSameAs(cancelled);
        assertThat(visits.get()).isEqualTo(3);
    }

    private static Fixture fixture(boolean opaque, boolean withProof) throws Exception {
        return fixture(opaque, withProof, false);
    }

    private static Fixture fixture(boolean opaque, boolean withProof, boolean reuseCore) throws Exception {
        var ownership = OwnershipContext.newBuilder().setAccountId(ACCOUNT).setDatasourceId("source")
                .setSecurity(DocumentSecurity.getDefaultInstance()).build();
        var document = Document.newBuilder().setDocId("manifest-doc").setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("manifest payload"), "type.test")).build();
        var address = NodeAddress.newBuilder().setAccountId(ACCOUNT).setDocId(document.getDocId())
                .setGraphId("graph").setGraphAddressId("node").build();
        var parts = DocumentPartCodec.split(document, PartLayouts.document());
        var member = DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(UUID.randomUUID().toString())
                .setOwnership(ownership).setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(address));
        var fragments = new HashMap<Integer, ByteString>();
        for (int ordinal = 0; ordinal < parts.size(); ordinal++) {
            var part = parts.get(ordinal); var bytes = ByteString.copyFrom(part.bytes()); fragments.put(ordinal, bytes);
            var declaration = DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                    .setPart(part.part()).setSubKey(part.subKey()));
            if (reuseCore && ordinal == 0) {
                var condition = DocumentRevisionCondition.newBuilder().setAddress(address.toBuilder().setDocId("source-doc").setGraphAddressId("source-node")).setExpectedMutationRevision(1).build();
                declaration.setReuse(PublicationReuse.newBuilder().setSource(condition)
                        .setSourceSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                        .setObject(PublicationObjectIdentity.newBuilder().setObjectId(REUSE_ID.toString())
                                .setBackendGeneration("generation").setStorageRealm("realm").setNamespace("namespace")
                                .setObjectKey("object/core").setSizeBytes(bytes.size()).setSha256(sha(bytes))
                                .setContentType("application/protobuf")));
            } else declaration.setUpload(PublicationUpload.newBuilder().setSizeBytes(bytes.size()).setSha256(sha(bytes))
                    .setContentType("application/protobuf"));
            member.addParts(declaration);
        }
        var memberValue = member.build();
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1)
                .setAccountId(ACCOUNT).setOperationId(UUID.randomUUID().toString()).addMembers(memberValue).build());
        memberValue = command.intent().getMembers(0);
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId(ACCOUNT)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setMode(opaque ? DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED
                        : DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(32)
                        .setMaxFragmentBytes(4_000_000).setMaxRoots(100).setMaxEvidenceBytes(4_000_000)
                        .setMaxBindings(20).setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000)).build(), () -> {});
        var selectedAssets = assets();
        Map<String, DocumentSchemaAdmission.Proof> proofs;
        if (withProof) {
            var proof = policy.prepareAndCheck(ByteString.copyFrom(HexFormat.of().parseHex(command.sha256())), memberValue,
                    Map.copyOf(fragments), selectedAssets.container, ignored -> selectedAssets.payload, () -> {});
            proofs = Map.of("member", proof);
        } else proofs = Map.of();
        var batch = DocumentSchemaBatch.prepare(command, new DocumentSchemaPolicies.Selection(ACCOUNT, 1, policy), proofs, () -> {});
        return new Fixture(command, memberValue, Map.copyOf(fragments), batch,
                proofs.get("member"));
    }

    private static DocumentCommitParts.Bound bound(Fixture f, UUID overrideCoreId) {
        var physical = new HashMap<DocumentCommitParts.Slot, DocumentCommitParts.Physical>();
        for (int i = 0; i < f.member.getPartsCount(); i++) {
            var part = f.member.getParts(i); if (part.hasEmpty()) continue;
            var object = part.hasUpload() ? null : part.getReuse().getObject();
            UUID id = object == null ? UUID.randomUUID() : (i == 0 && overrideCoreId != null ? overrideCoreId : UUID.fromString(object.getObjectId()));
            long size = part.hasUpload() ? part.getUpload().getSizeBytes() : object.getSizeBytes();
            String digest = part.hasUpload() ? part.getUpload().getSha256() : object.getSha256();
            physical.put(new DocumentCommitParts.Slot("member", i), new DocumentCommitParts.Physical(id,
                    part.getSlot().getPartValue(), part.getSlot().getSubKey(), "key/" + i, size, digest,
                    "application/protobuf", "version", "etag", "generation", "realm", "namespace"));
        }
        return new DocumentCommitParts.Bound(physical, Map.of("member", 1L));
    }

    private static Assets assets() throws Exception {
        return new Assets(definition(Document.getDescriptor()), definition(StringValue.getDescriptor()));
    }

    private static DocumentSchemaAdmission.Definition definition(com.google.protobuf.Descriptors.Descriptor type) throws Exception {
        var closure = DescriptorFingerprints.closure(type); var descriptor = closure.toByteString();
        var digest = sha(descriptor);
        var metadata = RepositorySchemaAsset.newBuilder().setSchema(PublicationSchemaCondition.newBuilder()
                        .setTypeName(type.getFullName()).setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setArtifactSha256(digest).setTypeUrl("type.test/" + type.getFullName())
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("synthetic test descriptor; compiler identity unknown")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"))).build();
        return new DocumentSchemaAdmission.Definition(metadata, descriptor, Optional.empty());
    }

    private static String sha(ByteString bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray())); }
        catch (Exception impossible) { throw new AssertionError(impossible); }
    }

    private static Struct parse(String json) throws InvalidProtocolBufferException {
        var result = Struct.newBuilder();
        com.google.protobuf.util.JsonFormat.parser().merge(json, result);
        return result.build();
    }

    private static ListValue list(Struct value, String field) {
        return value.getFieldsOrThrow(field).getListValue();
    }

    private static String text(Struct value, String field) {
        return value.getFieldsOrThrow(field).getStringValue();
    }

    private static final UUID REUSE_ID = UUID.fromString("11111111-1111-4111-8111-111111111111");
    private record Assets(DocumentSchemaAdmission.Definition container, DocumentSchemaAdmission.Definition payload) {}
    private record Fixture(DocumentPublicationCommand command, DocumentPublicationMember member, Map<Integer, ByteString> fragments,
            DocumentSchemaBatch batch, DocumentSchemaAdmission.Proof proof) {}
}
