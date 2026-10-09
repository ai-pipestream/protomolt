package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaAdmissionTest {
    private static final ClosedDescriptorSet.Limits DESCRIPTOR_LIMITS =
            new ClosedDescriptorSet.Limits(4_000_000, 1024, 10_000, 100);
    private static final DocumentAnyRootInventory.Limits INVENTORY_LIMITS =
            new DocumentAnyRootInventory.Limits(1_000_000, 10_000, 30, 100, 100);

    @Test void checksCoreAndParsedFragmentsAndReturnsTheCompleteImmutableAssetUnion() throws Exception {
        var f = fixture(true);
        var proof = check(f, reader(f.artifacts), limits(1_000_000));
        assertThat(proof.validationProfile()).isEqualTo(DocumentSchemaAdmission.PROFILE);
        assertThat(proof.commandSha256()).isEqualTo(ByteString.copyFrom(new byte[32]));
        assertThat(proof.policySha256()).isEqualTo("a".repeat(64));
        assertThat(proof.requireStructuredRoot()).isTrue();
        assertThat(proof.member()).isEqualTo(f.member);
        assertThat(proof.roots()).hasSize(3);
        assertThat(proof.roots()).extracting(DocumentSchemaAdmission.RootEvidence::ordinal).containsExactly(0, 1, 1);
        assertThat(proof.containerReference()).isEqualTo(f.container.reference);
        assertThat(proof.references()).containsExactly(f.container.reference, f.stringAsset.reference, f.timestampAsset.reference);
        assertThat(proof.artifacts()).containsAllEntriesOf(f.artifacts);
        assertThat(proof.document()).isEqualTo(f.document);
        assertThatThrownBy(() -> proof.fragments().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> proof.artifacts().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> proof.roots().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> proof.references().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void rejectsFragmentHashAndOwnershipMismatches() throws Exception {
        var f = fixture(true);
        var corrupt = new HashMap<>(f.fragments);
        byte[] bytes = corrupt.get(0).toByteArray(); bytes[bytes.length - 1] ^= 1;
        corrupt.put(0, ByteString.copyFrom(bytes));
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), f.member, corrupt, f.evidence,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .hasMessageContaining("fragment differs from publication declaration");

        var forgedOwnership = ownership("another-account");
        var member = f.member.toBuilder().setOwnership(forgedOwnership).build();
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), member, f.fragments, f.evidence,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .hasMessageContaining("decoded ownership differs from publication member");
    }

    @Test void requiresEveryRootAndRejectsDuplicateRootsAndUnusedReferences() throws Exception {
        var f = fixture(true);
        var missing = new HashMap<>(f.evidence); missing.remove(0);
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), f.member, f.fragments, missing,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .hasMessageContaining("cover every discovered");

        var duplicate = new HashMap<>(f.evidence);
        var core = new ArrayList<>(duplicate.get(0)); core.add(core.getFirst()); duplicate.put(0, List.copyOf(core));
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), f.member, f.fragments, duplicate,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .hasMessageContaining("duplicate fragment root");

        var alias = alias(f.stringAsset);
        var refs = List.of(f.stringAsset.reference, f.timestampAsset.reference, alias.reference);
        var retained = new HashMap<>(f.artifacts); retained.putAll(alias.artifacts);
        assertThatThrownBy(() -> check(f, reader(retained), limits(1_000_000), f.member, f.fragments, f.evidence, refs))
                .hasMessageContaining("schema associations differ from complete used set");
    }

    @Test void sourceClaimsMustResolveWhileSourceOmissionRemainsOptional() throws Exception {
        var f = fixture(true);
        var withoutSource = new HashMap<>(f.artifacts); withoutSource.remove(f.stringAsset.sourceHash);
        assertThatThrownBy(() -> check(f, reader(withoutSource), limits(1_000_000)))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("missing");

        var optional = fixture(false);
        assertThat(optional.stringAsset.reference.sourceSha256()).isEmpty();
        var proof = check(optional, reader(optional.artifacts), limits(1_000_000));
        assertThat(proof.roots()).hasSize(3);
    }

    @Test void evidenceMustStayWithItsRawOrdinalAndDecodedBytesShareTheMemberBudget() throws Exception {
        var f = fixture(true);
        var misplaced = Map.of(0, f.evidence.get(1), 1, f.evidence.get(0));
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), f.member, f.fragments, misplaced,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .hasMessageContaining("slot differs");

        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(11)))
                .hasMessageContaining("aggregate payload bytes");
        assertThat(check(f, reader(f.artifacts), limits(12)).roots()).hasSize(3);
    }

    @Test void checksEvidenceBoundsBeforeReadingAndRejectsAbsentAssociations() throws Exception {
        var f = fixture(true);
        DocumentSchemaAdmission.Reader unread = hash -> { throw new AssertionError("unexpected retained read"); };
        var outside = new HashMap<>(f.evidence); outside.put(f.member.getPartsCount(), f.evidence.get(0));
        assertThatThrownBy(() -> check(f, unread, limits(1_000_000), f.member, f.fragments, outside,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("outside the member");
        assertThatThrownBy(() -> check(f, unread,
                new DocumentSchemaAdmission.Limits(32, 4_000_000, 2, 4_000_000, 20, 16_000_000, 1_000_000)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("root limit");
        assertThatThrownBy(() -> check(f, unread,
                new DocumentSchemaAdmission.Limits(32, 4_000_000, 100, 1, 20, 16_000_000, 1_000_000)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("evidence byte limit");
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), f.member, f.fragments, f.evidence,
                List.of(f.stringAsset.reference)))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("metadata");
    }

    @Test void aZeroRootProofRecordsTheWeakerRequirementAndCannotMeetAStructuredPolicy() throws Exception {
        var document = Document.newBuilder().setDocId("doc").setOwnership(ownership("account")).build();
        var f = fixture(false, document);
        var request = new DocumentSchemaAdmission.Request(ByteString.copyFrom(new byte[32]), "a".repeat(64), false,
                f.member, f.fragments, Map.of(), f.container.reference, List.of());
        var proof = DocumentSchemaAdmission.check(request, reader(f.artifacts), limits(1_000_000), () -> {});
        assertThat(proof.roots()).isEmpty();
        assertThat(proof.requireStructuredRoot()).isFalse();
        assertThat(proof.references()).containsExactly(f.container.reference);
        assertThat(proof.artifacts()).isEqualTo(f.container.artifacts);
        var required = new DocumentSchemaAdmission.Request(request.commandSha256(), request.policySha256(), true,
                f.member, f.fragments, Map.of(), f.container.reference, List.of());
        assertThatThrownBy(() -> DocumentSchemaAdmission.check(required, reader(f.artifacts), limits(1_000_000), () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("required structured root is absent");
    }

    @Test void accessFailureImmediatelyBeforeProofDeliveryPropagatesUnchanged() throws Exception {
        var f = fixture(true);
        var request = new DocumentSchemaAdmission.Request(ByteString.copyFrom(new byte[32]), "a".repeat(64), true,
                f.member, f.fragments, f.evidence, f.container.reference,
                List.of(f.stringAsset.reference, f.timestampAsset.reference));
        var visits = new java.util.concurrent.atomic.AtomicInteger();
        DocumentSchemaAdmission.check(request, reader(f.artifacts), limits(1_000_000), visits::incrementAndGet);
        int finalVisit = visits.get();
        visits.set(0);
        var revoked = new SecurityException("revoked before delivery");
        assertThatThrownBy(() -> DocumentSchemaAdmission.check(request, reader(f.artifacts), limits(1_000_000), () -> {
            if (visits.incrementAndGet() == finalVisit) throw revoked;
        })).isSameAs(revoked);
        assertThat(visits.get()).isEqualTo(finalVisit);
    }

    @Test void enforcesStructuredRootOwnershipAndUnknownFieldBoundaries() throws Exception {
        var f = fixture(true);
        var wrongSchema = PublicationSchemaCondition.newBuilder().setTypeName("google.protobuf.StringValue")
                .setDescriptorFingerprint("f".repeat(64)).build();
        var member = f.member.toBuilder().setStructuredSchema(wrongSchema).build();
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), member, f.fragments, f.evidence,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .hasMessageContaining("structured root differs from required schema");

        var invalidOwnership = ownership("account").toBuilder().setDatasourceId("").build();
        var invalidMember = f.member.toBuilder().setOwnership(invalidOwnership).build();
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), invalidMember, f.fragments, f.evidence,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .hasMessageContaining("publication-ownership");

        var unknown = Document.parseFrom(f.fragments.get(0)).toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
                .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        var raw = unknown.toByteString();
        var fragments = new HashMap<>(f.fragments); fragments.put(0, raw);
        var declared = f.member.toBuilder().setParts(0, f.member.getParts(0).toBuilder()
                .setUpload(f.member.getParts(0).getUpload().toBuilder().setSha256(sha(raw)).setSizeBytes(raw.size()))).build();
        assertThatThrownBy(() -> check(f, reader(f.artifacts), limits(1_000_000), declared, fragments, f.evidence,
                List.of(f.stringAsset.reference, f.timestampAsset.reference)))
                .hasMessageContaining("unknown document fields");
    }

    @Test void rejectsBufAnnotatedPayloadWithOtherwiseConsistentEvidence() throws Exception {
        var field = com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("name").setNumber(1).setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setOptions(com.google.protobuf.DescriptorProtos.FieldOptions.newBuilder()
                        .setExtension(build.buf.validate.ValidateProto.field, build.buf.validate.FieldRules.newBuilder()
                                .setString(build.buf.validate.StringRules.newBuilder().setMinLen(3)).build()));
        var file = com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("admission_buf.proto").setPackage("admission.test").setSyntax("proto3")
                .addDependency(build.buf.validate.ValidateProto.getDescriptor().getName())
                .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("Payload").addField(field));
        var type = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(file.build(),
                new com.google.protobuf.Descriptors.FileDescriptor[]{build.buf.validate.ValidateProto.getDescriptor()})
                .findMessageTypeByName("Payload");
        var asset = asset(type, false);
        for (String name : List.of("valid", "x")) {
            var payload = com.google.protobuf.DynamicMessage.newBuilder(type).setField(type.findFieldByName("name"), name).build();
            var document = Document.newBuilder().setDocId("doc").setOwnership(ownership("account"))
                    .setStructuredData(Any.pack(payload, "type.test")).build();
            var f = fixture(document, asset, asset(Timestamp.getDescriptor(), false));
            var request = new DocumentSchemaAdmission.Request(ByteString.copyFrom(new byte[32]), "a".repeat(64), true,
                    f.member, f.fragments, f.evidence, f.container.reference, List.of(asset.reference));
            if (name.equals("valid")) {
                assertThat(DocumentSchemaAdmission.check(request, reader(f.artifacts), limits(1_000_000), () -> {}).roots()).hasSize(1);
            } else {
                assertThatThrownBy(() -> DocumentSchemaAdmission.check(request, reader(f.artifacts), limits(1_000_000), () -> {}))
                        .isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class)
                        .hasMessageContaining("min_len");
            }
        }
    }

    private static DocumentSchemaAdmission.Proof check(Fixture f, DocumentSchemaAdmission.Reader reader,
            DocumentSchemaAdmission.Limits limits) throws Exception {
        return check(f, reader, limits, f.member, f.fragments, f.evidence,
                List.of(f.stringAsset.reference, f.timestampAsset.reference));
    }

    private static DocumentSchemaAdmission.Proof check(Fixture f, DocumentSchemaAdmission.Reader reader,
            DocumentSchemaAdmission.Limits limits, DocumentPublicationMember member,
            Map<Integer, ByteString> fragments, Map<Integer, List<DocumentSchemaAdmission.EncodedEvidence>> evidence,
            List<DocumentSchemaAdmission.Reference> references) throws Exception {
        return DocumentSchemaAdmission.check(new DocumentSchemaAdmission.Request(ByteString.copyFrom(new byte[32]),
                "a".repeat(64), true, member, fragments, evidence, f.container.reference, references), reader, limits, () -> {});
    }

    private static DocumentSchemaAdmission.Limits limits(int decodedBytes) {
        return new DocumentSchemaAdmission.Limits(32, 4_000_000, 100, 4_000_000, 20, 16_000_000, decodedBytes);
    }

    private static DocumentSchemaAdmission.Reader reader(Map<String, ByteString> artifacts) {
        return hash -> Optional.ofNullable(artifacts.get(hash));
    }

    private static Fixture fixture(boolean retainSource) throws Exception {
        var ownership = ownership("account");
        var document = Document.newBuilder().setDocId("doc").setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("one"), "type.test"))
                .putParserResults("first", ParserResult.newBuilder()
                        .setDocument(ParserDocument.newBuilder().setShape(Any.pack(StringValue.of("one"), "type.test"))).build())
                .putParserResults("second", ParserResult.newBuilder()
                        .setDocument(ParserDocument.newBuilder().setShape(
                                Any.pack(Timestamp.newBuilder().setSeconds(2).build(), "type.test"))).build())
                .build();
        return fixture(retainSource, document);
    }

    private static Fixture fixture(boolean retainSource, Document document) throws Exception {
        var stringAsset = asset(StringValue.getDescriptor(), retainSource);
        var timestampAsset = asset(Timestamp.getDescriptor(), false);
        return fixture(document, stringAsset, timestampAsset);
    }

    private static Fixture fixture(Document document, Asset stringAsset, Asset timestampAsset) throws Exception {
        var container = asset(Document.getDescriptor(), false);
        var parts = DocumentPartCodec.split(document, PartLayouts.document());
        var member = DocumentPublicationMember.newBuilder().setMemberId("schema-member")
                .setDriveId(UUID.randomUUID().toString()).setOwnership(ownership("account"))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true)
                        .setAddress(NodeAddress.newBuilder().setAccountId("account").setDocId("doc")
                                .setGraphId("graph").setGraphAddressId("node")));
        var fragments = new HashMap<Integer, ByteString>();
        for (int i = 0; i < parts.size(); i++) {
            var part = parts.get(i);
            var bytes = ByteString.copyFrom(part.bytes());
            fragments.put(i, bytes);
            member.addParts(DocumentPublicationPart.newBuilder()
                    .setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(bytes.size()).setSha256(sha(bytes))
                            .setContentType("application/protobuf")));
        }
        var builtMember = member.build();
        var payloads = Map.of(stringAsset.reference.typeUrl(), stringAsset,
                timestampAsset.reference.typeUrl(), timestampAsset);
        var artifacts = new HashMap<String, ByteString>();
        for (var asset : List.of(container, stringAsset, timestampAsset)) artifacts.putAll(asset.artifacts);
        var evidence = evidence(parts, fragments, payloads, container);
        return new Fixture(document, builtMember, Map.copyOf(fragments), evidence, container, stringAsset,
                timestampAsset, payloads, Map.copyOf(artifacts));
    }

    private static Map<Integer, List<DocumentSchemaAdmission.EncodedEvidence>> evidence(
            List<ai.protomolt.proto.repo.codec.PartObject> parts, Map<Integer, ByteString> fragments,
            Map<String, Asset> payloads, Asset container) throws Exception {
        var result = new HashMap<Integer, List<DocumentSchemaAdmission.EncodedEvidence>>();
        for (int ordinal = 0; ordinal < parts.size(); ordinal++) {
            var part = parts.get(ordinal);
            if (part.part() != DocumentPart.DOCUMENT_PART_CORE && part.part() != DocumentPart.DOCUMENT_PART_PARSED) continue;
            var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()).build();
            var inventory = DocumentAnyRootInventory.inspect(container.binding.schema(), slot, fragments.get(ordinal), "doc",
                    INVENTORY_LIMITS, () -> {});
            var bundles = new ArrayList<DocumentSchemaAdmission.EncodedEvidence>();
            for (var root : inventory.roots()) {
                var asset = payloads.get(root.envelope().getTypeUrl());
                // Candidate-supplied claims for these non-nested fixtures. The admission
                // boundary must validate them; generating them is not a successful check.
                var paths = List.of(RepositorySchemaOccurrencePath.newBuilder().setEncodingVersion(1)
                        .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setAnyBoundary(RepositoryAnyResolution.newBuilder()
                                .setTypeUrl(root.envelope().getTypeUrl()).setValueSha256(sha(root.envelope().getValue()))
                                .setValueSizeBytes(root.envelope().getValue().size())
                                .setResolved(RepositoryResolvedSchema.newBuilder().setSchema(asset.metadata.getSchema())
                                        .setArtifactSha256(asset.descriptorHash)))).build());
                var bundle = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1)
                        .setRoot(DocumentSchemaRootProjection.project(inventory, root, () -> {})).addAllOccurrences(paths).build();
                var encoded = DocumentRootSchemaEvidenceCodec.encode(bundle, () -> {});
                bundles.add(new DocumentSchemaAdmission.EncodedEvidence(DocumentRootSchemaEvidenceCodec.CODEC,
                        DocumentRootSchemaEvidenceCodec.VERSION, encoded.bytes(), encoded.sha256()));
            }
            if (!bundles.isEmpty()) result.put(ordinal, List.copyOf(bundles));
        }
        return Map.copyOf(result);
    }

    private static Asset asset(com.google.protobuf.Descriptors.Descriptor type, boolean withSource) throws Exception {
        var closure = DescriptorFingerprints.closure(type);
        var descriptor = closure.toByteString();
        String descriptorHash = sha(descriptor);
        var condition = PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build();
        var source = withSource ? ByteString.copyFromUtf8("synthetic retained source; integrity only") : null;
        String sourceHash = source == null ? null : sha(source);
        var provenance = SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                .setUnknownCompilerReason("fixture producer did not identify its compiler")
                .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"));
        if (sourceHash != null) provenance.setSourceArtifactSha256(sourceHash);
        var metadata = RepositorySchemaAsset.newBuilder().setSchema(condition).setArtifactSha256(descriptorHash)
                .setTypeUrl("type.test/" + type.getFullName()).setCompilation(provenance).build();
        var metadataBytes = DocumentSchemaAssetCodec.encode(metadata, () -> {}).bytes();
        String metadataHash = sha(metadataBytes);
        var reference = new DocumentSchemaAdmission.Reference(metadata.getTypeUrl(), descriptorHash,
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, metadataHash,
                Optional.ofNullable(sourceHash));
        var binding = DocumentSchemaAssetBinding.bind(metadata, descriptor, DESCRIPTOR_LIMITS, () -> {});
        var artifacts = new HashMap<String, ByteString>();
        artifacts.put(descriptorHash, descriptor); artifacts.put(metadataHash, metadataBytes);
        if (source != null) artifacts.put(sourceHash, source);
        return new Asset(descriptor, metadata, metadataBytes, source, descriptorHash, metadataHash, sourceHash,
                reference, binding, Map.copyOf(artifacts));
    }

    private static Asset alias(Asset original) throws Exception {
        var metadata = original.metadata.toBuilder().setTypeUrl("alias/" + original.metadata.getSchema().getTypeName())
                .setCompilation(original.metadata.getCompilation().toBuilder().clearSourceArtifactSha256()).build();
        var metadataBytes = DocumentSchemaAssetCodec.encode(metadata, () -> {}).bytes();
        String metadataHash = sha(metadataBytes);
        var reference = new DocumentSchemaAdmission.Reference(metadata.getTypeUrl(), original.descriptorHash,
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, metadataHash, Optional.empty());
        var binding = DocumentSchemaAssetBinding.bind(metadata, original.descriptor, DESCRIPTOR_LIMITS, () -> {});
        return new Asset(original.descriptor, metadata, metadataBytes, null, original.descriptorHash, metadataHash, null,
                reference, binding, Map.of(metadataHash, metadataBytes));
    }

    private static OwnershipContext ownership(String account) {
        return OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("source")
                .setSecurity(DocumentSecurity.getDefaultInstance()).build();
    }
    private static String sha(ByteString bytes) { return HexFormat.of().formatHex(sha(bytes.toByteArray())); }
    private static byte[] sha(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    private record Asset(ByteString descriptor, RepositorySchemaAsset metadata, ByteString metadataBytes, ByteString source,
            String descriptorHash, String metadataHash, String sourceHash, DocumentSchemaAdmission.Reference reference,
            DocumentSchemaAssetBinding binding, Map<String, ByteString> artifacts) {}
    private record Fixture(Document document, DocumentPublicationMember member, Map<Integer, ByteString> fragments,
            Map<Integer, List<DocumentSchemaAdmission.EncodedEvidence>> evidence, Asset container, Asset stringAsset,
            Asset timestampAsset, Map<String, Asset> payloads, Map<String, ByteString> artifacts) {}
}
