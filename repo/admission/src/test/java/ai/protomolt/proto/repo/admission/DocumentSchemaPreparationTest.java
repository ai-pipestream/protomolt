package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSchemaPreparationTest {
    @Test void preparesAllCoreAndParsedRootsAndDoesNotResolveAgainDuringReplay() throws Exception {
        var f = fixture(false);
        var calls = new ArrayList<DocumentSchemaAdmission.Selection>();
        var proof = prepare(f, selection -> {
            calls.add(selection);
            return selection.typeUrl().endsWith("StringValue") ? f.string.definition() : f.timestamp.definition();
        }, limits(1_000_000), () -> {});
        assertThat(proof.roots()).extracting(DocumentSchemaAdmission.RootEvidence::ordinal).containsExactly(0, 1);
        assertThat(proof.references()).containsExactly(f.container.reference, f.string.reference, f.timestamp.reference);
        assertThat(calls).hasSize(2);
        assertThat(calls).extracting(DocumentSchemaAdmission.Selection::ordinal).containsExactly(0, 1);
        assertThat(proof.artifacts()).containsKeys(f.container.reference.descriptorSha256(),
                f.string.reference.descriptorSha256(), f.timestamp.reference.descriptorSha256());
    }

    @Test void resolvesOptionalSourcesAndRetainsTheirExactBytes() throws Exception {
        var f = fixture(true);
        var proof = prepare(f, selection -> selection.typeUrl().endsWith("StringValue")
                ? f.string.definition() : f.timestamp.definition(), limits(1_000_000), () -> {});
        assertThat(f.string.reference.sourceSha256()).isPresent();
        assertThat(proof.artifacts().get(f.string.reference.sourceSha256().orElseThrow())).isEqualTo(f.string.source);
        assertThat(f.timestamp.reference.sourceSha256()).isEmpty();
        assertThat(proof.references()).hasSize(3);
    }

    @Test void failedSourceResolutionProducesNoProofAndARepairedRetrySucceeds() throws Exception {
        var f = fixture(true);
        var missingSource = new DocumentSchemaAdmission.Definition(f.string.metadata, f.string.descriptors, Optional.empty());
        assertThatThrownBy(() -> prepare(f, selection -> selection.typeUrl().endsWith("StringValue")
                ? missingSource : f.timestamp.definition(), limits(1_000_000), () -> {}))
                .hasMessageContaining("source bytes differ from metadata presence");
        var corruptSource = new DocumentSchemaAdmission.Definition(f.string.metadata, f.string.descriptors,
                Optional.of(ByteString.copyFromUtf8("corrupt source")));
        assertThatThrownBy(() -> prepare(f, selection -> selection.typeUrl().endsWith("StringValue")
                ? corruptSource : f.timestamp.definition(), limits(1_000_000), () -> {}))
                .isInstanceOf(DocumentRetainedSchemaAssets.DataLoss.class).hasMessageContaining("digest");
        var retry = prepare(f, selection -> selection.typeUrl().endsWith("StringValue")
                ? f.string.definition() : f.timestamp.definition(), limits(1_000_000), () -> {});
        assertThat(retry.artifacts()).containsEntry(f.string.reference.sourceSha256().orElseThrow(), f.string.source);
    }

    @Test void resolverReceivesOccurrenceOrdinalAndCanSelectTheMatchingDefinition() throws Exception {
        var f = fixture(false);
        var calls = new ArrayList<DocumentSchemaAdmission.Selection>();
        var proof = prepare(f, selection -> {
            calls.add(selection);
            if (selection.ordinal() == 0) return f.string.definition();
            return f.timestamp.definition();
        }, limits(1_000_000), () -> {});
        assertThat(calls).extracting(DocumentSchemaAdmission.Selection::ordinal).containsExactly(0, 1);
        assertThat(proof.references()).contains(f.string.reference, f.timestamp.reference);
    }

    @Test void nestedAnySelectionCarriesItsPrefixAndExactValueIdentity() throws Exception {
        var f = fixture(false);
        var outerType = nestedContainerType();
        var outerAsset = asset(outerType, false);
        var inner = Any.pack(StringValue.of("nested"), "type.test");
        var outer = com.google.protobuf.DynamicMessage.newBuilder(outerType)
                .setField(outerType.findFieldByName("child"), inner).build();
        var document = f.document.toBuilder().setStructuredData(Any.pack(outer, "type.test")).build();
        var nestedFixture = fixture(false, document);
        var selections = new ArrayList<DocumentSchemaAdmission.Selection>();
        var proof = prepare(nestedFixture, selection -> {
            selections.add(selection);
            if (selection.typeUrl().endsWith("Container")) return outerAsset.definition();
            if (selection.typeUrl().endsWith("StringValue")) return nestedFixture.string.definition();
            return nestedFixture.timestamp.definition();
        }, limits(1_000_000), () -> {});
        var nested = selections.stream().filter(s -> s.typeUrl().endsWith("StringValue")).findFirst().orElseThrow();
        assertThat(selections).hasSize(3);
        assertThat(nested.prefix()).hasSize(2);
        assertThat(nested.prefix().getFirst().getAnyBoundary().getResolved().getArtifactSha256())
                .isEqualTo(outerAsset.reference.descriptorSha256());
        assertThat(nested.prefix().getLast().getFieldNumber()).isEqualTo(1);
        assertThat(nested.valueSha256()).isEqualTo(sha(inner.getValue()));
        assertThat(nested.valueSizeBytes()).isEqualTo(inner.getValue().size());
        assertThat(proof.roots()).hasSize(2);
    }

    @Test void selectsTwoDefinitionsForTheSameUrlByNestedOccurrenceAndReplaysBoth() throws Exception {
        var oldType = versionedType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        var newType = versionedType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
        var oldAsset = asset(oldType, false);
        var newAsset = asset(newType, false);
        var oldValue = Any.pack(com.google.protobuf.DynamicMessage.newBuilder(oldType)
                .setField(oldType.findFieldByName("value"), "old").build(), "type.test");
        var newValue = Any.pack(com.google.protobuf.DynamicMessage.newBuilder(newType)
                .setField(newType.findFieldByName("value"), 42).build(), "type.test");
        assertThat(oldValue.getTypeUrl()).isEqualTo(newValue.getTypeUrl());
        var outerType = nestedContainerType();
        var file = outerType.getFile().toProto().toBuilder();
        file.getMessageTypeBuilder(0).addField(file.getMessageType(0).getField(0).toBuilder().setName("other").setNumber(2));
        outerType = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(file.build(),
                new com.google.protobuf.Descriptors.FileDescriptor[]{Any.getDescriptor().getFile()}).findMessageTypeByName("Container");
        var outerAsset = asset(outerType, false);
        var outer = com.google.protobuf.DynamicMessage.newBuilder(outerType)
                .setField(outerType.findFieldByName("child"), oldValue)
                .setField(outerType.findFieldByName("other"), newValue).build();
        var f = fixture(false, fixture(false).document.toBuilder().clearParserResults()
                .setStructuredData(Any.pack(outer, "type.test")).build());
        var calls = new ArrayList<DocumentSchemaAdmission.Selection>();
        var proof = prepare(f, selection -> {
            calls.add(selection);
            if (selection.prefix().isEmpty()) return outerAsset.definition();
            return selection.prefix().getLast().getFieldNumber() == 1 ? oldAsset.definition() : newAsset.definition();
        }, limits(1_000_000), () -> {});
        assertThat(calls).hasSize(3); // No registry calls during final self-replay.
        var children = calls.subList(1, 3);
        assertThat(children).extracting(DocumentSchemaAdmission.Selection::typeUrl)
                .containsExactly(oldValue.getTypeUrl(), newValue.getTypeUrl());
        assertThat(children).extracting(s -> s.prefix().getLast().getFieldNumber()).containsExactly(1, 2);
        assertThat(children).extracting(DocumentSchemaAdmission.Selection::valueSha256)
                .containsExactly(sha(oldValue.getValue()), sha(newValue.getValue()));
        assertThat(children).extracting(DocumentSchemaAdmission.Selection::valueSizeBytes)
                .containsExactly((long) oldValue.getValue().size(), (long) newValue.getValue().size());
        assertThat(proof.references().stream().filter(r -> r.typeUrl().equals(oldValue.getTypeUrl())))
                .extracting(DocumentSchemaAdmission.Reference::descriptorSha256)
                .containsExactlyInAnyOrder(oldAsset.reference.descriptorSha256(), newAsset.reference.descriptorSha256());
        var encoded = proof.roots().getFirst().encoded();
        var bundle = DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), encoded.bytes(), encoded.sha256(), () -> {});
        assertThat(bundle.getOccurrencesList()).hasSize(3);
        assertThat(bundle.getOccurrencesList()).extracting(p -> p.getSteps(p.getStepsCount() - 1).getAnyBoundary()
                .getResolved().getArtifactSha256()).containsExactlyInAnyOrder(outerAsset.reference.descriptorSha256(),
                        oldAsset.reference.descriptorSha256(), newAsset.reference.descriptorSha256());
        assertThat(bundle.getOccurrencesList().stream().filter(p -> p.getStepsCount() > 1)
                .collect(java.util.stream.Collectors.toMap(p -> p.getSteps(p.getStepsCount() - 2).getFieldNumber(),
                        p -> p.getSteps(p.getStepsCount() - 1).getAnyBoundary().getResolved().getArtifactSha256())))
                .isEqualTo(Map.of(1, oldAsset.reference.descriptorSha256(), 2, newAsset.reference.descriptorSha256()));
    }

    @Test void invalidAnnotatedPayloadReturnsNoProofAndPropagatesValidationFailure() throws Exception {
        var f = fixture(false);
        var field = com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder().setName("name").setNumber(1)
                .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                .setOptions(com.google.protobuf.DescriptorProtos.FieldOptions.newBuilder()
                        .setExtension(build.buf.validate.ValidateProto.field, build.buf.validate.FieldRules.newBuilder()
                                .setString(build.buf.validate.StringRules.newBuilder().setMinLen(3)).build()));
        var file = com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder().setName("preparation_buf.proto")
                .setPackage("preparation.test").setSyntax("proto3")
                .addDependency(build.buf.validate.ValidateProto.getDescriptor().getName())
                .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("Payload").addField(field));
        var type = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(file.build(), new com.google.protobuf.Descriptors.FileDescriptor[]{
                build.buf.validate.ValidateProto.getDescriptor()}).findMessageTypeByName("Payload");
        var payload = com.google.protobuf.DynamicMessage.newBuilder(type).setField(type.findFieldByName("name"), "x").build();
        var doc = f.document.toBuilder().setStructuredData(Any.pack(payload, "type.test")).build();
        var changed = fixture(false, doc);
        var buf = asset(type, false);
        assertThatThrownBy(() -> prepare(changed, selection -> selection.typeUrl().endsWith("Payload")
                ? buf.definition() : selection.typeUrl().endsWith("StringValue") ? changed.string.definition() : changed.timestamp.definition(),
                limits(1_000_000), () -> {}))
                .isInstanceOf(ai.protomolt.proto.validate.ValidationResult.ValidationException.class)
                .hasMessageContaining("min_len");
    }

    @Test void resolverAbsenceWrongIdentityAndThrownFailuresDoNotYieldEvidence() throws Exception {
        var f = fixture(false);
        assertThatThrownBy(() -> prepare(f, selection -> null, limits(1_000_000), () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unresolved Any schema definition");
        var wrong = asset(Timestamp.getDescriptor(), false, "other/google.protobuf.Timestamp");
        assertThatThrownBy(() -> prepare(f, selection -> wrong.definition(), limits(1_000_000), () -> {}))
                .hasMessageContaining("type URL mismatch");
        var failure = new IllegalStateException("registry unavailable");
        assertThatThrownBy(() -> prepare(f, selection -> { throw failure; }, limits(1_000_000), () -> {})).isSameAs(failure);
    }

    @Test void enforcesRetainedAndRootBudgetsAndHonorsCancellation() throws Exception {
        var f = fixture(false);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> prepare(f, selection -> { calls.incrementAndGet(); return selection.typeUrl().endsWith("StringValue")
                ? f.string.definition() : f.timestamp.definition(); },
                new DocumentSchemaAdmission.Limits(32, 4_000_000, 100, 4_000_000, 20, 1, 1_000_000), () -> {}))
                .hasMessageContaining("retained artifact union exceeds limit");
        assertThat(calls).hasValue(0);
        assertThatThrownBy(() -> prepare(f, selection -> selection.typeUrl().endsWith("StringValue")
                ? f.string.definition() : f.timestamp.definition(),
                new DocumentSchemaAdmission.Limits(32, 4_000_000, 1, 4_000_000, 20, 16_000_000, 1_000_000), () -> {}))
                .hasMessageContaining("root limit");
        DocumentSchemaAdmission.Resolver resolver = selection -> selection.typeUrl().endsWith("StringValue")
                ? f.string.definition() : f.timestamp.definition();
        assertThatThrownBy(() -> prepare(f, resolver, limits(6), () -> {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("aggregate payload bytes");
        assertThat(prepare(f, resolver, limits(7), () -> {}).roots()).hasSize(2);
        assertThatThrownBy(() -> prepare(f, selection -> selection.typeUrl().endsWith("StringValue")
                ? f.string.definition() : f.timestamp.definition(), limits(1_000_000), () -> { throw new CancellationException("cancelled"); }))
                .isInstanceOf(CancellationException.class);
    }

    private static DocumentSchemaAdmission.Proof prepare(Fixture f, DocumentSchemaAdmission.Resolver resolver,
            DocumentSchemaAdmission.Limits limits, Runnable control) throws Exception {
        var request = new DocumentSchemaAdmission.Preparation(ByteString.copyFrom(new byte[32]), "a".repeat(64), true,
                f.member, f.fragments, new DocumentSchemaAdmission.Definition(f.container.metadata,
                        f.container.descriptors, Optional.ofNullable(f.container.source)));
        return DocumentSchemaAdmission.prepareAndCheck(request, resolver, limits, control);
    }

    private static DocumentSchemaAdmission.Limits limits(int decoded) {
        return new DocumentSchemaAdmission.Limits(32, 4_000_000, 100, 4_000_000, 20, 16_000_000, decoded);
    }

    private static Fixture fixture(boolean withSource) throws Exception {
        var ownership = OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                .setSecurity(DocumentSecurity.getDefaultInstance()).build();
        var document = Document.newBuilder().setDocId("doc").setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("one"), "type.test"))
                .putParserResults("parsed", ParserResult.newBuilder().setDocument(ParserDocument.newBuilder()
                        .setShape(Any.pack(Timestamp.newBuilder().setSeconds(2).build(), "type.test"))).build()).build();
        return fixture(withSource, document);
    }

    private static Fixture fixture(boolean withSource, Document document) throws Exception {
        var container = asset(Document.getDescriptor(), false);
        var string = asset(StringValue.getDescriptor(), withSource);
        var timestamp = asset(Timestamp.getDescriptor(), false);
        var parts = DocumentPartCodec.split(document, PartLayouts.document());
        var member = DocumentPublicationMember.newBuilder().setMemberId("schema-preparation-test")
                .setDriveId(UUID.randomUUID().toString()).setOwnership(document.getOwnership())
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setDocId("doc").setGraphId("graph").setGraphAddressId("node")));
        var fragments = new HashMap<Integer, ByteString>();
        for (int i = 0; i < parts.size(); i++) {
            var part = parts.get(i); var bytes = ByteString.copyFrom(part.bytes()); fragments.put(i, bytes);
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                    .setPart(part.part()).setSubKey(part.subKey())).setUpload(PublicationUpload.newBuilder()
                    .setSizeBytes(bytes.size()).setSha256(sha(bytes)).setContentType("application/protobuf")));
        }
        return new Fixture(document, member.build(), Map.copyOf(fragments), container, string, timestamp);
    }

    private static Asset asset(com.google.protobuf.Descriptors.Descriptor type, boolean withSource) throws Exception {
        return asset(type, withSource, "type.test/" + type.getFullName());
    }

    private static Asset asset(com.google.protobuf.Descriptors.Descriptor type, boolean withSource, String typeUrl) throws Exception {
        var closure = DescriptorFingerprints.closure(type); var descriptors = closure.toByteString(); var descriptorHash = sha(descriptors);
        var condition = PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)).build();
        var source = withSource ? ByteString.copyFromUtf8("synthetic retained source; integrity only") : null;
        var compilation = SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                .setUnknownCompilerReason("test fixture compiler identity unavailable")
                .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"));
        if (source != null) compilation.setSourceArtifactSha256(sha(source));
        var metadata = RepositorySchemaAsset.newBuilder().setSchema(condition).setArtifactSha256(descriptorHash)
                .setTypeUrl(typeUrl).setCompilation(compilation).build();
        var schemaAsset = DocumentSchemaAssetCodec.encode(metadata, () -> {}).bytes();
        var ref = new DocumentSchemaAdmission.Reference(typeUrl, descriptorHash, DocumentSchemaAssetCodec.CODEC,
                DocumentSchemaAssetCodec.VERSION, sha(schemaAsset), Optional.ofNullable(source == null ? null : sha(source)));
        return new Asset(metadata, descriptors, source, ref);
    }

    private static com.google.protobuf.Descriptors.Descriptor nestedContainerType() throws Exception {
        var field = com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder().setName("child").setNumber(1)
                .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".google.protobuf.Any");
        var file = com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder().setName("preparation_nested.proto")
                .setPackage("preparation.test").setSyntax("proto3").addDependency("google/protobuf/any.proto")
                .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("Container").addField(field));
        return com.google.protobuf.Descriptors.FileDescriptor.buildFrom(file.build(), new com.google.protobuf.Descriptors.FileDescriptor[]{
                Any.getDescriptor().getFile()}).findMessageTypeByName("Container");
    }

    private static com.google.protobuf.Descriptors.Descriptor versionedType(
            com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type valueType) throws Exception {
        var file = com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder().setName("versioned.proto")
                .setPackage("preparation.test").setSyntax("proto3")
                .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("Versioned")
                        .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("value").setNumber(1).setType(valueType))).build();
        return com.google.protobuf.Descriptors.FileDescriptor.buildFrom(file,
                new com.google.protobuf.Descriptors.FileDescriptor[0]).findMessageTypeByName("Versioned");
    }

    private static String sha(ByteString value) { try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.toByteArray())); }
        catch (Exception impossible) { throw new AssertionError(impossible); } }

    private record Asset(RepositorySchemaAsset metadata, ByteString descriptors, ByteString source,
            DocumentSchemaAdmission.Reference reference) {
        DocumentSchemaAdmission.Definition definition() { return new DocumentSchemaAdmission.Definition(metadata, descriptors, Optional.ofNullable(source)); }
    }
    private record Fixture(Document document, DocumentPublicationMember member, Map<Integer, ByteString> fragments,
            Asset container, Asset string, Asset timestamp) {}
}
