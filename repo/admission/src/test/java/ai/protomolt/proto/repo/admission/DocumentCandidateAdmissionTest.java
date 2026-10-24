package ai.protomolt.proto.repo.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v2.DocumentLink;
import ai.protomolt.proto.repo.v2.LinkField;
import ai.protomolt.proto.repo.v2.LinksProto;
import ai.protomolt.proto.repo.v2.StaticTarget;
import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptsProto;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests candidate admission over nested {@link Any} payloads (#609) and cooperative
 * deadline checks during fragment and root link/concept extraction (#611).
 */
class DocumentCandidateAdmissionTest {
    private static final NodeAddress TARGET_ADDRESS = NodeAddress.newBuilder()
            .setAccountId("account")
            .setGraphId("graph")
            .setGraphAddressId("ds")
            .setDocId("target-doc")
            .build();

    @Test
    void reusesDecodedNestedAnyPayloadsDuringLinkAndConceptExtraction() throws Exception {
        var file = buildNestedLinkAndConceptFile();
        var outerType = file.findMessageTypeByName("Outer");
        var innerType = file.findMessageTypeByName("Inner");

        var link = DocumentLink.newBuilder()
                .setStaticTarget(StaticTarget.newBuilder().setAddress(TARGET_ADDRESS))
                .build();
        var concept = ConceptRef.newBuilder()
                .setSchemeId("subject")
                .setConceptId("physics")
                .build();
        var innerMsg = DynamicMessage.newBuilder(innerType)
                .setField(innerType.findFieldByName("cites"),
                        DynamicMessage.parseFrom(innerType.findFieldByName("cites").getMessageType(), link.toByteString()))
                .setField(innerType.findFieldByName("subject"),
                        DynamicMessage.parseFrom(innerType.findFieldByName("subject").getMessageType(), concept.toByteString()))
                .build();
        String innerUrl = "type.test/" + innerType.getFullName();
        String outerUrl = "type.test/" + outerType.getFullName();
        var nestedAny = Any.newBuilder().setTypeUrl(innerUrl).setValue(innerMsg.toByteString()).build();
        var outerMsg = DynamicMessage.newBuilder(outerType)
                .setField(outerType.findFieldByName("nested"),
                        DynamicMessage.parseFrom(outerType.findFieldByName("nested").getMessageType(), nestedAny.toByteString()))
                .build();
        var rootAny = Any.newBuilder().setTypeUrl(outerUrl).setValue(outerMsg.toByteString()).build();

        var containerDef = definition(Document.getDescriptor());
        var outerDef = definition(outerType);
        var innerDef = definition(innerType);
        var definitions = Map.of(outerUrl, outerDef, innerUrl, innerDef);

        // Verify DocumentPayloadCheck retains both decoded occurrences by path (#609).
        var outerAsset = DocumentSchemaAssetBinding.bind(
                outerDef.metadata(), outerDef.descriptors(),
                new ai.protomolt.proto.descriptors.ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100), () -> {});
        var innerAsset = DocumentSchemaAssetBinding.bind(
                innerDef.metadata(), innerDef.descriptors(),
                new ai.protomolt.proto.descriptors.ClosedDescriptorSet.Limits(4_000_000, 100, 1000, 100), () -> {});
        var checked = DocumentPayloadCheck.checkContextualAssets(
                outerAsset, rootAny, outerUrl, DocumentSchemaAdmission.ADMITTED_VALIDATOR,
                new DocumentPayloadCheck.Limits(1_000_000, 10_000, 20, 100, 1000), () -> {},
                selection -> innerAsset, DocumentSchemaOccurrences.Limits.DEFAULT);
        assertThat(checked.payload().decodedOccurrences()).hasSize(2);

        var coreDoc = Document.newBuilder()
                .setDocId("doc-1")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("account")
                        .setDatasourceId("ds")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .setStructuredData(rootAny)
                .build();
        var request = new DocumentCandidateAdmission.Request(
                "doc-1",
                List.of(new DocumentCandidateAdmission.Fragment(DocumentPart.DOCUMENT_PART_CORE, coreDoc.toByteString())),
                containerDef);
        var reservations = new TrackingReservations();
        var resolverCalls = new AtomicInteger();

        try (var result = DocumentCandidateAdmission.check(
                request,
                selection -> {
                    resolverCalls.incrementAndGet();
                    return definitions.get(selection.typeUrl());
                },
                limits(),
                reservations,
                Instant.EPOCH,
                () -> {},
                DocumentSchemaAdmission.ADMITTED_VALIDATOR)) {
            assertThat(result).isInstanceOf(DocumentCandidateAdmission.Accepted.class);
            var accepted = (DocumentCandidateAdmission.Accepted) result;
            assertThat(resolverCalls.get()).isEqualTo(2);
            assertThat(accepted.occurrences()).hasSize(2);
            assertThat(accepted.links())
                    .extracting(DocumentLinks.Link::fieldPath, DocumentLinks.Link::ordinal, DocumentLinks.Link::link)
                    .containsExactly(tuple("structured_data.nested.cites", 0, link));
            assertThat(accepted.concepts())
                    .extracting(DocumentConcepts.Tag::fieldPath, DocumentConcepts.Tag::ordinal,
                            DocumentConcepts.Tag::schemeId, DocumentConcepts.Tag::conceptId)
                    .containsExactly(tuple("structured_data.nested.subject", 0, "subject", "physics"));
        }
        assertThat(reservations.live).isZero();
    }

    @Test
    void honorsDeadlineDuringFragmentAndRootLinkAndConceptExtractionAndReleasesLeases() throws Exception {
        var file = buildNestedLinkAndConceptFile();
        var innerType = file.findMessageTypeByName("Inner");
        var link = DocumentLink.newBuilder()
                .setStaticTarget(StaticTarget.newBuilder().setAddress(TARGET_ADDRESS))
                .build();
        var concept = ConceptRef.newBuilder()
                .setSchemeId("subject")
                .setConceptId("physics")
                .build();
        var innerMsg = DynamicMessage.newBuilder(innerType)
                .setField(innerType.findFieldByName("cites"),
                        DynamicMessage.parseFrom(innerType.findFieldByName("cites").getMessageType(), link.toByteString()))
                .setField(innerType.findFieldByName("subject"),
                        DynamicMessage.parseFrom(innerType.findFieldByName("subject").getMessageType(), concept.toByteString()))
                .build();
        String innerUrl = "type.test/" + innerType.getFullName();
        var rootAny = Any.newBuilder().setTypeUrl(innerUrl).setValue(innerMsg.toByteString()).build();

        var containerDef = definition(Document.getDescriptor());
        var innerDef = definition(innerType);
        var coreDoc = Document.newBuilder()
                .setDocId("doc-1")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("account")
                        .setDatasourceId("ds")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .setStructuredData(rootAny)
                .build();
        var request = new DocumentCandidateAdmission.Request(
                "doc-1",
                List.of(new DocumentCandidateAdmission.Fragment(DocumentPart.DOCUMENT_PART_CORE, coreDoc.toByteString())),
                containerDef);

        var totalControlTicks = new AtomicInteger();
        var baselineReservations = new TrackingReservations();
        try (var result = DocumentCandidateAdmission.check(
                request,
                selection -> innerDef,
                limits(),
                baselineReservations,
                Instant.EPOCH,
                totalControlTicks::incrementAndGet,
                DocumentSchemaAdmission.ADMITTED_VALIDATOR)) {
            assertThat(result).isInstanceOf(DocumentCandidateAdmission.Accepted.class);
        }
        assertThat(baselineReservations.live).isZero();
        int totalTicks = totalControlTicks.get();
        assertThat(totalTicks).isGreaterThan(10);

        for (int cancelAt = 1; cancelAt <= totalTicks; cancelAt++) {
            int stopPoint = cancelAt;
            var ticks = new AtomicInteger();
            var reservations = new TrackingReservations();
            var cancelled = new CancellationException("deadline expired at tick " + stopPoint);
            assertThatThrownBy(() -> {
                try (var ignored = DocumentCandidateAdmission.check(
                        request,
                        selection -> innerDef,
                        limits(),
                        reservations,
                        Instant.EPOCH,
                        () -> {
                            if (ticks.incrementAndGet() == stopPoint) {
                                throw cancelled;
                            }
                        },
                        DocumentSchemaAdmission.ADMITTED_VALIDATOR)) {
                    throw new AssertionError("expected cancellation at tick " + stopPoint);
                }
            }).isSameAs(cancelled);
            assertThat(reservations.live).as("leases released at tick %s", stopPoint).isZero();
        }
    }

    private static FileDescriptor buildNestedLinkAndConceptFile() throws Exception {
        var linkField = FieldDescriptorProto.newBuilder()
                .setName("cites")
                .setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".ai.protomolt.proto.repo.v2.DocumentLink")
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setOptions(FieldOptions.newBuilder().setExtension(
                        LinksProto.link, LinkField.newBuilder().setRelation("references").build()));
        var conceptField = FieldDescriptorProto.newBuilder()
                .setName("subject")
                .setNumber(2)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".ai.protomolt.proto.types.v1.ConceptRef")
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setOptions(FieldOptions.newBuilder().setExtension(
                        ValidateProto.field, FieldRules.newBuilder().addConceptSchemes("subject").build()));
        var nestedAnyField = FieldDescriptorProto.newBuilder()
                .setName("nested")
                .setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".google.protobuf.Any")
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        var proto = FileDescriptorProto.newBuilder()
                .setName("acme/v1/nested_admission.proto")
                .setPackage("acme.v1")
                .setSyntax("proto3")
                .addDependency(LinksProto.getDescriptor().getName())
                .addDependency(ConceptsProto.getDescriptor().getName())
                .addDependency(ValidateProto.getDescriptor().getName())
                .addDependency(AnyProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Inner")
                        .addField(linkField)
                        .addField(conceptField))
                .addMessageType(DescriptorProto.newBuilder().setName("Outer")
                        .addField(nestedAnyField))
                .build();
        var deps = new FileDescriptor[]{
                LinksProto.getDescriptor(),
                ConceptsProto.getDescriptor(),
                ValidateProto.getDescriptor(),
                AnyProto.getDescriptor()
        };
        var bytes = FileDescriptor.buildFrom(proto, deps).toProto().toByteString();
        return FileDescriptor.buildFrom(FileDescriptorProto.parseFrom(bytes), deps);
    }

    private static DocumentSchemaAdmission.Definition definition(Descriptor type) throws Exception {
        var closure = DescriptorFingerprints.closure(type);
        var descriptors = closure.toByteString();
        String descriptorHash = sha(descriptors);
        var condition = PublicationSchemaCondition.newBuilder()
                .setTypeName(type.getFullName())
                .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure))
                .build();
        var compilation = SchemaCompilationProvenance.newBuilder()
                .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                .setUnknownCompilerReason("test fixture")
                .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"))
                .build();
        var metadata = RepositorySchemaAsset.newBuilder()
                .setSchema(condition)
                .setArtifactSha256(descriptorHash)
                .setTypeUrl("type.test/" + type.getFullName())
                .setCompilation(compilation)
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, descriptors, Optional.empty());
    }

    private static DocumentSchemaAdmission.Limits limits() {
        return new DocumentSchemaAdmission.Limits(32, 4_000_000, 100, 4_000_000, 20, 16_000_000, 1_000_000);
    }

    private static String sha(ByteString value) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.toByteArray()));
    }

    private static final class TrackingReservations implements DocumentAdmissionReservations {
        long live;

        @Override
        public Lease reserve(long bytes) {
            live += bytes;
            var closed = new AtomicBoolean();
            return () -> {
                if (closed.compareAndSet(false, true)) {
                    live -= bytes;
                }
            };
        }
    }
}
