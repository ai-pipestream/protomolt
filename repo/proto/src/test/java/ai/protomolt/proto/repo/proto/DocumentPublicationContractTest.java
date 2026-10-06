package ai.protomolt.proto.repo.proto;

import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Shape fixtures only: no executor, provider verification or authorization claim. */
class DocumentPublicationContractTest {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String UUID = "10000000-0000-4000-8000-000000000001";
    private static final String SHA = "a".repeat(64);

    private static NodeAddress address() {
        return NodeAddress.newBuilder().setAccountId("account").setDocId("doc")
                .setGraphId("graph").setGraphAddressId("node").build();
    }

    private static DocumentRevisionCondition condition() {
        return DocumentRevisionCondition.newBuilder().setAddress(address()).setIfAbsent(true).build();
    }

    private static DocumentPublicationSlot slot(DocumentPart part, String subKey) {
        return DocumentPublicationSlot.newBuilder().setPart(part).setSubKey(subKey).build();
    }

    private static PublicationUpload upload() {
        return PublicationUpload.newBuilder().setSha256(SHA).setContentType("application/protobuf").build();
    }

    private static DocumentPublicationPart core() {
        return DocumentPublicationPart.newBuilder().setSlot(slot(DocumentPart.DOCUMENT_PART_CORE, ""))
                .setUpload(upload()).build();
    }

    private static DocumentPublicationMember member() {
        return DocumentPublicationMember.newBuilder().setMemberId("member-1").setDestination(condition())
                .setDriveId(UUID).setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE).setOwnership(OwnershipContext.newBuilder().setAccountId("account")
                        .setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()))
                .addParts(core()).build();
    }

    private static DocumentPublicationIntent intent() {
        return DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID).addMembers(member()).build();
    }

    private static PublicationReuse reuse() {
        return PublicationReuse.newBuilder().setSource(condition().toBuilder().setExpectedMutationRevision(7))
                .setSourceSlot(core().getSlot()).setObject(PublicationObjectIdentity.newBuilder()
                        .setObjectId(UUID).setBackendGeneration("profile-7").setStorageRealm("realm")
                        .setNamespace("documents").setObjectKey("immutable/original")
                        .setSha256(SHA).setContentType("application/protobuf")).build();
    }

    private static void check(Message value, boolean expected) throws Exception {
        for (Message message : new Message[]{value,
                DynamicMessage.parseFrom(value.getDescriptorForType(), value.toByteArray())}) {
            var result = VALIDATOR.validate(message);
            assertThat(result.valid()).as("%s violations: %s", message.getDescriptorForType().getName(), result.violations())
                    .isEqualTo(expected);
        }
    }

    @Test void acceptsUploadReuseEmptyAndExplicitRevision() throws Exception {
        check(intent(), true);
        check(member().toBuilder().setDestination(condition().toBuilder().setExpectedMutationRevision(7)).build(), true);
        check(member().toBuilder().setParts(0, core().toBuilder().setReuse(reuse())).build(), true);
        check(member().toBuilder().addParts(DocumentPublicationPart.newBuilder()
                .setSlot(slot(DocumentPart.DOCUMENT_PART_BLOBS, "")).setEmpty(true)).build(), true);
        check(reuse().toBuilder().setObject(reuse().getObject().toBuilder().setProviderVersion("opaque-v1")).build(), true);
    }

    @Test void requiresVersionScopeAndMembers() throws Exception {
        check(intent().toBuilder().clearEncodingVersion().build(), false);
        check(intent().toBuilder().setEncodingVersion(2).build(), false);
        check(intent().toBuilder().clearAccountId().build(), false);
        check(intent().toBuilder().setAccountId(" ").build(), false);
        check(intent().toBuilder().clearOperationId().build(), false);
        check(intent().toBuilder().setOperationId("not-a-uuid").build(), false);
        check(intent().toBuilder().clearMembers().build(), false);
        var maximum = intent().toBuilder().clearMembers();
        for (int i = 0; i < 64; i++) maximum.addMembers(member().toBuilder().setMemberId("m" + i)
                .setDestination(condition().toBuilder().setAddress(address().toBuilder().setDocId("d" + i))));
        check(maximum.build(), true);
        check(maximum.addMembers(member()).build(), false);
    }

    @Test void explicitPreconditionAndAllAddressCoordinatesAreRequired() throws Exception {
        check(condition().toBuilder().clearCondition().build(), false);
        check(condition().toBuilder().setIfAbsent(false).build(), false);
        check(condition().toBuilder().setExpectedMutationRevision(0).build(), false);
        check(condition().toBuilder().setExpectedMutationRevision(-1).build(), false);
        check(condition().toBuilder().setExpectedMutationRevision(Long.MAX_VALUE).build(), true);
        check(condition().toBuilder().clearAddress().build(), false);
        for (var field : NodeAddress.getDescriptor().getFields()) {
            check(condition().toBuilder().setAddress(address().toBuilder().setField(field, " \t")).build(), false);
        }
    }

    @Test void accountMustMatchDestinationsOwnershipAndSources() throws Exception {
        check(intent().toBuilder().setAccountId("other").build(), false);
        check(intent().toBuilder().setMembers(0, member().toBuilder().setOwnership(member().getOwnership()
                .toBuilder().setAccountId("other"))).build(), false);
        var foreign = condition().toBuilder().setExpectedMutationRevision(7)
                .setAddress(address().toBuilder().setAccountId("other")).build();
        check(intent().toBuilder().setMembers(0, member().toBuilder().addSources(foreign)).build(), false);
        check(intent().toBuilder().setMembers(0, member().toBuilder().setParts(0, core().toBuilder()
                .setReuse(reuse().toBuilder().setSource(foreign)))).build(), false);
    }

    @Test void requiresOwnershipAndLogicalDrive() throws Exception {
        check(member().toBuilder().clearOwnership().build(), false);
        check(member().toBuilder().setOwnership(member().getOwnership().toBuilder().clearSecurity()).build(), false);
        check(member().toBuilder().setOwnership(member().getOwnership().toBuilder().setDatasourceId(" ")).build(), false);
        check(member().toBuilder().clearDriveId().build(), false);
        check(member().toBuilder().setDriveId("drive-name-is-not-id").build(), false);
        check(member().toBuilder().clearMemberId().build(), false);
        check(member().toBuilder().addSources(condition()).build(), false);
        check(member().toBuilder().addSources(condition().toBuilder().setExpectedMutationRevision(1)).build(), true);
    }

    @Test void coreAndPartSelectionAreExplicit() throws Exception {
        check(member().toBuilder().clearParts().build(), false);
        check(member().toBuilder().addParts(core()).build(), false);
        check(member().toBuilder().setParts(0, core().toBuilder().setEmpty(true)).build(), false);
        check(core().toBuilder().clearContent().build(), false);
        check(core().toBuilder().clearSlot().build(), false);
        check(core().toBuilder().setEmpty(false).build(), false);
        check(slot(DocumentPart.DOCUMENT_PART_CORE, "extra"), false);
        check(slot(DocumentPart.DOCUMENT_PART_CHUNKS, ""), false);
        check(slot(DocumentPart.DOCUMENT_PART_CHUNKS, " \t"), false);
        check(slot(DocumentPart.DOCUMENT_PART_CHUNKS, "set-1"), true);
        check(slot(DocumentPart.DOCUMENT_PART_UNSPECIFIED, ""), false);
        check(core().getSlot().toBuilder().setPartValue(99).build(), false);
    }

    @Test void explicitRowKindAndLifecyclePolicyAreConsistent() throws Exception {
        check(member().toBuilder().clearRowKind().build(), false);
        check(member().toBuilder().setRowKindValue(99).build(), false);
        check(member().toBuilder().setDeleteSourceBlobsOnSettle(true).setSourceBlobDeleteReason("settled").build(), false);
        var intake = member().toBuilder().setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_INTAKE)
                .setDestination(condition().toBuilder().setAddress(address().toBuilder()
                        .setGraphId("intake:account").setGraphAddressId("source")));
        check(intake.build(), true);
        check(intake.clone().setClusterId("cluster").build(), false);
        check(intake.clone().setSourceBlobDeleteReason("settled").build(), false);
        check(intake.clone().setDeleteSourceBlobsOnSettle(true).build(), false);
        check(intake.clone().setDeleteSourceBlobsOnSettle(true).setSourceBlobDeleteReason("settled").build(), true);
    }

    @Test void uploadClaimsHaveBoundsButDoNotProveActualBytes() throws Exception {
        check(upload(), true); // Zero bytes is PRESENT, not an EMPTY slot.
        check(upload().toBuilder().setSizeBytes(-1).build(), false);
        check(upload().toBuilder().clearSha256().build(), false);
        check(upload().toBuilder().setSha256("A".repeat(64)).build(), false);
        check(upload().toBuilder().setSha256("a".repeat(63)).build(), false);
        check(upload().toBuilder().setContentType(" ").build(), false);
        // A well-shaped digest may be semantically false. Provider verification is mandatory.
        check(upload().toBuilder().setSha256("b".repeat(64)).build(), true);
    }

    @Test void retainedIdentityRequiresExactOriginalCoordinates() throws Exception {
        check(reuse().toBuilder().setSource(condition()).build(), false);
        check(reuse().toBuilder().clearSourceSlot().build(), false);
        check(reuse().toBuilder().clearObject().build(), false);
        for (String field : new String[]{"object_id", "backend_generation", "storage_realm", "namespace", "object_key", "sha256", "content_type"}) {
            var identity = reuse().getObject().toBuilder().clearField(PublicationObjectIdentity.getDescriptor().findFieldByName(field));
            check(reuse().toBuilder().setObject(identity).build(), false);
        }
        check(reuse().toBuilder().setObject(reuse().getObject().toBuilder().setProviderVersion("")).build(), false);
        check(reuse().toBuilder().setObject(reuse().getObject().toBuilder().setSizeBytes(-1)).build(), false);
    }

    @Test void schemaBindsTypeAndExactClosure() throws Exception {
        var schema = PublicationSchemaCondition.newBuilder().setTypeName("example.Case").setDescriptorFingerprint(SHA);
        check(member().toBuilder().setStructuredSchema(schema).build(), true);
        check(schema.clone().clearDescriptorFingerprint().build(), false);
        check(schema.clone().setTypeName("type.googleapis.com/example.Case").build(), false);
        check(schema.clone().setTypeName(".example.Case").build(), false);
    }

    private static PublicationHistoricalReuse historical() {
        return PublicationHistoricalReuse.newBuilder().setSource(address()).setRevisionId(UUID)
                .setRevisionOrdinal(0).setSourceSlot(core().getSlot()).setObject(reuse().getObject()).build();
    }

    @Test void historicalSelectionIsExactAndBoundedForGeneratedAndDynamicMessages() throws Exception {
        check(historical(), true);
        check(historical().toBuilder().clearSource().build(), false);
        check(historical().toBuilder().clearRevisionId().build(), false);
        check(historical().toBuilder().setRevisionId("latest").build(), false);
        check(historical().toBuilder().setRevisionId("ABCDEF00-0000-4000-8000-000000000001").build(), false);
        check(historical().toBuilder().setRevisionOrdinal(-1).build(), false);
        check(historical().toBuilder().setRevisionOrdinal(9999).build(), true);
        check(historical().toBuilder().setRevisionOrdinal(10000).build(), false);
        check(historical().toBuilder().clearSourceSlot().build(), false);
        check(historical().toBuilder().clearObject().build(), false);
        for (var field : NodeAddress.getDescriptor().getFields())
            check(historical().toBuilder().setSource(address().toBuilder().setField(field, " \t")).build(), false);
        // A well-shaped nonexistent revision/object remains a handler membership check.
        check(historical().toBuilder().setRevisionId("10000000-0000-4000-8000-000000000099")
                .setObject(reuse().getObject().toBuilder().setSha256("b".repeat(64))).build(), true);
    }

    @Test void historicalSelectionPreservesAccountSlotAndExclusiveContent() throws Exception {
        var part = core().toBuilder().setHistoricalReuse(historical()).build();
        assertThat(part.hasUpload()).isFalse();
        assertThat(part.hasReuse()).isFalse();
        check(part, true);
        check(intent().toBuilder().setMembers(0, member().toBuilder().setParts(0, part)).build(), true);
        check(part.toBuilder().setSlot(slot(DocumentPart.DOCUMENT_PART_PARSED, "")).build(), false);
        var foreign = historical().toBuilder().setSource(address().toBuilder().setAccountId("other"));
        check(intent().toBuilder().setMembers(0, member().toBuilder().setParts(0,
                part.toBuilder().setHistoricalReuse(foreign))).build(), false);
        var selectedUpload = part.toBuilder().setUpload(upload()).build();
        assertThat(selectedUpload.hasHistoricalReuse()).isFalse();
        check(selectedUpload, true);
        assertThat(DocumentPublicationPart.getDescriptor().findFieldByName("historical_reuse").getNumber()).isEqualTo(5);
        assertThat(Any.pack(historical()).getTypeUrl())
                .isEqualTo("type.googleapis.com/ai.protomolt.proto.repo.v1.PublicationHistoricalReuse");
    }

    @Test void historicalProjectionExposesBoundsAndRuntimeOnlyConstraints() {
        var schema = new ObjectMapper().valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(PublicationHistoricalReuse.getDescriptor()));
        var ordinal = schema.at("/properties/revisionOrdinal");
        assertThat(ordinal.has("minimum")).isTrue();
        assertThat(ordinal.path("minimum").asInt()).isZero();
        assertThat(ordinal.path("exclusiveMaximum").asInt()).isEqualTo(10000);
        assertThat(schema.path("required").toString()).contains("source", "sourceSlot", "object");
        assertThat(schema.path("x-protomolt-cel").toString()).contains("publication-historical-address");
    }

    @Test void metadataBoundsApplyToKeysAndValues() throws Exception {
        check(member().toBuilder().putMetadata("label", "value").build(), true);
        check(member().toBuilder().putMetadata("", "value").build(), false);
        check(member().toBuilder().putMetadata("label", "x".repeat(4097)).build(), false);
        var maximum = member().toBuilder();
        for (int i = 0; i < 128; i++) maximum.putMetadata("k" + i, "value");
        check(maximum.build(), true);
        check(maximum.putMetadata("excess", "value").build(), false);
    }

    @Test void oneofWireShapeAndAnyNameAreStable() {
        var selected = core().toBuilder().setReuse(reuse()).build();
        assertThat(selected.hasUpload()).isFalse();
        assertThat(selected.hasReuse()).isTrue();
        assertThat(Any.pack(intent()).getTypeUrl())
                .isEqualTo("type.googleapis.com/ai.protomolt.proto.repo.v1.DocumentPublicationIntent");
        assertThat(DocumentPublicationIntent.getDescriptor().getFile().getServices()).isEmpty();
    }

    @Test void schemaProjectionExposesBoundsAndMarksRuntimeCel() {
        var schema = new ObjectMapper().valueToTree(ProtoJsonSchemaGenerator.create()
                .generateRooted(DocumentPublicationIntent.getDescriptor()));
        assertThat(schema.at("/properties/members/minItems").asInt()).isEqualTo(1);
        assertThat(schema.at("/properties/members/maxItems").asInt()).isEqualTo(64);
        assertThat(schema.path("x-protomolt-cel").toString()).contains("publication-account");
        // CEL extension documents rules; ordinary JSON Schema validators do not execute them.
    }
}
