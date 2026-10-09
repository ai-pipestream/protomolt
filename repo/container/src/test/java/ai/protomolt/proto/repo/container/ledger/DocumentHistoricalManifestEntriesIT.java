package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real immutable SQL manifests; physical provider observations are explicitly synthetic. */
@Testcontainers
class DocumentHistoricalManifestEntriesIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void preservesHistoricalProvenanceAndRefusesSubstitutionOrExpiredUse(boolean knownProducer) throws Exception {
        try (var c = context(POSTGRES)) {
            var caller = new RepositoryCaller("principal", true);
            var provenance = WriteProvenance.newBuilder().setModuleId("parser").setNodeId("original-worker")
                    .setGraphId("source-graph").setGraphVersion(7).build();
            var document = Document.newBuilder().setDocId("doc").setOwnership(OwnershipContext.newBuilder()
                    .setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()))
                    .setStructuredData(com.google.protobuf.Any.pack(com.google.protobuf.StringValue.of("retained"), "type.test")).build();
            var f = DocumentSchemaRetentionFixture.prepare(c, false, false, document, "node", "storage", knownProducer ? provenance : null);
            var revision = DocumentSchemaRetentionFixture.publish(c, f, (em, id) -> {});
            var address = f.command().intent().getMembers(0).getDestination().getAddress();
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(caller, address, revision);
            try (var use = history.use()) {
                var source = use.plan().entries().getFirst(); var part = source.part().part(); var binding = source.part().binding();
                var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()).build();
                var object = PublicationObjectIdentity.newBuilder().setObjectId(source.objectId().toString())
                        .setBackendGeneration(binding.generation()).setStorageRealm(binding.profile().storageRealm())
                        .setNamespace(binding.namespace()).setObjectKey(part.key()).setSizeBytes(part.size()).setSha256(part.sha256())
                        .setContentType(part.contentType()).setProviderVersion(part.providerVersion()).build();
                var selector = PublicationHistoricalReuse.newBuilder().setSource(address).setRevisionId(revision.toString())
                        .setRevisionOrdinal(source.revisionOrdinal()).setSourceSlot(slot).setObject(object).build();
                var member = f.command().intent().getMembers(0).toBuilder().clearParts()
                        .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address).setExpectedMutationRevision(1))
                        .addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setHistoricalReuse(selector)).build();
                var command = new DocumentPublicationCommand(f.command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).setMembers(0, member).build());
                var refs = List.of(DocumentHistoricalReferenceAdmission.prepare(history, use, List.of(selector), RepositoryReadControl.NONE));
                var retained = DocumentHistoricalManifestEntries.prepare(command, refs, () -> {});
                var placement = f.prepared().plan().members().getFirst().placement();
                var plan = DocumentUploadPlan.prepare(command, Map.of(placement.drive().id(), placement), Map.of(), refs, () -> {});
                var content = DocumentCommandContent.checkHistorical(command, "member", Map.of(0, document.toByteString()), false,
                        new DocumentRevisionAssembly.Limits(1_000_000, 10, 100, 100, 1_000_000), refs, () -> {});
                var physical = new DocumentCommitParts.Physical(source.objectId(), part.part().getNumber(), part.subKey(), part.key(),
                        part.size(), part.sha256(), part.contentType(), part.providerVersion(), part.etag(), binding.generation(),
                        binding.profile().storageRealm(), binding.namespace());
                var bound = new DocumentCommitParts.Bound(Map.of(new DocumentCommitParts.Slot("member", 0), physical), Map.of("member", 1L));
                var policy = f.batch().policy();
                var payload = DocumentSchemaRetentionFixture.definition(com.google.protobuf.StringValue.getDescriptor());
                var proof = policy.policy().prepareAndCheck(com.google.protobuf.ByteString.copyFrom(HexFormat.of().parseHex(command.sha256())),
                        command.intent().getMembers(0), Map.of(0, document.toByteString()),
                        DocumentSchemaRetentionFixture.definition(Document.getDescriptor()), ignored -> payload, () -> {});
                var proofs = Map.of("member", proof);
                var budget = new ai.protomolt.proto.repo.blob.spi.PayloadBudget(32L * 1024 * 1024);
                ai.protomolt.proto.repo.admission.DocumentAdmissionReservations reservations = bytes -> {
                    var lease = budget.reserve(bytes); return lease::close;
                };
                assertThatThrownBy(() -> DocumentSchemaBatch.prepare(command, policy, proofs, reservations, () -> {}))
                        .isInstanceOf(UnsupportedOperationException.class);
                assertThatThrownBy(() -> DocumentSchemaBatch.prepareHistorical(command, policy, proofs, reservations, List.of(), () -> {}))
                        .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                var schemas = DocumentSchemaBatch.prepareHistorical(command, policy, proofs, reservations, refs, () -> {});
                assertThat(budget.reservedBytes()).isZero();
                var manifest = DocumentSchemaManifest.prepare(schemas, "member", bound, () -> {});
                assertThat(manifest.decision()).isEqualTo("TYPED");
                var json = com.google.protobuf.Struct.newBuilder();
                com.google.protobuf.util.JsonFormat.parser().merge(manifest.json(), json);
                var manifestPart = json.getFieldsOrThrow("parts").getListValue().getValues(0).getStructValue();
                assertThat(manifestPart.getFieldsOrThrow("size").getStringValue()).isEqualTo(Long.toString(physical.size()));
                assertThat(manifestPart.getFieldsOrThrow("sha256").getStringValue()).isEqualTo(physical.sha256());
                assertThat(manifestPart.getFieldsOrThrow("object_id").getStringValue()).isEqualTo(physical.id().toString());
                assertThat(DocumentSchemaRetention.prepare(schemas, "member")).isNotNull();
                var prior = new DocumentLedger(c.tx()).findByNodeId(plan.members().getFirst().nodeId()).orElseThrow();
                var locked = Map.of(prior.nodeId, prior);
                var later = Instant.now().plusSeconds(1000);
                assertThatThrownBy(() -> DocumentCommitWriter.prepare(plan.members().getFirst(), content, bound, locked, Map.of(), later, () -> {}))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retained provenance");
                var candidate = DocumentCommitWriter.prepareHistorical(plan.members().getFirst(), content, bound, locked, Map.of(), retained, later, () -> {});
                var entry = candidate.row().readManifest().getParts(0);
                assertThat(entry).isEqualTo(use.plan().manifest().getParts(source.revisionOrdinal()));
                assertThat(entry.hasWrittenBy()).isEqualTo(knownProducer);
                if (knownProducer) assertThat(entry.getWrittenBy()).isEqualTo(provenance);
                assertThat(candidate.row().readManifest().getDocVersion()).isEqualTo(prior.readManifest().getDocVersion() + 1);
                assertThat(candidate.row().updatedAt).isEqualTo(later);
                var wrong = new DocumentCommitParts.Physical(UUID.randomUUID(), physical.part(), physical.subKey(), physical.key(), physical.size(),
                        physical.sha256(), physical.contentType(), physical.version(), physical.etag(), physical.generation(), physical.realm(), physical.namespace());
                assertThatThrownBy(() -> retained.select(selector, wrong, () -> {})).isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                var substituted = new DocumentCommitParts.Bound(Map.of(new DocumentCommitParts.Slot("member", 0), wrong), Map.of("member", 1L));
                assertThatThrownBy(() -> DocumentSchemaManifest.prepare(schemas, "member", substituted, () -> {}))
                        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("physical part differs");
                var wrongVersion = new DocumentCommitParts.Physical(physical.id(), physical.part(), physical.subKey(), physical.key(), physical.size(),
                        physical.sha256(), physical.contentType(), "another-provider-version", physical.etag(), physical.generation(), physical.realm(), physical.namespace());
                assertThatThrownBy(() -> retained.select(selector, wrongVersion, () -> {})).isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                assertThatThrownBy(() -> retained.select(selector.toBuilder().setRevisionId(UUID.randomUUID().toString()).build(), physical, () -> {}))
                        .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                var checks = new java.util.concurrent.atomic.AtomicInteger();
                assertThatThrownBy(() -> retained.select(selector, physical, () -> {
                    if (checks.incrementAndGet() == 2) use.close();
                })).isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(() -> retained.select(selector, physical, () -> {})).isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(() -> DocumentSchemaBatch.prepareHistorical(command, policy, proofs, reservations, refs, () -> {}))
                        .isInstanceOf(IllegalStateException.class);
                assertThat(budget.reservedBytes()).isZero();
            } finally {
                history.close(); assertThat(history.awaitDrained(Duration.ofSeconds(1))).isTrue(); history.release();
                reads.fence(); reads.attestLocalQuiescence();
            }
        }
    }
}
