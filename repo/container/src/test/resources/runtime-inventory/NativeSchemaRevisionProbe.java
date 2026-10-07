package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentHistoricalOperations;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.validate.CelRule;
import ai.protomolt.proto.validate.MessageRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.StringValue;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Real retained bytes with changing descriptor contracts and immutable metadata snapshots. */
public final class NativeSchemaRevisionProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source, javax.sql.DataSource database) throws Exception {
        run(tx, provider, source, database, false);
    }
    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            javax.sql.DataSource database, boolean reconciliationOnly) throws Exception {
        run(tx, provider, source, database, reconciliationOnly, false);
    }
    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            javax.sql.DataSource database, boolean reconciliationOnly, boolean selfSupersession) throws Exception {
        run(tx, provider, source, database, reconciliationOnly, selfSupersession, false);
    }
    static void run(Tx tx, AssessmentProviderProbe provider, AssessmentMixedReuseProbe.Source source,
            javax.sql.DataSource database, boolean reconciliationOnly, boolean selfSupersession, boolean overlap) throws Exception {
        var caller = new RepositoryCaller("principal", true);
        var budget = new PayloadBudget(128_000_000);
        var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
        var a = ObservedAssessmentProbe.asset(StringValue.getDescriptor());
        var b = nonemptySchema();
        require(!a.metadata().getArtifactSha256().equals(b.metadata().getArtifactSha256()), "different retained descriptors");
        require(a.metadata().getTypeUrl().equals(b.metadata().getTypeUrl()), "one Any URL can use different definitions across revisions");
        var definitions = List.of(a, b, b);
        var published = new ArrayList<DocumentPublishedRevision>();
        var commands = new ArrayList<DocumentPublicationCommand>();
        var registryCalls = new AtomicInteger();
        var drives = new DriveLedger(tx);
        var placements = Map.of(source.placement().drive().id(), new DocumentPublicationRuntime.Placement(
                drives.findById(source.placement().drive().id()).orElseThrow(), "assessment-s3", provider.profile()));
        var container = Optional.of(ObservedAssessmentProbe.asset(Document.getDescriptor()));
        try (var reader = new DocumentPartReader((generation, profile) -> {
            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "original versioned backend");
            return provider.store();
        }, 2, 16_000_000, budget)) {
            var runtime = new DocumentPublicationRuntime(tx, drives, reads, reader, budget,
                    (generation, profile) -> { throw new AssertionError("Schema/metadata-only revisions must not request an upload backend"); },
                    new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000),
                    new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)), 2, Duration.ofMillis(25),
                    Duration.ofMinutes(5), 1, 4_000_000, 1, false,
                    new DocumentPublicationRuntime.Assessments(Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")),
                            Duration.ofMinutes(2), Duration.ofSeconds(1)));
            try {
                var originalObjects = objects(tx, source.revision());
                var originalParts = List.copyOf(new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow()
                        .readManifest().getPartsList());
                var opaqueCommand = AssessmentMixedReuseProbe.command(allReuse(tx, source));
                var opaqueRevision = runtime.execute(caller, opaqueCommand, placements, Map.of(), Map.of(),
                        Map.of("a", DocumentPublicationRuntime.Mode.OPAQUE), Optional.empty(),
                        (authenticated, selected, occurrence) -> { throw new AssertionError("Opaque publication must not resolve schemas"); },
                        RepositoryReadControl.NONE).getMembers(0);
                for (int phase = 0; phase < definitions.size(); phase++) {
                    var member = allReuse(tx, source);
                    if (phase == 2) member = member.toBuilder().setClusterId("archive-routing-v2").build();
                    var command = AssessmentMixedReuseProbe.command(member);
                    var definition = definitions.get(phase);
                    var result = runtime.execute(caller, command, placements, Map.of(), Map.of(),
                            Map.of("a", DocumentPublicationRuntime.Mode.TYPED), container,
                            (authenticated, selected, occurrence) -> { registryCalls.incrementAndGet(); return definition; }, RepositoryReadControl.NONE);
                    var revision = result.getMembers(0);
                    var id = UUID.fromString(revision.getRevisionId());
                    published.add(revision); commands.add(command);
                    require(objects(tx, id).equals(originalObjects), "same original physical objects across revisions");
                    require(descriptor(tx, id, a.metadata().getTypeUrl()).equals(definition.metadata().getArtifactSha256()),
                            "each revision pins its own descriptor under the same Any URL");
                    require(descriptor(tx, id, container.orElseThrow().metadata().getTypeUrl()).equals(
                            container.orElseThrow().metadata().getArtifactSha256()), "container schema remains retained");
                    var parts = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow().readManifest().getPartsList();
                    require(parts.equals(originalParts), "provider versions and per-part provenance survive unchanged from source");
                    require(Objects.equals(cluster(tx, id), phase == 2 ? "archive-routing-v2" : null), "exact metadata snapshot");
                    long attempts = tx.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM document_part_attempts WHERE operation_id=:op")
                            .setParameter("op", command.operationId()).getSingleResult()).longValue());
                    require(attempts == 0, "no empty upload attempt for unchanged bytes");
                }
                if (overlap) HistoricalAssessmentCreationProbe.overlappingGenerations(tx, provider, source, published.getFirst(), database);
                else if (selfSupersession) HistoricalAssessmentCreationProbe.selfSupersession(tx, provider, source, published.getFirst(), database);
                else HistoricalAssessmentCreationProbe.run(tx, provider, source, published.getFirst(), opaqueRevision, database, reconciliationOnly);
                NativeHistoricalMaterializationProbe.run(tx, provider, published.getFirst());
                require(registryCalls.get() == 3, "one explicit contract selection for each revision");
                for (var definition : List.of(a, b)) {
                    long catalogRows = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                            SELECT count(*) FROM repository_schema_artifacts WHERE account_id='account'
                            AND artifact_sha256=decode(:sha,'hex')
                            """).setParameter("sha", definition.metadata().getArtifactSha256()).getSingleResult()).longValue());
                    require(catalogRows == 1, "normalized schema bytes shared by immutable revision references");
                }
                var current = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow();
                var invalid = ObservedAssessmentProbe.invalidSchema();
                var refused = AssessmentMixedReuseProbe.command(allReuse(tx, source).toBuilder().setClusterId("archive-routing-v2").build());
                try {
                    runtime.execute(caller, refused, placements, Map.of(), Map.of(), Map.of("a", DocumentPublicationRuntime.Mode.TYPED),
                            container, (authenticated, member, occurrence) -> invalid, RepositoryReadControl.NONE);
                    throw new AssertionError("Invalid replacement schema advanced the document");
                } catch (DocumentPublicationRuntime.Rejected expected) {
                    require(expected.receipt().hasAssessment(), "invalid replacement retains its own assessment");
                    require(new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow().mutationRevision == current.mutationRevision,
                            "failed contract change leaves current version intact");
                }
                var history = new DocumentHistoricalOperations(reads, reader, budget);
                Document originalDocument = null;
                for (int index = 0; index < published.size(); index++) {
                    var revision = published.get(index);
                    var id = UUID.fromString(revision.getRevisionId());
                    try (var archived = history.readValidated(caller, revision.getAddress(), id, RepositoryReadControl.NONE)) {
                        if (originalDocument == null) originalDocument = archived.document();
                        else require(archived.document().equals(originalDocument), "historical decode uses unchanged original payload bytes");
                        require(archived.commandSha256().equals(com.google.protobuf.ByteString.copyFrom(
                                HexFormat.of().parseHex(commands.get(index).sha256()))), "historical proof binds its original command");
                        require(archived.publicationRevision() == revision.getMutationRevision(), "exact historical revision identity");
                        require(archived.metadata().hasKnown(), "native history exposes its recorded metadata");
                        var metadata = archived.metadata().getKnown();
                        require(metadata.getAccountId().equals(revision.getAddress().getAccountId()), "historical metadata ownership");
                        require(metadata.hasClusterId() == (index == 2), "historical null routing stays absent");
                        if (index == 2) require(metadata.getClusterId().equals("archive-routing-v2"), "historical routing snapshot");
                    }
                    require(descriptor(tx, id, a.metadata().getTypeUrl()).equals(definitions.get(index).metadata().getArtifactSha256()),
                            "new schema and metadata do not rewrite earlier schema bindings");
                    require(Objects.equals(cluster(tx, id), index == 2 ? "archive-routing-v2" : null),
                            "later publication does not rewrite earlier metadata snapshots");
                }
                require(registryCalls.get() == 3, "historical reads use retained schemas without registry calls");
                require(originalDocument.getStructuredData().unpack(StringValue.class).getValue().equals("retained payload"), "real payload retained");
                HistoricalPublicationProbe.run(tx, provider, source, published.getFirst(), database);
                HistoricalMixedPublicationProbe.run(tx, provider, source, published.getFirst(), container.orElseThrow(), a);
            } finally {
                boolean stopped = false;
                for (int pass = 0; pass < 4 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(5));
                require(stopped && budget.reservedBytes() == 0 && reads.outstandingReads() == 0, "schema revision host drains");
            }
        }
        System.out.println("NATIVE_SCHEMA_METADATA_REVISIONS_OK");
    }

    private static DocumentPublicationMember allReuse(Tx tx, AssessmentMixedReuseProbe.Source source) {
        var current = new DocumentLedger(tx).findByNodeId(source.node()).orElseThrow();
        var publication = new DocumentPublicationLedger(tx).findForRead(current).orElseThrow();
        var originalAddress = source.candidate().getPartsList().stream().filter(part -> part.hasReuse()).findFirst().orElseThrow()
                .getReuse().getSource().getAddress();
        var condition = DocumentRevisionCondition.newBuilder().setAddress(originalAddress).setExpectedMutationRevision(current.mutationRevision).build();
        var member = source.candidate().toBuilder().setDestination(condition).clearParts();
        for (var declaration : source.candidate().getPartsList()) {
            // The shared mixed-input fixture adds an artificial empty slot; a pure
            // schema/metadata change preserves only the original manifest's slots.
            if (declaration.hasEmpty()) continue;
            var bound = publication.boundParts().stream().filter(part -> part.part().part() == declaration.getSlot().getPart()
                    && part.part().subKey().equals(declaration.getSlot().getSubKey())).findFirst().orElseThrow();
            var part = bound.part();
            var object = tx.readOnly(em -> em.createNativeQuery("""
                    SELECT object_id FROM document_revision_parts WHERE revision_id=:revision AND part=:part AND sub_key=:key
                    """).setParameter("revision", publication.revisionId()).setParameter("part", part.part().getNumber())
                    .setParameter("key", part.subKey()).getSingleResult().toString());
            var identity = PublicationObjectIdentity.newBuilder().setObjectId(object).setBackendGeneration(bound.binding().generation())
                    .setStorageRealm(bound.binding().profile().storageRealm()).setNamespace(bound.binding().namespace())
                    .setObjectKey(part.key()).setProviderVersion(part.providerVersion()).setSizeBytes(part.size())
                    .setSha256(part.sha256()).setContentType(part.contentType());
            member.addParts(declaration.toBuilder().clearUpload().clearReuse().setReuse(PublicationReuse.newBuilder()
                    .setSource(condition).setSourceSlot(declaration.getSlot()).setObject(identity)));
        }
        return member.build();
    }
    private static List<?> objects(Tx tx, UUID revision) {
        return tx.readOnly(em -> em.createNativeQuery("SELECT object_id FROM document_revision_parts WHERE revision_id=:id ORDER BY part,sub_key")
                .setParameter("id", revision).getResultList());
    }
    private static String cluster(Tx tx, UUID revision) {
        return tx.readOnly(em -> (String) em.createNativeQuery(
                "SELECT metadata_snapshot->>'cluster_id' FROM document_revision_commits WHERE revision_id=:id")
                .setParameter("id", revision).getSingleResult());
    }
    private static String descriptor(Tx tx, UUID revision, String url) {
        return tx.readOnly(em -> (String) em.createNativeQuery("""
                SELECT encode(descriptor_sha256,'hex') FROM document_revision_schema_assets WHERE revision_id=:id AND type_url=:url
                """).setParameter("id", revision).setParameter("url", url).getSingleResult());
    }
    private static DocumentSchemaAdmission.Definition nonemptySchema() throws Exception {
        var proto = StringValue.getDescriptor().getFile().toProto().toBuilder().addDependency(ValidateProto.getDescriptor().getName());
        for (var type : proto.getMessageTypeBuilderList()) if (type.getName().equals("StringValue"))
            type.setOptions(type.getOptions().toBuilder().setExtension(ValidateProto.message, MessageRules.newBuilder()
                    .addCel(CelRule.newBuilder().setId("archive-nonempty").setExpression("this.size() > 0")).build()));
        return ObservedAssessmentProbe.asset(com.google.protobuf.Descriptors.FileDescriptor.buildFrom(proto.build(),
                new com.google.protobuf.Descriptors.FileDescriptor[]{ValidateProto.getDescriptor()}).findMessageTypeByName("StringValue"));
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
