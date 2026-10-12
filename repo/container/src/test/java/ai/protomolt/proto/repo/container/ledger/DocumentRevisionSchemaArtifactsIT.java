package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL constraint tests with synthetic native publications, not typed/provider qualification. */
@Testcontainers
class DocumentRevisionSchemaArtifactsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void migrationPreservesPublishedRevisionsWithoutInventingSchemaEvidence() {
        try (var c = context(POSTGRES, "54")) {
            var p = prepare(c, 2);
            stage(c, p.owner());
            var result = publish(c, p, Fault.NONE, em -> {});
            var before = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id
                    """).getResultList());
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            var after = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id
                    """).getResultList());
            // Preparation publishes one legacy source per member; native publication adds one more.
            assertThat(after).isEqualTo(before).hasSize(2 * result.getMembersCount());
            assertThat(count(c, "document_revision_commits")).isEqualTo(2);
            assertThat(count(c, "repository_operation_success")).isEqualTo(1);
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(1);
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
        }
    }

    @Test void upgradeSeparatesPayloadsWhilePreservingIdentitiesAndAssociations() {
        try (var c = context(POSTGRES, "119")) {
            var p = prepare(c, 2);
            var descriptor = descriptorBytes("shared.proto");
            var metadata = descriptorBytes("meta.proto");
            String descriptorHash = stageBytes(c, p.owner(), descriptor);
            String metadataHash = stageBytes(c, p.owner(), metadata);
            var other = otherOwner(c);
            assertThat(stageBytes(c, other, descriptor)).isEqualTo(descriptorHash);
            publish(c, p, Fault.NONE, false, (em, revision) -> {
                insert(em, revision, p.owner(), descriptorHash);
                insert(em, revision, p.owner(), metadataHash);
                insertEvidence(em, revision, p.owner());
                bindAsset(em, revision, p.owner(), descriptorHash, metadataHash);
            }, em -> {});
            var publications = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id").getResultList());
            var receipts = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM repository_operation_success r ORDER BY account_id,principal,operation_id").getResultList());
            var identities = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT account_id,encode(artifact_sha256,'hex'),creator_principal,creator_operation::text,creator_generation,"
                    + "encode(artifact_bytes,'hex'),size_bytes FROM repository_schema_artifacts ORDER BY 1,2").getResultList());
            var claims = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM repository_schema_artifact_claims r ORDER BY 1").getResultList());
            var references = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_schema_artifacts r ORDER BY 1").getResultList());
            var evidence = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_schema_evidence r ORDER BY 1").getResultList());
            var assets = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_schema_assets r ORDER BY 1").getResultList());
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(2);
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(4);
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            var publicationsAfter = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id").getResultList());
            assertThat(publicationsAfter).isEqualTo(publications);
            var receiptsAfter = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM repository_operation_success r ORDER BY account_id,principal,operation_id").getResultList());
            assertThat(receiptsAfter).isEqualTo(receipts);
            var claimsAfter = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM repository_schema_artifact_claims r ORDER BY 1").getResultList());
            assertThat(claimsAfter).isEqualTo(claims);
            var referencesAfter = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_schema_artifacts r ORDER BY 1").getResultList());
            assertThat(referencesAfter).isEqualTo(references);
            var evidenceAfter = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_schema_evidence r ORDER BY 1").getResultList());
            assertThat(evidenceAfter).isEqualTo(evidence);
            var assetsAfter = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_schema_assets r ORDER BY 1").getResultList());
            assertThat(assetsAfter).isEqualTo(assets);
            // One normalized payload per recorded identity, with matching bytes, digest and size.
            var payloads = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT account_id,encode(artifact_sha256,'hex'),encode(payload_bytes,'hex'),size_bytes,"
                    + "artifact_sha256=sha256(payload_bytes) FROM repository_schema_artifact_payloads ORDER BY 1,2").getResultList());
            assertThat(payloads).hasSize(identities.size());
            for (int row = 0; row < identities.size(); row++) {
                var identity = (Object[]) identities.get(row);
                var payload = (Object[]) payloads.get(row);
                assertThat(payload[0]).isEqualTo(identity[0]);
                assertThat(payload[1]).isEqualTo(identity[1]);
                assertThat(payload[2]).isEqualTo(identity[5]);
                assertThat(((Number) payload[3]).longValue()).isEqualTo(((Number) identity[6]).longValue());
                assertThat(payload[4]).isEqualTo(Boolean.TRUE);
            }
            // Catalog identities survive; only the redundant byte columns are gone.
            var identitiesAfter = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT account_id,encode(artifact_sha256,'hex'),creator_principal,creator_operation::text,creator_generation"
                    + " FROM repository_schema_artifacts ORDER BY 1,2").getResultList());
            assertThat(identitiesAfter).hasSize(identities.size());
            var removedColumns = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema()"
                    + " AND table_name='repository_schema_artifacts' AND column_name IN ('artifact_bytes','size_bytes')")
                    .getResultList());
            assertThat(removedColumns).isEmpty();
            var digestChecks = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT count(*) FROM pg_constraint WHERE conrelid='repository_schema_artifact_payloads'::regclass"
                    + " AND contype='c' AND pg_get_constraintdef(oid) LIKE '%sha256(payload_bytes)%'").getSingleResult());
            assertThat(digestChecks).isEqualTo(1L);
            // Supported readers serve the migrated payloads with the same ownership checks.
            var storage = new RepositorySchemaArtifacts(c.tx());
            assertThat(storage.readRetained(p.owner(), descriptorHash, () -> {})).isEqualTo(descriptor);
            assertThat(storage.readRetained(other, descriptorHash, () -> {})).isEqualTo(descriptor);
            assertThat(storage.readRetained(p.owner(), metadataHash, () -> {})).isEqualTo(metadata);
            var foreign = new RepositoryOperationLedger(c.tx()).admit(
                    new RepositoryOperationLedger.Key("foreign", "principal", UUID.randomUUID()),
                    new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("fixture")),
                    UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
            assertThatThrownBy(() -> storage.readRetained(foreign, descriptorHash, () -> {}))
                    .hasMessageContaining("unavailable");
        }
    }

    @Test void revisionsShareNormalizedBytesAndReferencesAreImmutable() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2);
            String hash = stage(c, p.owner());
            var result = publish(c, p, Fault.NONE, false,
                    (em, revision) -> insert(em, revision, p.owner(), hash), em -> {});
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(1);
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(2);
            assertThat(result.getMembersCount()).isEqualTo(2);
            for (String statement : List.of("DELETE FROM document_revision_schema_artifacts",
                    "UPDATE document_revision_schema_artifacts SET principal=principal")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(statement).executeUpdate(); }))
                        .hasStackTraceContaining("Revision schema references are immutable");
            }
            var foreignTables = c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT confrelid::regclass::text FROM pg_constraint
                    WHERE conrelid='document_revision_schema_artifacts'::regclass AND contype='f'
                    """).getResultList());
            assertThat(foreignTables).contains("repository_schema_artifacts", "document_revision_publications", "document_revision_commits")
                    .doesNotContain("repository_schema_artifact_claims");
        }
    }

    @Test void aDifferentOperationsClaimCannotSupplyTheCurrentCommit() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var other = otherOwner(c);
            String hash = stage(c, other);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false,
                    (em, revision) -> insert(em, revision, p.owner(), hash), em -> {}))
                    .hasStackTraceContaining("requires a current owner claim");
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
            assertThat(count(c, "repository_operation_success")).isZero();
            assertThat(count(c, "document_revision_commits")).isZero();
        }
    }

    @Test void evenAFencedOtherOwnerCannotAttachToThisRevision() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var other = otherOwner(c);
            String hash = stage(c, other);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                RepositoryOperationLedger.fenceLiveOwner(em, other);
                insert(em, revision, other, hash);
            }, em -> {})).hasStackTraceContaining("differs from native commit owner or transaction");
        }
    }

    @Test void wrongAccountPrincipalOrGenerationCannotBorrowTheWriteFence() {
        for (int variant = 0; variant < 4; variant++) {
            try (var c = context(POSTGRES)) {
                var p = prepare(c, 1);
                String hash = stage(c, p.owner());
                var key = p.owner().key();
                // Real admitted operations under the foreign account and principal: their scopes exist,
                // so exactly one binding differs from the owner fenced in this transaction.
                foreignScopedOwner(c, "foreign", key.principal(), key.operationId());
                foreignScopedOwner(c, key.account(), "foreign", key.operationId());
                int choice = variant;
                assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                    insert(em, revision, choice == 0 ? "foreign" : choice == 3 ? "unscoped" : key.account(),
                            choice == 1 ? "foreign" : key.principal(), key.operationId(),
                            choice == 2 ? p.owner().generation() + 1 : p.owner().generation(), hash);
                }, em -> {})).hasStackTraceContaining(choice == 3 ? "Repository execution scope is absent"
                        : "live owner write fence");
                assertThat(count(c, "document_revision_schema_artifacts")).isZero();
                assertThat(count(c, "document_revision_commits")).isZero();
            }
        }
    }

    @Test void sealingAndTerminalSuccessBothCloseReferenceInsertion() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            String hash = stage(c, p.owner());
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                em.createNativeQuery("UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=:id")
                        .setParameter("id", revision).executeUpdate();
                insert(em, revision, p.owner(), hash);
            }, em -> {})).hasStackTraceContaining("current unsealed native projection");
            var id = new java.util.concurrent.atomic.AtomicReference<UUID>();
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> id.set(revision),
                    em -> insert(em, id.get(), p.owner(), hash))).hasStackTraceContaining("terminal");
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
            var result = publish(c, p, Fault.NONE, em -> {});
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                insert(em, UUID.fromString(result.getMembers(0).getRevisionId()), p.owner(), hash);
            })).hasStackTraceContaining("terminal");
        }
    }

    @Test void missingArtifactAndAbortedPublicationLeaveNoReferences() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false,
                    (em, revision) -> insert(em, revision, p.owner(), "0".repeat(64)), em -> {}))
                    .hasStackTraceContaining("artifact is missing");
            String hash = stage(c, p.owner());
            assertThatThrownBy(() -> publish(c, p, Fault.OMIT_OUTCOME, false,
                    (em, revision) -> insert(em, revision, p.owner(), hash), em -> {})).isInstanceOf(RuntimeException.class);
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
            assertThat(count(c, "document_revision_commits")).isZero();
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(1);
            publish(c, p, Fault.NONE, false, (em, revision) -> insert(em, revision, p.owner(), hash), em -> {});
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(1);
        }
    }

    @Test void expirationRefusesInsertionButDoesNotRemoveCommittedReferences() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1, false, Duration.ofSeconds(10));
            String hash = stage(c, p.owner());
            publish(c, p, Fault.NONE, false, (em, revision) -> insert(em, revision, p.owner(), hash), em -> {});
            c.tx().readOnly(em -> { expire(em, p.owner()); return null; });
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(1);
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(1);
        }
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1, false, Duration.ofSeconds(10));
            String hash = stage(c, p.owner());
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                expire(em, p.owner());
                insert(em, revision, p.owner(), hash);
            }, em -> {})).hasStackTraceContaining("live owner write fence");
            assertThat(count(c, "document_revision_schema_artifacts")).isZero();
        }
    }

    private static RepositoryOperationLedger.Owner otherOwner(Context c) {
        return new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account", "principal", UUID.randomUUID()),
                new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("fixture")),
                UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
    }
    private static String stage(Context c, RepositoryOperationLedger.Owner owner) {
        var descriptor = DescriptorProtos.FileDescriptorSet.newBuilder().addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("retained.proto").setSyntax("proto3").setPackage("retained")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Record"))).build().toByteString();
        return new RepositorySchemaArtifacts(c.tx()).stage(owner, List.of(descriptor), () -> {}).getFirst();
    }
    private static ByteString descriptorBytes(String name) {
        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(name).setSyntax("proto3").setPackage("migrated")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Record"))).build().toByteString();
    }
    private static String stageBytes(Context c, RepositoryOperationLedger.Owner owner, ByteString bytes) {
        return new RepositorySchemaArtifacts(c.tx()).stage(owner, List.of(bytes), () -> {}).getFirst();
    }
    private static void insertEvidence(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner) {
        var raw = fragment(em, revision, 0);
        byte[] evidence = new byte[8];
        em.createNativeQuery("""
                INSERT INTO document_revision_schema_evidence(revision_id,revision_ordinal,account_id,principal,operation_id,
                 owner_generation,root_locator_sha256,fragment_sha256,fragment_size,evidence_codec,evidence_version,evidence_bytes,evidence_sha256)
                VALUES(:r,0,:a,:p,:op,:g,:loc,:fh,:fs,'document-root-schema-evidence',1,:b,:eh)
                """).setParameter("r", revision).setParameter("a", owner.key().account())
                .setParameter("p", owner.key().principal()).setParameter("op", owner.key().operationId()).setParameter("g", owner.generation())
                .setParameter("loc", digest("locator-" + revision)).setParameter("fh", raw.hash).setParameter("fs", raw.size)
                .setParameter("b", evidence).setParameter("eh", sha(evidence)).executeUpdate();
    }
    private static void bindAsset(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner,
            String descriptorHash, String metadataHash) {
        String url = "type.test/migrated.Record";
        em.createNativeQuery("""
                INSERT INTO document_revision_schema_assets(revision_id,account_id,principal,operation_id,owner_generation,
                 type_url,type_url_sha256,descriptor_sha256,metadata_sha256,metadata_codec,metadata_version,source_sha256)
                VALUES(:r,:a,:p,:op,:g,:url,:uh,:dh,:mh,'repository-schema-asset',1,NULL)
                """).setParameter("r", revision).setParameter("a", owner.key().account())
                .setParameter("p", owner.key().principal()).setParameter("op", owner.key().operationId())
                .setParameter("g", owner.generation()).setParameter("url", url)
                .setParameter("uh", sha(url.getBytes(StandardCharsets.UTF_8)))
                .setParameter("dh", HexFormat.of().parseHex(descriptorHash))
                .setParameter("mh", HexFormat.of().parseHex(metadataHash)).executeUpdate();
    }
    private static Raw fragment(EntityManager em, UUID revision, int ordinal) {
        Object[] row = (Object[]) em.createNativeQuery("""
                SELECT decode(o.expected_sha256,'hex'),o.expected_size FROM document_revision_parts p
                JOIN repository_physical_locations l ON l.object_id=p.object_id AND l.source_kind='DOCUMENT_PART'
                JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                WHERE p.revision_id=:r AND p.revision_ordinal=:o ORDER BY p.part LIMIT 1
                """).setParameter("r", revision).setParameter("o", ordinal).getSingleResult();
        return new Raw((byte[]) row[0], ((Number) row[1]).longValue());
    }
    private record Raw(byte[] hash, long size) {}
    private static byte[] digest(String input) { return sha(input.getBytes(StandardCharsets.UTF_8)); }
    private static byte[] sha(byte[] input) {
        try { return MessageDigest.getInstance("SHA-256").digest(input); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static void expire(EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                FROM repository_operation_owners WHERE operation_id=:id
                """).setParameter("id", owner.key().operationId()).getSingleResult();
    }
    private static void insert(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, String hash) {
        insert(em, revision, owner.key().account(), owner.key().principal(), owner.key().operationId(), owner.generation(), hash);
    }
    private static void insert(EntityManager em, UUID revision, String account, String principal, UUID operation, long generation, String hash) {
        em.createNativeQuery("""
                INSERT INTO document_revision_schema_artifacts(revision_id,account_id,principal,operation_id,owner_generation,artifact_sha256)
                VALUES(:revision,:account,:principal,:operation,:generation,:sha)
                """).setParameter("revision", revision).setParameter("account", account).setParameter("principal", principal)
                .setParameter("operation", operation).setParameter("generation", generation)
                .setParameter("sha", HexFormat.of().parseHex(hash)).executeUpdate();
    }
}
