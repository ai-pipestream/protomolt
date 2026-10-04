package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL storage tests with explicitly synthetic metadata and bytes, not compiler qualification. */
@Testcontainers
class DocumentRevisionSchemaAssetsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final int METADATA_LIMIT = 512 * 1024;

    @Test void sharedCatalogBytesBindToTwoRevisionsAndBindingsStayImmutable() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 2);
            var assets = stage(c, p.owner(), metadata(16), true);
            var result = publish(c, p, Fault.NONE, false, (em, revision) -> {
                referenceAssets(em, revision, p.owner(), assets);
                bind(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor);
            }, em -> {});
            assertThat(result.getMembersCount()).isEqualTo(2);
            assertThat(count(c, "repository_schema_artifacts")).isEqualTo(3);
            assertThat(count(c, "document_revision_schema_artifacts")).isEqualTo(6);
            assertThat(count(c, "document_revision_schema_assets")).isEqualTo(2);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(
                    "UPDATE document_revision_schema_assets SET type_url=type_url").executeUpdate(); }))
                    .hasStackTraceContaining("Revision schema asset bindings are immutable");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(
                    "DELETE FROM document_revision_schema_assets").executeUpdate(); }))
                    .hasStackTraceContaining("Revision schema asset bindings are immutable");
        }
    }

    @Test void allDescriptorMetadataAndOptionalSourceReferencesMustExist() {
        for (int omitted = 0; omitted < 3; omitted++) {
            try (var c = context(POSTGRES)) {
                var p = prepare(c, 1);
                var assets = stage(c, p.owner(), metadata(12), true);
                int missing = omitted;
                String expected = missing == 0 ? "revision_schema_asset_descriptor_ref"
                        : missing == 1 ? "revision_schema_asset_metadata_ref" : "revision_schema_asset_source_ref";
                assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                    for (int role = 0; role < 3; role++) if (role != missing)
                        reference(em, revision, p.owner(), assets.hash(role));
                    bind(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor);
                }, em -> {})).hasStackTraceContaining(expected);
                assertThat(count(c, "document_revision_schema_assets")).isZero();
            }
        }
    }

    @Test void metadataMustExistAndFitThe512KibBound() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var assets = stageWithoutMetadata(c, p.owner(), metadata(16));
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                reference(em, revision, p.owner(), assets.descriptorHash);
                reference(em, revision, p.owner(), assets.sourceHash);
                bind(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor);
            }, em -> {})).hasStackTraceContaining("Revision schema metadata is missing or exceeds byte bound");
        }
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var assets = stage(c, p.owner(), metadata(METADATA_LIMIT + 1), true);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                reference(em, revision, p.owner(), assets.descriptorHash);
                reference(em, revision, p.owner(), assets.sourceHash);
                bindRow(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor,
                        assets.metadataHash, null);
            }, em -> {})).hasStackTraceContaining("metadata is missing or exceeds byte bound");
            assertThat(count(c, "document_revision_schema_assets")).isZero();
        }
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var assets = stage(c, p.owner(), metadata(METADATA_LIMIT), false);
            publish(c, p, Fault.NONE, false, (em, revision) -> {
                reference(em, revision, p.owner(), assets.descriptorHash);
                reference(em, revision, p.owner(), assets.metadataHash);
                bindRow(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor, assets.metadataHash, null);
            }, em -> {});
            assertThat(count(c, "document_revision_schema_assets")).isEqualTo(1);
        }
    }

    @Test void optionalSourceMayBeAbsent() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var assets = stage(c, p.owner(), metadata(16), false);
            publish(c, p, Fault.NONE, false, (em, revision) -> {
                reference(em, revision, p.owner(), assets.descriptorHash);
                reference(em, revision, p.owner(), assets.metadataHash);
                bindRow(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor, assets.metadataHash, null);
            }, em -> {});
            Object source = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT source_sha256 FROM document_revision_schema_assets").getSingleResult());
            assertThat(source).isNull();
        }
    }

    @Test void wrongScopeAndGenerationCannotUseCurrentOwnerFence() {
        for (int variant = 0; variant < 3; variant++) {
            try (var c = context(POSTGRES)) {
                var p = prepare(c, 1);
                var assets = stage(c, p.owner(), metadata(16), false);
                int fault = variant;
                assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                    reference(em, revision, p.owner(), assets.descriptorHash);
                    reference(em, revision, p.owner(), assets.metadataHash);
                    bindRow(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor,
                            assets.metadataHash, null,
                            fault == 0 ? "foreign" : p.owner().key().account(),
                            fault == 1 ? "foreign" : p.owner().key().principal(),
                            fault == 2 ? p.owner().generation() + 1 : p.owner().generation());
                }, em -> {})).hasStackTraceContaining("live owner write fence");
            }
        }
    }

    @Test void sealedAndTerminalBindingsAreRefusedAndFailedTerminalCanRetry() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var assets = stage(c, p.owner(), metadata(16), false);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                referenceAssets(em, revision, p.owner(), assets);
                em.createNativeQuery("UPDATE document_revision_publications SET projection_sealed=true WHERE revision_id=:r")
                        .setParameter("r", revision).executeUpdate();
                bind(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor);
            }, em -> {})).hasStackTraceContaining("current unsealed native projection");
            assertThat(count(c, "document_revision_schema_assets")).isZero();

            assertThatThrownBy(() -> publish(c, p, Fault.OMIT_OUTCOME, false, (em, revision) -> {
                referenceAssets(em, revision, p.owner(), assets);
                bind(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor);
            }, em -> {}))
                    .isInstanceOf(RuntimeException.class);
            assertThat(count(c, "document_revision_schema_assets")).isZero();
            assertThat(count(c, "document_revision_commits")).isZero();
            publish(c, p, Fault.NONE, false, (em, revision) -> {
                referenceAssets(em, revision, p.owner(), assets);
                bind(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor);
            }, em -> {});
            UUID revision = UUID.fromString((String)c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT revision_id::text FROM document_revision_commits LIMIT 1").getSingleResult()));
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { bind(em, revision, p.owner(), assets,
                    "type.test/fixture.Record", assets.descriptor); }))
                    .hasStackTraceContaining("terminal");
        }
    }

    @Test void migrationFromPopulatedV56DoesNotInventAssetBindings() {
        try (var c = context(POSTGRES, "56")) {
            var p = prepare(c, 1);
            var evidence = stage(c, p.owner(), metadata(16), false);
            publish(c, p, Fault.NONE, false, (em, revision) -> {
                reference(em, revision, p.owner(), evidence.descriptorHash);
                reference(em, revision, p.owner(), evidence.metadataHash);
                var part = rawFragment(em, revision);
                em.createNativeQuery("""
                        INSERT INTO document_revision_schema_evidence(revision_id,revision_ordinal,account_id,principal,
                         operation_id,owner_generation,root_locator_sha256,fragment_sha256,fragment_size,evidence_codec,
                         evidence_version,evidence_bytes,evidence_sha256)
                        VALUES(:r,0,:a,:p,:op,:g,:loc,:fh,:fs,'document-root-schema-evidence',1,:b,:eh)
                        """).setParameter("r", revision).setParameter("a", p.owner().key().account())
                        .setParameter("p", p.owner().key().principal()).setParameter("op", p.owner().key().operationId())
                        .setParameter("g", p.owner().generation()).setParameter("loc", sha("root locator".getBytes(StandardCharsets.UTF_8)))
                        .setParameter("fh", part.hash).setParameter("fs", part.size).setParameter("b", new byte[]{1, 2})
                        .setParameter("eh", sha(new byte[]{1, 2})).executeUpdate();
            }, em -> {});
            assertThat(count(c, "document_revision_schema_evidence")).isEqualTo(1);
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            assertThat(count(c, "document_revision_schema_evidence")).isEqualTo(1);
            assertThat(count(c, "document_revision_schema_assets")).isZero();
        }
    }

    @Test void longUnicodeUrlUsesDigestIdentityAndAllowsDescriptorVersions() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var first = stage(c, p.owner(), metadata(16), false);
            var secondDescriptor = ByteString.copyFromUtf8("synthetic descriptor version 2");
            String secondHash = stageBytes(c, p, secondDescriptor);
            String url = "漢".repeat(4096);
            assertThat(url.length()).isEqualTo(4096);
            assertThat(url.getBytes(StandardCharsets.UTF_8).length).isGreaterThan(2704);
            publish(c, p, Fault.NONE, false, (em, revision) -> {
                reference(em, revision, p.owner(), first.descriptorHash);
                reference(em, revision, p.owner(), first.metadataHash);
                reference(em, revision, p.owner(), secondHash);
                String suffix = "/fixture.Record";
                String longUrl = "漢".repeat(4096 - suffix.length()) + suffix;
                bindRow(em, revision, p.owner(), first, longUrl, first.descriptor, first.metadataHash, null);
                bindRow(em, revision, p.owner(), first, longUrl, secondDescriptor, first.metadataHash, null,
                        p.owner().key().account(), p.owner().key().principal(), p.owner().generation(), secondHash);
            }, em -> {});
            assertThat(count(c, "document_revision_schema_assets")).isEqualTo(2);
            Object[] stored = (Object[])c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT char_length(type_url),type_url_sha256=sha256(convert_to(type_url,'UTF8')) FROM document_revision_schema_assets LIMIT 1")
                    .getSingleResult());
            assertThat(((Number)stored[0]).intValue()).isEqualTo(4096);
            assertThat(stored[1]).isEqualTo(true);
        }
    }

    @Test void duplicateTypeAndDescriptorBindingIsRejected() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var assets = stage(c, p.owner(), metadata(16), false);
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                reference(em, revision, p.owner(), assets.descriptorHash);
                reference(em, revision, p.owner(), assets.metadataHash);
                bind(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor);
                bind(em, revision, p.owner(), assets, "type.test/fixture.Record", assets.descriptor);
            }, em -> {})).hasStackTraceContaining("document_revision_schema_assets_pkey");
            assertThat(count(c, "document_revision_schema_assets")).isZero();
        }
    }

    @Test void bindingCountAllowsSixtyFourAndRefusesTheSixtyFifth() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var assets = stage(c, p.owner(), metadata(16), false);
            var reachedLimit = new AtomicBoolean();
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, false, (em, revision) -> {
                reference(em, revision, p.owner(), assets.descriptorHash);
                reference(em, revision, p.owner(), assets.metadataHash);
                for (int i = 0; i < 64; i++) bind(em, revision, p.owner(), assets, "type.test/record-" + i, assets.descriptor);
                long count = ((Number)em.createNativeQuery("SELECT count(*) FROM document_revision_schema_assets WHERE revision_id=:r")
                        .setParameter("r", revision).getSingleResult()).longValue();
                reachedLimit.set(count == 64);
                bind(em, revision, p.owner(), assets, "type.test/overflow", assets.descriptor);
            }, em -> {})).hasStackTraceContaining("Revision schema asset binding count exceeds bound");
            assertThat(reachedLimit).isTrue();
            assertThat(count(c, "document_revision_schema_assets")).isZero();
        }
    }

    private static Assets stage(Context c, RepositoryOperationLedger.Owner owner, ByteString metadata, boolean withSource) {
        var descriptor = ByteString.copyFromUtf8("synthetic descriptor bytes; SQL storage test only");
        var source = withSource ? ByteString.copyFromUtf8("synthetic source archive bytes") : null;
        var bytes = new java.util.ArrayList<ByteString>();
        bytes.add(descriptor); bytes.add(metadata); if (source != null) bytes.add(source);
        new RepositorySchemaArtifacts(c.tx()).stage(owner, bytes, () -> {});
        String descriptorHash = shaHex(descriptor.toByteArray()), metadataHash = shaHex(metadata.toByteArray());
        String sourceHash = source == null ? null : shaHex(source.toByteArray());
        return new Assets(descriptor, metadata, source, descriptorHash, metadataHash, sourceHash);
    }

    private static Assets stageWithoutMetadata(Context c, RepositoryOperationLedger.Owner owner, ByteString metadata) {
        var descriptor = ByteString.copyFromUtf8("synthetic descriptor bytes; SQL storage test only");
        var source = ByteString.copyFromUtf8("synthetic source archive bytes");
        new RepositorySchemaArtifacts(c.tx()).stage(owner, List.of(descriptor, source), () -> {});
        return new Assets(descriptor, metadata, source, shaHex(descriptor.toByteArray()), shaHex(metadata.toByteArray()), shaHex(source.toByteArray()));
    }

    private static String stageBytes(Context c, Prepared p, ByteString bytes) {
        return new RepositorySchemaArtifacts(c.tx()).stage(p.owner(), List.of(bytes), () -> {}).getFirst();
    }
    private static void bind(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, Assets assets,
            String url, ByteString descriptor) {
        bindRow(em, revision, owner, assets, url, descriptor, assets.metadataHash, assets.sourceHash);
    }
    private static void referenceAssets(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, Assets assets) {
        reference(em, revision, owner, assets.descriptorHash);
        reference(em, revision, owner, assets.metadataHash);
        if (assets.sourceHash != null) reference(em, revision, owner, assets.sourceHash);
    }
    private static void bindRow(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, Assets assets,
            String url, ByteString descriptor, String metadataHash, String sourceHash) {
        bindRow(em, revision, owner, assets, url, descriptor, metadataHash, sourceHash,
                owner.key().account(), owner.key().principal(), owner.generation(), shaHex(descriptor.toByteArray()));
    }
    private static void bindRow(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, Assets assets,
            String url, ByteString descriptor, String metadataHash, String sourceHash,
            String account, String principal, long generation) {
        bindRow(em, revision, owner, assets, url, descriptor, metadataHash, sourceHash,
                account, principal, generation, shaHex(descriptor.toByteArray()));
    }
    private static void bindRow(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, Assets assets,
            String url, ByteString descriptor, String metadataHash, String sourceHash,
            String account, String principal, long generation, String descriptorHash) {
        em.createNativeQuery("""
                INSERT INTO document_revision_schema_assets(revision_id,account_id,principal,operation_id,owner_generation,
                 type_url,type_url_sha256,descriptor_sha256,metadata_sha256,metadata_codec,metadata_version,source_sha256)
                VALUES(:r,:a,:p,:op,:g,:url,:uh,:dh,:mh,'repository-schema-asset',1,:sh)
                """).setParameter("r", revision).setParameter("a", account).setParameter("p", principal)
                .setParameter("op", owner.key().operationId()).setParameter("g", generation).setParameter("url", url)
                .setParameter("uh", sha(url.getBytes(StandardCharsets.UTF_8))).setParameter("dh", HexFormat.of().parseHex(descriptorHash))
                .setParameter("mh", HexFormat.of().parseHex(metadataHash)).setParameter("sh", sourceHash == null ? null : HexFormat.of().parseHex(sourceHash))
                .executeUpdate();
    }

    private static void reference(EntityManager em, UUID revision, RepositoryOperationLedger.Owner owner, String hash) {
        em.createNativeQuery("""
                INSERT INTO document_revision_schema_artifacts(revision_id,account_id,artifact_sha256,principal,operation_id,owner_generation)
                VALUES(:r,:a,:h,:p,:op,:g)
                """).setParameter("r", revision).setParameter("a", owner.key().account())
                .setParameter("h", HexFormat.of().parseHex(hash)).setParameter("p", owner.key().principal())
                .setParameter("op", owner.key().operationId()).setParameter("g", owner.generation()).executeUpdate();
    }

    private static Raw rawFragment(EntityManager em, UUID revision) {
        Object[] row = (Object[])em.createNativeQuery("""
                SELECT decode(o.expected_sha256,'hex'),o.expected_size FROM document_revision_parts p
                JOIN repository_physical_locations l ON l.object_id=p.object_id AND l.source_kind='DOCUMENT_PART'
                JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                WHERE p.revision_id=:r AND p.revision_ordinal=0 ORDER BY p.part LIMIT 1
                """).setParameter("r", revision).getSingleResult();
        return new Raw((byte[])row[0], ((Number)row[1]).longValue());
    }

    private static byte[] sha(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static String shaHex(byte[] bytes) { return HexFormat.of().formatHex(sha(bytes)); }
    private static ByteString metadata(int size) {
        byte[] bytes = new byte[size];
        byte[] marker = "synthetic metadata; SQL binding only".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(marker, 0, bytes, 0, Math.min(marker.length, bytes.length));
        return ByteString.copyFrom(bytes);
    }
    private record Assets(ByteString descriptor, ByteString metadata, ByteString source,
            String descriptorHash, String metadataHash, String sourceHash) {
        String hash(int role) { return role == 0 ? descriptorHash : role == 1 ? metadataHash : sourceHash; }
    }
    private record Raw(byte[] hash, long size) {}
}
