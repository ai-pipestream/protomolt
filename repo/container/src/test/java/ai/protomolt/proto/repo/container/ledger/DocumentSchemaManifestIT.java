package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Compares real checked proofs with PostgreSQL retention; provider observations are synthetic. */
@Testcontainers
class DocumentSchemaManifestIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void matchesRetainedRowsBeforeSealAndAfterCommit() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var expected = new AtomicReference<String>();
            UUID revision = DocumentSchemaRetentionFixture.publishWithManifest(c, f, (em, id, manifest) -> {
                assertThat(manifest.decision()).isEqualTo("TYPED");
                expected.set(manifest.json());
                assertThat(matches(em, id, manifest.json())).as("missing retention cannot satisfy proof").isFalse();
                f.retention().write(em, f.owner(), id, () -> {});
                assertThat(matches(em, id, manifest.json())).isTrue();
            });
            boolean matchesAfterCommit = c.tx().readOnly(em -> matches(em, revision, expected.get()));
            assertThat(matchesAfterCommit).isTrue();
        }
    }

    @Test void rejectsSameCountSubstitutionsInEveryRetentionSet() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var expected = new AtomicReference<String>();
            UUID revision = DocumentSchemaRetentionFixture.publishWithManifest(c, f, (em, id, manifest) -> {
                expected.set(manifest.json());
                f.retention().write(em, f.owner(), id, () -> {});
            });
            // Alter the expected set, leaving the immutable real SQL rows intact.
            // Counts remain equal in every case; identity equality must still fail.
            for (String field : new String[]{"ordinal", "part", "object_id", "size", "sha256", "sub_key"}) {
                assertMismatch(c, revision, expected.get(), "{parts,0," + field + "}",
                        field.equals("object_id") ? UUID.randomUUID().toString() : "different");
            }
            assertMismatch(c, revision, expected.get(), "{artifacts,0}", "0".repeat(64));
            for (String field : new String[]{"descriptor_sha256", "metadata_sha256", "source_sha256", "type_url", "type_url_sha256", "metadata_codec", "metadata_version"}) {
                assertMismatch(c, revision, expected.get(), "{assets,0," + field + "}", "different");
            }
            for (String field : new String[]{"ordinal", "locator_sha256", "evidence_sha256", "fragment_sha256", "fragment_size", "evidence_codec", "evidence_version"}) {
                assertMismatch(c, revision, expected.get(), "{roots,0," + field + "}", "different");
            }

        }
    }

    @Test void unknownRevisionDoesNotBecomeAnEmptyManifest() throws Exception {
        try (var c = context(POSTGRES)) {
            assertThatThrownBy(() -> c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT CAST(document_schema_retention_manifest_v1(:revision) AS text)")
                    .setParameter("revision", UUID.randomUUID()).getSingleResult()))
                    .hasStackTraceContaining("Schema manifest requires a native revision");
        }
    }

    private static void assertMismatch(Context c, UUID revision, String expected, String path, String replacement) {
        boolean matches = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                SELECT jsonb_set(CAST(:expected AS jsonb),CAST(:path AS text[]),to_jsonb(CAST(:replacement AS text)),false)
                       =document_schema_retention_manifest_v1(:revision)
                """).setParameter("expected", expected).setParameter("path", path)
                .setParameter("replacement", replacement).setParameter("revision", revision).getSingleResult());
        assertThat(matches).as(path).isFalse();
    }

    private static boolean matches(EntityManager em, UUID revision, String expected) {
        return (Boolean) em.createNativeQuery("""
                SELECT CAST(:expected AS jsonb)=document_schema_retention_manifest_v1(:revision)
                """).setParameter("expected", expected).setParameter("revision", revision).getSingleResult();
    }
}
