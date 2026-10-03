package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentSaveCandidateTest {
    @Test void scopedRewritePreservesDestinationPolicyAndBookkeepingWithoutMutatingSnapshot() {
        var request = SaveDocumentRequest.newBuilder().setDrive("primary").setGraphId("graph")
                .setGraphLocationId("destination").setConnectorId("connector").setCrawlId("crawl")
                .setDocument(Document.newBuilder().setDocId("doc").setOwnership(OwnershipContext.newBuilder()
                        .setAccountId("account").setDatasourceId("source-provenance"))).build();
        var resolved = SaveResolution.resolve(request);
        var existing = new DocumentRecord();
        existing.datasourceId = "destination-owner";
        existing.security = "{\"inheritFromParent\":false}";
        existing.createdAt = Instant.parse("2020-01-01T00:00:00Z");
        existing.updatedAt = existing.createdAt;
        existing.reprocessCount = 7;
        existing.lastReprocessedAt = Instant.parse("2021-01-01T00:00:00Z");
        existing.status = DocumentStatus.AVAILABLE;
        existing.objectKey = "old/";
        var drive = new DriveRecord(); drive.name = "primary";
        var manifest = DocumentManifest.newBuilder().setAddress(resolved.address()).setDocVersion(2).build();
        var caller = new RepositoryCaller("user", false, Set.of("account"), Set.of());
        var candidate = DocumentSaveCandidate.build(caller, resolved, request, drive, UUID.randomUUID(),
                "new/", manifest, "checksum", 42, "etag", "version", existing);
        assertThat(candidate).isNotSameAs(existing);
        assertThat(candidate.datasourceId).isEqualTo("destination-owner");
        assertThat(candidate.security).isEqualTo(existing.security);
        assertThat(candidate.createdAt).isEqualTo(existing.createdAt);
        assertThat(candidate.reprocessCount).isEqualTo(7);
        assertThat(candidate.lastReprocessedAt).isEqualTo(existing.lastReprocessedAt);
        assertThat(candidate.updatedAt).isAfter(existing.updatedAt);
        assertThat(candidate.readManifest()).isEqualTo(manifest);
        assertThat(candidate.connectorId).isEqualTo("connector");
        assertThat(candidate.crawlId).isEqualTo("crawl");
        assertThat(candidate.versionId).isEqualTo("version");
        assertThat(existing.objectKey).isEqualTo("old/");
        assertThat(existing.updatedAt).isEqualTo(existing.createdAt);
    }
}
