package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ChangeSource;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.pipestream.proto.acquire.confluence.v1.Page;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.index.lucene.LuceneIndexWriter;
import ai.pipestream.proto.index.lucene.ProtoLuceneMapper;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import com.google.protobuf.Timestamp;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Lucene projection end to end minus Kafka: changes applied through
 * {@link ConfluenceLuceneProjector#project} land in a real on-disk index and
 * are searchable (exact term on the change id, full text over the entity
 * payload), and a DELETE change tombstones the previously upserted documents
 * of its entity. Follows the {@code LuceneEngineEndToEndTest} idioms.
 */
class ConfluenceLuceneProjectorTest {

    @TempDir
    Path indexDir;

    private static ConfluenceChange pageChange(String changeId, String pageId, String title) {
        return ConfluenceChange.newBuilder()
                .setChangeId(changeId)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId(pageId)
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1_753_000_000))
                        .setPage(Page.newBuilder()
                                .setId(pageId)
                                .setSpaceId("456")
                                .setTitle(title)))
                .setSource(ChangeSource.CHANGE_SOURCE_CRAWL)
                .setCursor("run-1")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1_753_000_001))
                .build();
    }

    @Test
    void upsertsBecomeSearchableDocumentsAndDeletesTombstoneThem() throws Exception {
        ProtoLuceneMapper mapper = new ProtoLuceneMapper(
                new ProtoFieldMapperImpl(new DescriptorRegistry()));
        IndexMapping mapping = ConfluenceLuceneProjector.indexMapping();

        try (LuceneIndexWriter writer = new LuceneIndexWriter(indexDir)) {
            ConfluenceLuceneProjector.project(
                    pageChange("c1", "111", "Hello Lucene World"), mapper, mapping, writer);
            ConfluenceLuceneProjector.project(
                    pageChange("c2", "222", "Another Page"), mapper, mapping, writer);
            ConfluenceLuceneProjector.project(
                    pageChange("c3", "111", "ignored").toBuilder()
                            .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                            .build(),
                    mapper, mapping, writer);
            // buffered deletes are not reflected in numDocs(); they apply on commit
            writer.commit();
        }

        try (Directory directory = FSDirectory.open(indexDir);
                DirectoryReader reader = DirectoryReader.open(directory)) {
            assertThat(reader.numDocs()).as("the delete tombstones page 111").isEqualTo(1);
            IndexSearcher searcher = new IndexSearcher(reader);
            // change_id infers KEYWORD (name ends in _id): exact term match.
            assertThat(searcher.search(new TermQuery(new Term("change_id", "c1")), 10)
                    .totalHits.value()).as("page 111 is gone").isZero();
            assertThat(searcher.search(new TermQuery(new Term("change_id", "c2")), 10)
                    .totalHits.value()).isEqualTo(1);
            // The delete matched on the entity_id tag, not on change_id c3.
            assertThat(searcher.search(new TermQuery(new Term("change_id", "c3")), 10)
                    .totalHits.value()).isZero();
            // The entity is one OBJECT field of compact JSON: full text reaches the title.
            assertThat(searcher.search(new TermQuery(new Term("entity", "lucene")), 10)
                    .totalHits.value()).isZero();
            assertThat(searcher.search(new TermQuery(new Term("entity", "page")), 10)
                    .totalHits.value()).isEqualTo(1);
        }
    }
}
