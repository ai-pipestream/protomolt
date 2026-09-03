package ai.protomolt.proto.search.index.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
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
 * {@link LuceneIndexWriter} basics: adds and term deletions are buffered and
 * become visible to a reopened reader on {@code commit()}.
 */
class LuceneIndexWriterTest {

    @TempDir
    Path indexDir;

    private static Document doc(String id) {
        Document document = new Document();
        document.add(new StringField("id", id, Field.Store.NO));
        return document;
    }

    @Test
    void deleteByTermRemovesMatchingDocumentsOnCommit() throws Exception {
        try (LuceneIndexWriter writer = new LuceneIndexWriter(indexDir)) {
            writer.add(doc("a"));
            writer.add(doc("b"));
            writer.delete(new Term("id", "a"));
            writer.commit();
        }

        try (Directory directory = FSDirectory.open(indexDir);
                DirectoryReader reader = DirectoryReader.open(directory)) {
            assertThat(reader.numDocs()).isEqualTo(1);
            IndexSearcher searcher = new IndexSearcher(reader);
            assertThat(searcher.search(new TermQuery(new Term("id", "a")), 10)
                    .totalHits.value()).isZero();
            assertThat(searcher.search(new TermQuery(new Term("id", "b")), 10)
                    .totalHits.value()).isEqualTo(1);
        }
    }

    @Test
    void deleteOfAbsentTermIsANoOp() throws Exception {
        try (LuceneIndexWriter writer = new LuceneIndexWriter(indexDir)) {
            writer.add(doc("a"));
            writer.delete(new Term("id", "missing"));
            writer.commit();
        }

        try (Directory directory = FSDirectory.open(indexDir);
                DirectoryReader reader = DirectoryReader.open(directory)) {
            assertThat(reader.numDocs()).isEqualTo(1);
        }
    }
}
