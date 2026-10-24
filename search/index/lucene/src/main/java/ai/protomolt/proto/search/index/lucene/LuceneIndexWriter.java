package ai.protomolt.proto.search.index.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/** Thin Lucene {@link IndexWriter} wrapper for protobuf → document indexing demos. */
public final class LuceneIndexWriter implements Closeable {

    private final Directory directory;
    private final IndexWriter writer;

    /**
     * Opens an {@link IndexWriter} over {@code indexPath} using {@link StandardAnalyzer}.
     *
     * @param indexPath filesystem path of the Lucene index directory
     * @throws IOException if the directory or index writer cannot be opened
     */
    public LuceneIndexWriter(Path indexPath) throws IOException {
        Objects.requireNonNull(indexPath, "indexPath");
        Directory opened = FSDirectory.open(indexPath);
        try {
            this.writer = new IndexWriter(opened, new IndexWriterConfig(new StandardAnalyzer()));
            this.directory = opened;
        } catch (IOException | RuntimeException e) {
            try {
                opened.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /**
     * Buffers {@code document} for addition on the next {@link #commit()}.
     *
     * @param document Lucene document to add
     * @throws IOException if the underlying writer fails
     */
    public void add(Document document) throws IOException {
        writer.addDocument(Objects.requireNonNull(document, "document"));
    }

    /**
     * Deletes every document matching {@code term}. Like adds, deletions are
     * buffered and become visible to reopened readers on {@link #commit()}.
     *
     * @param term identity term to delete
     * @throws IOException if the underlying writer fails
     */
    public void delete(Term term) throws IOException {
        writer.deleteDocuments(Objects.requireNonNull(term, "term"));
    }

    /**
     * Flushes and commits buffered document additions and deletions.
     *
     * @throws IOException if the commit fails
     */
    public void commit() throws IOException {
        writer.commit();
    }

    /**
     * Returns the current number of live + buffered documents known to the writer.
     *
     * @return document count
     */
    public int numDocs() {
        return writer.getDocStats().numDocs;
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } finally {
            directory.close();
        }
    }
}
