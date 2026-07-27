package ai.pipestream.proto.index.lucene;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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

    public LuceneIndexWriter(Path indexPath) throws IOException {
        Objects.requireNonNull(indexPath, "indexPath");
        this.directory = FSDirectory.open(indexPath);
        this.writer = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()));
    }

    public void add(Document document) throws IOException {
        writer.addDocument(Objects.requireNonNull(document, "document"));
    }

    public void commit() throws IOException {
        writer.commit();
    }

    public int numDocs() {
        return writer.getDocStats().numDocs;
    }

    @Override
    public void close() throws IOException {
        writer.close();
        directory.close();
    }
}
