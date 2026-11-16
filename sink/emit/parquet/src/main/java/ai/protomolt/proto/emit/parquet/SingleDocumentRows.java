package ai.protomolt.proto.emit.parquet;

import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.requireRecognizedEnumsInSelectedColumns;

import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider.Cancellation;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Set;

/** Writes the input message itself as the single Parquet row. Any message type is accepted. */
final class SingleDocumentRows implements ParquetRowMode {

    static final SingleDocumentRows INSTANCE = new SingleDocumentRows();

    private SingleDocumentRows() {}

    @Override
    public void verifySource(String providerId, Descriptor inputDescriptor) {
        // No declared source: the row schema is the input's own descriptor.
    }

    @Override
    public Descriptor rowDescriptor(Descriptor inputDescriptor) {
        return inputDescriptor;
    }

    @Override
    public List<Message> rows(
            Message input, Set<String> selectedColumns, Cancellation cancellation)
            throws RenderFailure {
        requireRecognizedEnumsInSelectedColumns(input, selectedColumns, cancellation);
        return List.of(input);
    }
}
