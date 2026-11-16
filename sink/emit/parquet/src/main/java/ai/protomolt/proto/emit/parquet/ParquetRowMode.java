package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider.Cancellation;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Set;

/**
 * How a {@link ParquetRendererProvider} turns one input message into Parquet rows: the input
 * itself as one row, one projected row, or one projected row per element of a declared repeated
 * field. Each mode refuses its own failures; the provider runs the checks common to all modes.
 */
sealed interface ParquetRowMode permits SingleDocumentRows, ProjectedRows, RepeatedProjectedRows {

    /**
     * Refuses an input whose type does not match the source this mode declares.
     *
     * @param providerId provider ID named in the refusal
     * @param inputDescriptor descriptor of the supplied input
     * @throws RenderFailure {@code UNSUPPORTED} when the input type is not the declared source
     */
    void verifySource(String providerId, Descriptor inputDescriptor) throws RenderFailure;

    /**
     * Returns the descriptor of every emitted row.
     *
     * @param inputDescriptor descriptor of the supplied input
     * @return the row descriptor
     */
    Descriptor rowDescriptor(Descriptor inputDescriptor);

    /**
     * Builds and checks the rows. The input has already passed its unknown-field and rule checks.
     *
     * @param input the source message
     * @param selectedColumns columns selected by the options, empty for all columns
     * @param cancellation cooperative cancellation checked between rows
     * @return the rows to write, in order
     * @throws RenderFailure when a row fails projection, enum or rule checks
     */
    List<Message> rows(Message input, Set<String> selectedColumns, Cancellation cancellation)
            throws RenderFailure;
}
