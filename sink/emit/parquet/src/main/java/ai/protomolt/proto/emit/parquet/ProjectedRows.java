package ai.protomolt.proto.emit.parquet;

import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.requireNoUnknownWireFields;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.requireRecognizedEnumsInSelectedColumns;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.validateMessageRules;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.verifyFieldsCompatible;

import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider.Cancellation;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.ProjectionException;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Projects the input, which must be the declared source type, through a compiled {@link
 * MessageProjection} into one Parquet row.
 */
final class ProjectedRows implements ParquetRowMode {

    private final Descriptor declaredSource;
    private final MessageProjection projection;

    ProjectedRows(Descriptor declaredSource, MessageProjection projection) {
        this.declaredSource = Objects.requireNonNull(declaredSource, "declaredSource");
        this.projection = Objects.requireNonNull(projection, "projection");
    }

    @Override
    public void verifySource(String providerId, Descriptor inputDescriptor) throws RenderFailure {
        requireDeclaredSourceType(providerId, declaredSource, inputDescriptor);
        verifyFieldsCompatible(declaredSource, inputDescriptor);
    }

    @Override
    public Descriptor rowDescriptor(Descriptor inputDescriptor) {
        return projection.targetType();
    }

    @Override
    public List<Message> rows(
            Message input, Set<String> selectedColumns, Cancellation cancellation)
            throws RenderFailure {
        requireRecognizedEnumsInSelectedColumns(input, Set.of(), cancellation);
        Message projectedRow = project(projection, input, -1);
        requireNoUnknownWireFields(projectedRow, cancellation);
        requireRecognizedEnumsInSelectedColumns(projectedRow, selectedColumns, cancellation);
        validateMessageRules(projection.targetType(), projectedRow, 0);
        return List.of(projectedRow);
    }

    static void requireDeclaredSourceType(
            String providerId, Descriptor declaredSource, Descriptor actualSource)
            throws RenderFailure {
        if (!actualSource.getFullName().equals(declaredSource.getFullName())) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Provider '" + providerId + "' expects source type '"
                            + declaredSource.getFullName() + "', got '"
                            + actualSource.getFullName() + "'");
        }
    }

    /**
     * Projects one source message, naming the row in the refusal when {@code rowIndex >= 0}.
     */
    static Message project(MessageProjection projection, Message sourceElement, int rowIndex)
            throws RenderFailure {
        try {
            return projection.project(sourceElement);
        } catch (ProjectionException failure) {
            String prefix = rowIndex >= 0
                    ? "Row " + rowIndex + " failed declared projection to "
                            + projection.targetType().getFullName() + ": "
                    : "Source failed declared projection to "
                            + projection.targetType().getFullName() + ": ";
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED, prefix + failure.getMessage(), failure);
        }
    }
}
