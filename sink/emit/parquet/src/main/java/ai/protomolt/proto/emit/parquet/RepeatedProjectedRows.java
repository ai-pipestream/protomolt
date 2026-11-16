package ai.protomolt.proto.emit.parquet;

import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.evaluateRules;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.formatValidationFailure;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.requireNoUnknownWireFields;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.requireRecognizedEnumsInSelectedColumns;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.verifyFieldsCompatible;

import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider.Cancellation;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Projects every element of a declared repeated message field on the source into one Parquet row
 * each. Any element that fails projection, enum or rule checks refuses the whole render.
 */
final class RepeatedProjectedRows implements ParquetRowMode {

    private final Descriptor declaredSource;
    private final FieldDescriptor repeatedRowsField;
    private final MessageProjection projection;

    RepeatedProjectedRows(
            Descriptor declaredSource,
            FieldDescriptor repeatedRowsField,
            MessageProjection projection) {
        this.declaredSource = Objects.requireNonNull(declaredSource, "declaredSource");
        this.repeatedRowsField = Objects.requireNonNull(repeatedRowsField, "repeatedRowsField");
        this.projection = Objects.requireNonNull(projection, "projection");
    }

    @Override
    public void verifySource(String providerId, Descriptor inputDescriptor) throws RenderFailure {
        ProjectedRows.requireDeclaredSourceType(providerId, declaredSource, inputDescriptor);
        FieldDescriptor actualField =
                inputDescriptor.findFieldByNumber(repeatedRowsField.getNumber());
        if (actualField == null
                || !actualField.getName().equals(repeatedRowsField.getName())
                || !actualField.isRepeated()
                || actualField.isMapField()
                || actualField.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                || !actualField.getMessageType().getFullName()
                        .equals(repeatedRowsField.getMessageType().getFullName())) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Source descriptor '" + inputDescriptor.getFullName()
                            + "' is incompatible with declared repeated row field '"
                            + repeatedRowsField.getName() + "'");
        }
        verifyFieldsCompatible(repeatedRowsField.getMessageType(), actualField.getMessageType());
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
        Descriptor rowDescriptor = projection.targetType();
        FieldDescriptor inputRepeatedField =
                input.getDescriptorForType().findFieldByNumber(repeatedRowsField.getNumber());
        int count = input.getRepeatedFieldCount(inputRepeatedField);
        List<Message> projectedRows = new ArrayList<>(count);
        ProtoValidator rowValidator = ProtoValidator.forMessageType(rowDescriptor);
        for (int i = 0; i < count; i++) {
            cancellation.check();
            Message element = (Message) input.getRepeatedField(inputRepeatedField, i);
            Message projectedRow = ProjectedRows.project(projection, element, i);
            requireNoUnknownWireFields(projectedRow, cancellation);
            requireRecognizedEnumsInSelectedColumns(projectedRow, selectedColumns, cancellation);
            ValidationResult rowValidation = evaluateRules(rowValidator, projectedRow);
            if (!rowValidation.valid()) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        formatValidationFailure(rowDescriptor, i, rowValidation));
            }
            projectedRows.add(projectedRow);
        }
        return List.copyOf(projectedRows);
    }
}
