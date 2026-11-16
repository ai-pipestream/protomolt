package ai.protomolt.proto.emit.parquet;

import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.formatValidationFailure;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.parquet.v1.ParquetMaskStrategy;
import ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Parses and validates {@link ParquetRendererOptions} supplied to a {@link ParquetRendererProvider}
 * and maps them to {@link ParquetExportOptions}.
 */
final class ParquetOptionChecks {
    static final String OPTIONS_FINGERPRINT =
            DescriptorFingerprints.fingerprintOf(ParquetRendererOptions.getDescriptor());

    private ParquetOptionChecks() {}

    static ParquetRendererOptions parseAndValidateOptions(Message options) throws RenderFailure {
        Descriptor suppliedDesc = options.getDescriptorForType();
        if (!suppliedDesc.getFullName().equals(ParquetRendererOptions.getDescriptor().getFullName())
                || !DescriptorFingerprints.fingerprintOf(suppliedDesc).equals(OPTIONS_FINGERPRINT)) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    "Options descriptor '" + suppliedDesc.getFullName()
                            + "' does not match capability options '"
                            + ParquetRendererOptions.getDescriptor().getFullName() + "'");
        }
        requireNoUnknownOptionFields(options);
        ParquetRendererOptions typed;
        if (options instanceof ParquetRendererOptions direct) {
            typed = direct;
        } else {
            try {
                typed = ParquetRendererOptions.parseFrom(options.toByteString());
            } catch (InvalidProtocolBufferException failure) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        "Malformed binary ParquetRendererOptions: " + failure.getMessage(),
                        failure);
            }
        }
        if (!typed.getUnknownFields().asMap().isEmpty()) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    "Unknown fields are not allowed on ParquetRendererOptions");
        }
        if (typed.getMaskStrategy() == ParquetMaskStrategy.UNRECOGNIZED) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    "Unrecognized ParquetMaskStrategy value: " + typed.getMaskStrategyValue());
        }
        ValidationResult validation =
                ProtoValidator.forMessageType(ParquetRendererOptions.getDescriptor())
                        .validate(typed);
        if (!validation.valid()) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    formatValidationFailure(
                            ParquetRendererOptions.getDescriptor(), -1, validation));
        }
        return typed;
    }

    static Set<String> extractSelectedColumns(ParquetRendererOptions options)
            throws RenderFailure {
        if (options.getColumnsCount() == 0) {
            return Set.of();
        }
        Set<String> columns = new LinkedHashSet<>(options.getColumnsCount());
        for (String col : options.getColumnsList()) {
            if (col == null || col.isBlank()) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        "ParquetRendererOptions.columns must not contain blank column names");
            }
            if (!columns.add(col)) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        "ParquetRendererOptions.columns contains duplicate column '" + col + "'");
            }
        }
        return Set.copyOf(columns);
    }

    static ParquetExportOptions toExportOptions(
            ParquetRendererOptions options, Set<String> selectedColumns) throws RenderFailure {
        if (options.getMaskedSensitivityClassesCount() == 0) {
            return new ParquetExportOptions(selectedColumns, Set.of(), null, null, false);
        }
        Set<String> classes = new LinkedHashSet<>(options.getMaskedSensitivityClassesCount());
        for (String sensitivityClass : options.getMaskedSensitivityClassesList()) {
            if (sensitivityClass == null || sensitivityClass.isBlank()) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        "masked_sensitivity_classes must not contain blank entries");
            }
            if (!classes.add(sensitivityClass)) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        "Duplicate sensitivity class '" + sensitivityClass + "'");
            }
        }
        SensitivityMasker.Strategy strategy = switch (options.getMaskStrategy()) {
            case PARQUET_MASK_STRATEGY_UNSPECIFIED, PARQUET_MASK_STRATEGY_REDACT ->
                    SensitivityMasker.Strategy.REDACT;
            case PARQUET_MASK_STRATEGY_REMOVE -> SensitivityMasker.Strategy.REMOVE;
            case UNRECOGNIZED -> throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    "Unrecognized ParquetMaskStrategy: " + options.getMaskStrategyValue());
        };
        return new ParquetExportOptions(
                selectedColumns, Set.copyOf(classes), strategy, null, false);
    }

    static void requireNoUnknownOptionFields(Message root) throws RenderFailure {
        Deque<Message> stack = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            Message current = stack.pop();
            if (!current.getUnknownFields().asMap().isEmpty()) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        "Unknown fields are not permitted on "
                                + current.getDescriptorForType().getFullName());
            }
            for (var entry : current.getAllFields().entrySet()) {
                FieldDescriptor field = entry.getKey();
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    continue;
                }
                if (field.isRepeated()) {
                    for (Object item : (List<?>) entry.getValue()) {
                        stack.push((Message) item);
                    }
                } else {
                    stack.push((Message) entry.getValue());
                }
            }
        }
    }
}
