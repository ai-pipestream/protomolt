package ai.protomolt.proto.emit.parquet;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.parquet.v1.ParquetMaskStrategy;
import ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.projection.ProjectionException;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.RuleCompilationException;
import ai.protomolt.proto.validate.RuleEvaluationException;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Qualified {@link RendererProvider} over {@link ParquetEmitter} for explicit repository
 * {@code Save as...} rendition publication.
 *
 * <p>Advertises explicit bounded conversion contracts without guessing or flattening arbitrary
 * nested repeated content:</p>
 * <ul>
 *   <li><b>Single-document mode</b> ({@link #ParquetRendererProvider()} and
 *       {@link #singleDocument(int, int)}, {@code provider_id = "ai.protomolt.emit.parquet.single"},
 *       {@code source_profile = "protobuf.single_message"}): renders one authorized protobuf
 *       message as a single-row Parquet artifact.</li>
 *   <li><b>Declared projection mode</b> ({@link #projected} and {@link #projectedRepeated}):
 *       renders either a single projected row or an explicitly declared repeated message field
 *       through a compiled {@link MessageProjection} into its target row schema, failing closed
 *       if any row fails projection or validation.</li>
 * </ul>
 *
 * <p>Refusal policy is strict and fail-closed: empty schemas, cyclic/recursive message graphs
 * (outside {@code google.protobuf.Struct}, {@code Value}, and {@code ListValue}), opaque
 * {@code google.protobuf.Any} columns, unknown wire fields, unrecognized enum values, invalid
 * source or projected messages, unknown column names, and input/output byte budget overflows
 * are explicitly refused. Derived Parquet renditions never replace authoritative protobuf
 * state, and validation bypass or raw cryptographic keys are never exposed through
 * {@link ParquetRendererOptions}.</p>
 */
public final class ParquetRendererProvider implements RendererProvider {

    /** Default installed provider ID discovered via {@link java.util.ServiceLoader}. */
    public static final String PROVIDER_ID = "ai.protomolt.emit.parquet.single";

    /** Provider implementation version bound into {@link RendererCapability}. */
    public static final String PROVIDER_VERSION = "1.0";

    /** Semantic source profile for single-message (1-row) Parquet rendition export. */
    public static final String SINGLE_DOCUMENT_SOURCE_PROFILE = "protobuf.single_message";

    /** IANA / Apache Parquet media type produced by this renderer. */
    public static final String PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet";

    /** Filename suffix (without leading dot) suggested for Parquet renditions. */
    public static final String PARQUET_EXTENSION = "parquet";

    /** Default maximum serialized input plus options budget (16 MiB). */
    public static final int DEFAULT_MAX_INPUT_BYTES = 16 * 1024 * 1024;

    /** Default maximum rendered Parquet artifact budget (32 MiB). */
    public static final int DEFAULT_MAX_OUTPUT_BYTES = 32 * 1024 * 1024;

    /** Hard upper ceiling permitted for {@code maxOutputBytes} (64 MiB). */
    public static final int HARD_MAX_OUTPUT_BYTES = 64 * 1024 * 1024;

    private static final String ANY_FULL_NAME = "google.protobuf.Any";
    private static final int MAX_MESSAGE_WALK_DEPTH = 128;
    private static final String OPTIONS_FINGERPRINT =
            DescriptorFingerprints.fingerprintOf(ParquetRendererOptions.getDescriptor());

    private static final String SINGLE_DOCUMENT_LOSS_DESCRIPTION =
            "Derived non-authoritative columnar Parquet artifact (1 row per selected protobuf "
                    + "message); does not replace authoritative protobuf state, omits unknown "
                    + "fields, maps google.protobuf.Struct/Value/ListValue to UTF-8 JSON strings, "
                    + "maps google.protobuf.Timestamp to microsecond UTC timestamps, widens "
                    + "unsigned 32-bit integers to INT64, and stores unsigned 64-bit integers with "
                    + "Parquet UINT_64 logical annotations over signed INT64 physical storage.";

    private final RendererCapability capability;
    private final Descriptor declaredSourceDescriptor;
    private final FieldDescriptor repeatedRowsField;
    private final MessageProjection projection;

    /**
     * Default no-argument constructor for {@link java.util.ServiceLoader} discovery: creates the
     * bounded single-document Parquet renderer ({@value #PROVIDER_ID}).
     */
    public ParquetRendererProvider() {
        this(
                buildCapability(
                        PROVIDER_ID,
                        SINGLE_DOCUMENT_SOURCE_PROFILE,
                        SINGLE_DOCUMENT_LOSS_DESCRIPTION,
                        DEFAULT_MAX_INPUT_BYTES,
                        DEFAULT_MAX_OUTPUT_BYTES),
                null,
                null,
                null);
    }

    private ParquetRendererProvider(
            RendererCapability capability,
            Descriptor declaredSourceDescriptor,
            FieldDescriptor repeatedRowsField,
            MessageProjection projection) {
        this.capability = Objects.requireNonNull(capability, "capability");
        this.declaredSourceDescriptor = declaredSourceDescriptor;
        this.repeatedRowsField = repeatedRowsField;
        this.projection = projection;
    }

    /**
     * Creates a single-document Parquet renderer ({@value #PROVIDER_ID}) with explicit bounded
     * input and output byte budgets.
     *
     * @param maxInputBytes maximum serialized input plus options bytes ({@code > 0})
     * @param maxOutputBytes maximum rendered Parquet bytes ({@code 1..HARD_MAX_OUTPUT_BYTES})
     * @return the configured single-document provider
     */
    public static ParquetRendererProvider singleDocument(int maxInputBytes, int maxOutputBytes) {
        return new ParquetRendererProvider(
                buildCapability(
                        PROVIDER_ID,
                        SINGLE_DOCUMENT_SOURCE_PROFILE,
                        SINGLE_DOCUMENT_LOSS_DESCRIPTION,
                        maxInputBytes,
                        maxOutputBytes),
                null,
                null,
                null);
    }

    /**
     * Creates a single-row projected Parquet renderer bound to an explicit {@code sourceDescriptor}
     * and compiled {@link MessageProjection}.
     *
     * @param providerId unique provider ID for this projected profile
     * @param sourceProfile semantic source profile label
     * @param sourceDescriptor expected source protobuf descriptor
     * @param projection compiled projection from {@code sourceDescriptor} to the target row type
     * @return the configured projected provider
     */
    public static ParquetRendererProvider projected(
            String providerId,
            String sourceProfile,
            Descriptor sourceDescriptor,
            MessageProjection projection) {
        return projected(
                providerId,
                sourceProfile,
                sourceDescriptor,
                projection,
                DEFAULT_MAX_INPUT_BYTES,
                DEFAULT_MAX_OUTPUT_BYTES);
    }

    /**
     * Creates a single-row projected Parquet renderer with explicit byte budgets.
     *
     * @param providerId unique provider ID for this projected profile
     * @param sourceProfile semantic source profile label
     * @param sourceDescriptor expected source protobuf descriptor
     * @param projection compiled projection from {@code sourceDescriptor} to the target row type
     * @param maxInputBytes maximum serialized input plus options bytes ({@code > 0})
     * @param maxOutputBytes maximum rendered Parquet bytes ({@code 1..HARD_MAX_OUTPUT_BYTES})
     * @return the configured projected provider
     */
    public static ParquetRendererProvider projected(
            String providerId,
            String sourceProfile,
            Descriptor sourceDescriptor,
            MessageProjection projection,
            int maxInputBytes,
            int maxOutputBytes) {
        Objects.requireNonNull(sourceDescriptor, "sourceDescriptor");
        Objects.requireNonNull(projection, "projection");
        if (!projection.supports(sourceDescriptor)) {
            throw new IllegalArgumentException(
                    "Projection target " + projection.targetType().getFullName()
                            + " does not declare source " + sourceDescriptor.getFullName());
        }
        String loss = projectedLossDescription(sourceDescriptor, null, projection.targetType());
        return new ParquetRendererProvider(
                buildCapability(providerId, sourceProfile, loss, maxInputBytes, maxOutputBytes),
                sourceDescriptor,
                null,
                projection);
    }

    /**
     * Creates a multi-row projected Parquet renderer that extracts elements strictly from an
     * explicitly declared repeated message field on {@code sourceDescriptor} and projects every
     * element through {@code projection} into its target row schema, failing closed if any row
     * fails projection or validation.
     *
     * @param providerId unique provider ID for this repeated-row projected profile
     * @param sourceProfile semantic source profile label
     * @param sourceDescriptor expected container protobuf descriptor
     * @param repeatedRowsField repeated message field on {@code sourceDescriptor} supplying rows
     * @param projection compiled projection from {@code repeatedRowsField.getMessageType()} to row
     * @return the configured repeated-row projected provider
     */
    public static ParquetRendererProvider projectedRepeated(
            String providerId,
            String sourceProfile,
            Descriptor sourceDescriptor,
            FieldDescriptor repeatedRowsField,
            MessageProjection projection) {
        return projectedRepeated(
                providerId,
                sourceProfile,
                sourceDescriptor,
                repeatedRowsField,
                projection,
                DEFAULT_MAX_INPUT_BYTES,
                DEFAULT_MAX_OUTPUT_BYTES);
    }

    /**
     * Creates a multi-row projected Parquet renderer with explicit byte budgets.
     *
     * @param providerId unique provider ID for this repeated-row projected profile
     * @param sourceProfile semantic source profile label
     * @param sourceDescriptor expected container protobuf descriptor
     * @param repeatedRowsField repeated message field on {@code sourceDescriptor} supplying rows
     * @param projection compiled projection from {@code repeatedRowsField.getMessageType()} to row
     * @param maxInputBytes maximum serialized input plus options bytes ({@code > 0})
     * @param maxOutputBytes maximum rendered Parquet bytes ({@code 1..HARD_MAX_OUTPUT_BYTES})
     * @return the configured repeated-row projected provider
     */
    public static ParquetRendererProvider projectedRepeated(
            String providerId,
            String sourceProfile,
            Descriptor sourceDescriptor,
            FieldDescriptor repeatedRowsField,
            MessageProjection projection,
            int maxInputBytes,
            int maxOutputBytes) {
        Objects.requireNonNull(sourceDescriptor, "sourceDescriptor");
        Objects.requireNonNull(repeatedRowsField, "repeatedRowsField");
        Objects.requireNonNull(projection, "projection");
        if (!repeatedRowsField.getContainingType().getFullName()
                        .equals(sourceDescriptor.getFullName())
                || sourceDescriptor.findFieldByNumber(repeatedRowsField.getNumber()) == null) {
            throw new IllegalArgumentException(
                    "Field " + repeatedRowsField.getFullName()
                            + " does not belong to source descriptor "
                            + sourceDescriptor.getFullName());
        }
        if (!repeatedRowsField.isRepeated()
                || repeatedRowsField.isMapField()
                || repeatedRowsField.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            throw new IllegalArgumentException(
                    "Field " + repeatedRowsField.getFullName()
                            + " must be a repeated message field (not a map or scalar)");
        }
        if (!projection.supports(repeatedRowsField.getMessageType())) {
            throw new IllegalArgumentException(
                    "Projection target " + projection.targetType().getFullName()
                            + " does not declare repeated element source "
                            + repeatedRowsField.getMessageType().getFullName());
        }
        String loss = projectedLossDescription(
                sourceDescriptor, repeatedRowsField, projection.targetType());
        return new ParquetRendererProvider(
                buildCapability(providerId, sourceProfile, loss, maxInputBytes, maxOutputBytes),
                sourceDescriptor,
                repeatedRowsField,
                projection);
    }

    @Override
    public RendererCapability capability() {
        return capability;
    }

    @Override
    public Descriptor optionsDescriptor() {
        return ParquetRendererOptions.getDescriptor();
    }

    @Override
    public void check(Message input, Message options) throws RenderFailure {
        prepare(input, options, () -> false);
    }

    @Override
    public void render(
            Message input,
            Message options,
            OutputStream output,
            Cancellation cancellation) throws IOException {
        Objects.requireNonNull(output, "output");
        Cancellation activeCancellation = Objects.requireNonNull(cancellation, "cancellation");
        PreparedRender prepared = prepare(input, options, activeCancellation);
        long maxUncompressedBytes = Math.max(
                prepared.effectiveMaxOutputBytes() * 8L,
                (long) capability.getMaxInputBytes() * 4L);
        ParquetEmitter.writeBounded(
                prepared.rowDescriptor(),
                prepared.rows(),
                ProtoParquetSchemas.FieldIdResolver.NONE,
                prepared.exportOptions(),
                output,
                prepared.effectiveMaxOutputBytes(),
                maxUncompressedBytes,
                activeCancellation);
    }

    private PreparedRender prepare(
            Message input, Message options, Cancellation cancellation) throws RenderFailure {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(cancellation, "cancellation").check();

        ParquetRendererOptions typedOptions = parseAndValidateOptions(options);
        long combinedInputBytes =
                (long) input.getSerializedSize() + options.getSerializedSize();
        if (combinedInputBytes > capability.getMaxInputBytes()) {
            throw new RenderFailure(
                    RenderFailure.Kind.LIMIT,
                    "Input plus options size (" + combinedInputBytes
                            + " bytes) exceeds max_input_bytes ("
                            + capability.getMaxInputBytes() + ")");
        }

        long requestedMaxOut = typedOptions.getMaxOutputBytes();
        if (requestedMaxOut < 0 || requestedMaxOut > capability.getMaxOutputBytes()) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    "ParquetRendererOptions.max_output_bytes (" + requestedMaxOut
                            + ") exceeds provider max_output_bytes ("
                            + capability.getMaxOutputBytes() + ")");
        }
        long effectiveMaxOutputBytes =
                requestedMaxOut > 0 ? requestedMaxOut : capability.getMaxOutputBytes();

        Set<String> selectedColumns = extractSelectedColumns(typedOptions);
        ParquetExportOptions exportOptions = toExportOptions(typedOptions, selectedColumns);

        Descriptor inputDescriptor = input.getDescriptorForType();
        if (projection != null) {
            verifyDeclaredSourceCompatibility(inputDescriptor);
        }

        Descriptor rowDescriptor =
                projection != null ? projection.targetType() : inputDescriptor;
        validateRowSchemaAndColumns(rowDescriptor, selectedColumns);

        // Refuse unknown wire fields anywhere on the input so unmodeled wire extensions are
        // never silently discarded during columnar conversion.
        requireNoUnknownWireFields(input, cancellation);

        // Validate the input message against all declared constraint/CEL rules before any
        // projection or masking runs.
        validateMessageRules(inputDescriptor, input, -1);

        List<Message> rows;
        if (projection == null) {
            requireRecognizedEnumsInSelectedColumns(input, selectedColumns, cancellation);
            rows = List.of(input);
        } else if (repeatedRowsField == null) {
            requireRecognizedEnumsInSelectedColumns(input, Set.of(), cancellation);
            Message projectedRow = projectSingleRow(input, -1);
            requireNoUnknownWireFields(projectedRow, cancellation);
            requireRecognizedEnumsInSelectedColumns(projectedRow, selectedColumns, cancellation);
            validateMessageRules(rowDescriptor, projectedRow, 0);
            rows = List.of(projectedRow);
        } else {
            requireRecognizedEnumsInSelectedColumns(input, Set.of(), cancellation);
            FieldDescriptor inputRepeatedField =
                    inputDescriptor.findFieldByNumber(repeatedRowsField.getNumber());
            int count = input.getRepeatedFieldCount(inputRepeatedField);
            List<Message> projectedRows = new ArrayList<>(count);
            ProtoValidator rowValidator = ProtoValidator.forMessageType(rowDescriptor);
            for (int i = 0; i < count; i++) {
                cancellation.check();
                Message element = (Message) input.getRepeatedField(inputRepeatedField, i);
                Message projectedRow = projectSingleRow(element, i);
                requireNoUnknownWireFields(projectedRow, cancellation);
                requireRecognizedEnumsInSelectedColumns(
                        projectedRow, selectedColumns, cancellation);
                ValidationResult rowValidation = evaluateRules(rowValidator, projectedRow);
                if (!rowValidation.valid()) {
                    throw new RenderFailure(
                            RenderFailure.Kind.INVALID,
                            formatValidationFailure(rowDescriptor, i, rowValidation));
                }
                projectedRows.add(projectedRow);
            }
            rows = List.copyOf(projectedRows);
        }

        cancellation.check();
        return new PreparedRender(
                rowDescriptor, rows, exportOptions, effectiveMaxOutputBytes);
    }

    private Message projectSingleRow(Message sourceElement, int rowIndex) throws RenderFailure {
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

    private ParquetRendererOptions parseAndValidateOptions(Message options) throws RenderFailure {
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

    private static Set<String> extractSelectedColumns(ParquetRendererOptions options)
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

    private static ParquetExportOptions toExportOptions(
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

    private void verifyDeclaredSourceCompatibility(Descriptor actualSource) throws RenderFailure {
        if (!actualSource.getFullName().equals(declaredSourceDescriptor.getFullName())) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Provider '" + capability.getProviderId() + "' expects source type '"
                            + declaredSourceDescriptor.getFullName() + "', got '"
                            + actualSource.getFullName() + "'");
        }
        if (repeatedRowsField != null) {
            FieldDescriptor actualField =
                    actualSource.findFieldByNumber(repeatedRowsField.getNumber());
            if (actualField == null
                    || !actualField.getName().equals(repeatedRowsField.getName())
                    || !actualField.isRepeated()
                    || actualField.isMapField()
                    || actualField.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || !actualField.getMessageType().getFullName()
                            .equals(repeatedRowsField.getMessageType().getFullName())) {
                throw new RenderFailure(
                        RenderFailure.Kind.UNSUPPORTED,
                        "Source descriptor '" + actualSource.getFullName()
                                + "' is incompatible with declared repeated row field '"
                                + repeatedRowsField.getName() + "'");
            }
            verifyFieldsCompatible(repeatedRowsField.getMessageType(), actualField.getMessageType());
        } else {
            verifyFieldsCompatible(declaredSourceDescriptor, actualSource);
        }
    }

    private static void verifyFieldsCompatible(Descriptor expected, Descriptor actual)
            throws RenderFailure {
        for (FieldDescriptor expectedField : expected.getFields()) {
            FieldDescriptor actualField = actual.findFieldByNumber(expectedField.getNumber());
            if (actualField == null
                    || !actualField.getName().equals(expectedField.getName())
                    || actualField.getType() != expectedField.getType()
                    || actualField.isRepeated() != expectedField.isRepeated()
                    || actualField.isMapField() != expectedField.isMapField()) {
                throw new RenderFailure(
                        RenderFailure.Kind.UNSUPPORTED,
                        "Source descriptor '" + actual.getFullName()
                                + "' is structurally incompatible on field '"
                                + expectedField.getName() + "' (#" + expectedField.getNumber()
                                + ")");
            }
            if (expectedField.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && !actualField.getMessageType().getFullName()
                            .equals(expectedField.getMessageType().getFullName())) {
                throw new RenderFailure(
                        RenderFailure.Kind.UNSUPPORTED,
                        "Source descriptor '" + actual.getFullName()
                                + "' differs in message type for field '"
                                + expectedField.getName() + "'");
            }
        }
    }

    private static void validateRowSchemaAndColumns(
            Descriptor rowDescriptor, Set<String> selectedColumns) throws RenderFailure {
        if (rowDescriptor.getFields().isEmpty()) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Empty message schema '" + rowDescriptor.getFullName()
                            + "' has zero columns and cannot be rendered as Parquet");
        }
        List<FieldDescriptor> topFields;
        if (selectedColumns.isEmpty()) {
            topFields = rowDescriptor.getFields();
        } else {
            topFields = new ArrayList<>(selectedColumns.size());
            for (String column : selectedColumns) {
                FieldDescriptor field = rowDescriptor.findFieldByName(column);
                if (field == null) {
                    throw new RenderFailure(
                            RenderFailure.Kind.INVALID,
                            "Unknown column '" + column + "' on row schema '"
                                    + rowDescriptor.getFullName() + "'");
                }
                topFields.add(field);
            }
            if (topFields.isEmpty()) {
                throw new RenderFailure(
                        RenderFailure.Kind.INVALID,
                        "Selected column list produced zero columns on '"
                                + rowDescriptor.getFullName() + "'");
            }
        }

        Deque<String> ancestry = new ArrayDeque<>();
        ancestry.push(rowDescriptor.getFullName());
        for (FieldDescriptor field : topFields) {
            inspectFieldSchema(field, field.getName(), ancestry);
        }
        ancestry.pop();

        try {
            ProtoParquetSchemas.schema(
                    rowDescriptor, ProtoParquetSchemas.FieldIdResolver.NONE, selectedColumns);
        } catch (IllegalArgumentException schemaFailure) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED, schemaFailure.getMessage(), schemaFailure);
        }
    }

    private static void inspectFieldSchema(
            FieldDescriptor field, String path, Deque<String> ancestry) throws RenderFailure {
        if (field.getType() == FieldDescriptor.Type.GROUP) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Proto2 group field '" + path + "' is not supported for Parquet");
        }
        if (field.isMapField()) {
            FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
            inspectFieldSchema(valueField, path + ".value", ancestry);
            return;
        }
        if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
            return;
        }
        Descriptor nested = field.getMessageType();
        String fullName = nested.getFullName();
        if (ANY_FULL_NAME.equals(fullName)) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Opaque google.protobuf.Any field '" + path
                            + "' cannot be rendered directly to Parquet; select an unpacked Any "
                            + "occurrence source, exclude the column via ParquetRendererOptions."
                            + "columns, or configure a declared MessageProjection profile");
        }
        if (ProtoParquetSchemas.TIMESTAMP.equals(fullName)
                || ProtoParquetSchemas.JSON_TYPES.contains(fullName)) {
            return;
        }
        if (ancestry.contains(fullName)) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Recursive message schema at '" + path + "' ("
                            + String.join(" -> ", ancestry.reversed()) + " -> " + fullName
                            + ") cannot be rendered as a columnar Parquet file");
        }
        if (nested.getFields().isEmpty()) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Empty nested message schema '" + fullName + "' at '" + path
                            + "' has zero columns and cannot be rendered as Parquet");
        }
        ancestry.push(fullName);
        for (FieldDescriptor child : nested.getFields()) {
            inspectFieldSchema(child, path + "." + child.getName(), ancestry);
        }
        ancestry.pop();
    }

    private static void requireNoUnknownOptionFields(Message root) throws RenderFailure {
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

    private static void requireNoUnknownWireFields(Message root, Cancellation cancellation)
            throws RenderFailure {
        record Frame(Message message, int depth) {}
        Deque<Frame> stack = new ArrayDeque<>();
        stack.push(new Frame(root, 0));
        while (!stack.isEmpty()) {
            cancellation.check();
            Frame frame = stack.pop();
            if (frame.depth() > MAX_MESSAGE_WALK_DEPTH) {
                throw new RenderFailure(
                        RenderFailure.Kind.LIMIT,
                        "Input message exceeds maximum nesting depth ("
                                + MAX_MESSAGE_WALK_DEPTH + ")");
            }
            Message current = frame.message();
            if (!current.getUnknownFields().asMap().isEmpty()) {
                throw new RenderFailure(
                        RenderFailure.Kind.UNSUPPORTED,
                        "Source message '" + current.getDescriptorForType().getFullName()
                                + "' contains unknown wire fields "
                                + current.getUnknownFields().asMap().keySet()
                                + " which would be lost in Parquet conversion");
            }
            for (var entry : current.getAllFields().entrySet()) {
                FieldDescriptor field = entry.getKey();
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    continue;
                }
                if (field.isRepeated()) {
                    List<?> items = (List<?>) entry.getValue();
                    for (int i = items.size() - 1; i >= 0; i--) {
                        stack.push(new Frame((Message) items.get(i), frame.depth() + 1));
                    }
                } else {
                    stack.push(new Frame((Message) entry.getValue(), frame.depth() + 1));
                }
            }
        }
    }

    private static void requireRecognizedEnumsInSelectedColumns(
            Message root, Set<String> selectedTopColumns, Cancellation cancellation)
            throws RenderFailure {
        record EnumFrame(Message message, String pathPrefix, boolean rootLevel, int depth) {}
        Deque<EnumFrame> stack = new ArrayDeque<>();
        stack.push(new EnumFrame(root, "", true, 0));
        while (!stack.isEmpty()) {
            cancellation.check();
            EnumFrame frame = stack.pop();
            if (frame.depth() > MAX_MESSAGE_WALK_DEPTH) {
                throw new RenderFailure(
                        RenderFailure.Kind.LIMIT,
                        "Message exceeds maximum nesting depth (" + MAX_MESSAGE_WALK_DEPTH + ")");
            }
            Message msg = frame.message();
            for (var entry : msg.getAllFields().entrySet()) {
                FieldDescriptor field = entry.getKey();
                if (frame.rootLevel()
                        && !selectedTopColumns.isEmpty()
                        && !selectedTopColumns.contains(field.getName())) {
                    continue;
                }
                String path = frame.pathPrefix().isEmpty()
                        ? field.getName()
                        : frame.pathPrefix() + "." + field.getName();
                if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                    if (field.isRepeated()) {
                        List<?> values = (List<?>) entry.getValue();
                        for (int i = 0; i < values.size(); i++) {
                            checkEnumValue(
                                    field,
                                    (EnumValueDescriptor) values.get(i),
                                    path + "[" + i + "]");
                        }
                    } else {
                        checkEnumValue(field, (EnumValueDescriptor) entry.getValue(), path);
                    }
                } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                    if (ANY_FULL_NAME.equals(field.getMessageType().getFullName())) {
                        continue;
                    }
                    if (field.isRepeated()) {
                        List<?> items = (List<?>) entry.getValue();
                        for (int i = items.size() - 1; i >= 0; i--) {
                            stack.push(new EnumFrame(
                                    (Message) items.get(i),
                                    path + "[" + i + "]",
                                    false,
                                    frame.depth() + 1));
                        }
                    } else {
                        stack.push(new EnumFrame(
                                (Message) entry.getValue(), path, false, frame.depth() + 1));
                    }
                }
            }
        }
    }

    private static void checkEnumValue(
            FieldDescriptor field, EnumValueDescriptor value, String path) throws RenderFailure {
        if (value.getIndex() < 0
                || field.getEnumType().findValueByNumber(value.getNumber()) == null) {
            throw new RenderFailure(
                    RenderFailure.Kind.UNSUPPORTED,
                    "Unrecognized enum number " + value.getNumber() + " at '" + path
                            + "' for enum " + field.getEnumType().getFullName());
        }
    }

    private static void validateMessageRules(
            Descriptor descriptor, Message message, int rowIndex) throws RenderFailure {
        ValidationResult result =
                evaluateRules(ProtoValidator.forMessageType(descriptor), message);
        if (!result.valid()) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    formatValidationFailure(descriptor, rowIndex, result));
        }
    }

    private static ValidationResult evaluateRules(ProtoValidator validator, Message message)
            throws RenderFailure {
        try {
            return validator.validate(message);
        } catch (RuleCompilationException | RuleEvaluationException failure) {
            throw new RenderFailure(
                    RenderFailure.Kind.INVALID,
                    "Validation rule evaluation failed on "
                            + message.getDescriptorForType().getFullName() + ": "
                            + failure.getMessage(),
                    failure);
        }
    }

    private static String formatValidationFailure(
            Descriptor descriptor, int rowIndex, ValidationResult result) {
        StringBuilder sb = new StringBuilder();
        if (rowIndex >= 0) {
            sb.append("Row ").append(rowIndex).append(" of type ");
        } else {
            sb.append("Message of type ");
        }
        sb.append(descriptor.getFullName()).append(" failed validation:");
        for (ValidationResult.Violation violation : result.violations()) {
            sb.append(" [").append(violation.path()).append("] ")
                    .append(violation.ruleId()).append(": ").append(violation.message());
        }
        return sb.toString();
    }

    private static RendererCapability buildCapability(
            String providerId,
            String sourceProfile,
            String lossDescription,
            int maxInputBytes,
            int maxOutputBytes) {
        if (providerId == null || providerId.isBlank()) {
            throw new IllegalArgumentException("providerId must not be blank");
        }
        if (sourceProfile == null || sourceProfile.isBlank()) {
            throw new IllegalArgumentException("sourceProfile must not be blank");
        }
        if (maxInputBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxInputBytes must be positive, got " + maxInputBytes);
        }
        if (maxOutputBytes <= 0 || maxOutputBytes > HARD_MAX_OUTPUT_BYTES) {
            throw new IllegalArgumentException(
                    "maxOutputBytes must be in 1.." + HARD_MAX_OUTPUT_BYTES
                            + ", got " + maxOutputBytes);
        }
        RendererCapability built = RendererCapability.newBuilder()
                .setProviderId(providerId.trim())
                .setProviderVersion(PROVIDER_VERSION)
                .setSourceProfile(sourceProfile.trim())
                .setMediaType(PARQUET_MEDIA_TYPE)
                .setExtension(PARQUET_EXTENSION)
                .setOptionsType(ParquetRendererOptions.getDescriptor().getFullName())
                .setOptionsFingerprint(OPTIONS_FINGERPRINT)
                .setLossDescription(lossDescription)
                .setMaxInputBytes(maxInputBytes)
                .setMaxOutputBytes(maxOutputBytes)
                .build();
        ValidationResult check =
                ProtoValidator.forMessageType(RendererCapability.getDescriptor()).validate(built);
        if (!check.valid()) {
            throw new IllegalArgumentException(
                    "Invalid RendererCapability for '" + providerId + "': " + check.violations());
        }
        return built;
    }

    private static String projectedLossDescription(
            Descriptor sourceDescriptor,
            FieldDescriptor repeatedField,
            Descriptor targetRowDescriptor) {
        String sourceSummary = repeatedField == null
                ? sourceDescriptor.getFullName() + " -> " + targetRowDescriptor.getFullName()
                : sourceDescriptor.getFullName() + "." + repeatedField.getName()
                        + " (repeated " + repeatedField.getMessageType().getFullName()
                        + ") -> " + targetRowDescriptor.getFullName();
        return "Derived non-authoritative columnar Parquet artifact projected via "
                + sourceSummary
                + "; unprojected source fields and unknown wire fields are omitted, "
                + "google.protobuf.Struct/Value/ListValue map to UTF-8 JSON strings, "
                + "google.protobuf.Timestamp maps to microsecond UTC timestamps, and "
                + "unsigned 64-bit integers use Parquet UINT_64 over signed INT64 storage.";
    }

    private record PreparedRender(
            Descriptor rowDescriptor,
            List<Message> rows,
            ParquetExportOptions exportOptions,
            long effectiveMaxOutputBytes) {}
}
