package ai.protomolt.proto.emit.parquet;

import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.requireNoUnknownWireFields;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.validateMessageRules;
import static ai.protomolt.proto.emit.parquet.ParquetInputChecks.validateRowSchemaAndColumns;
import static ai.protomolt.proto.emit.parquet.ParquetOptionChecks.OPTIONS_FINGERPRINT;
import static ai.protomolt.proto.emit.parquet.ParquetOptionChecks.extractSelectedColumns;
import static ai.protomolt.proto.emit.parquet.ParquetOptionChecks.parseAndValidateOptions;
import static ai.protomolt.proto.emit.parquet.ParquetOptionChecks.toExportOptions;

import ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions;
import ai.protomolt.proto.emit.renderer.RenderFailure;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.projection.MessageProjection;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.OutputStream;
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

    private static final String SINGLE_DOCUMENT_LOSS_DESCRIPTION =
            "Derived non-authoritative columnar Parquet artifact (1 row per selected protobuf "
                    + "message); does not replace authoritative protobuf state, omits unknown "
                    + "fields, maps google.protobuf.Struct/Value/ListValue to UTF-8 JSON strings, "
                    + "maps google.protobuf.Timestamp to microsecond UTC timestamps, widens "
                    + "unsigned 32-bit integers to INT64, and stores unsigned 64-bit integers with "
                    + "Parquet UINT_64 logical annotations over signed INT64 physical storage.";

    private final RendererCapability capability;
    private final ParquetRowMode mode;

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
                SingleDocumentRows.INSTANCE);
    }

    private ParquetRendererProvider(RendererCapability capability, ParquetRowMode mode) {
        this.capability = Objects.requireNonNull(capability, "capability");
        this.mode = Objects.requireNonNull(mode, "mode");
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
                SingleDocumentRows.INSTANCE);
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
                new ProjectedRows(sourceDescriptor, projection));
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
                new RepeatedProjectedRows(sourceDescriptor, repeatedRowsField, projection));
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
        mode.verifySource(capability.getProviderId(), inputDescriptor);
        Descriptor rowDescriptor = mode.rowDescriptor(inputDescriptor);
        validateRowSchemaAndColumns(rowDescriptor, selectedColumns);

        // Refuse unknown wire fields anywhere on the input so unmodeled wire extensions are
        // never silently discarded during columnar conversion.
        requireNoUnknownWireFields(input, cancellation);

        // Validate the input message against all declared constraint/CEL rules before any
        // projection or masking runs.
        validateMessageRules(inputDescriptor, input, -1);

        List<Message> rows = mode.rows(input, selectedColumns, cancellation);

        cancellation.check();
        return new PreparedRender(
                rowDescriptor, rows, exportOptions, effectiveMaxOutputBytes);
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
