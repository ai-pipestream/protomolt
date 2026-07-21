package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * A Kafka protobuf serializer that writes the Confluent wire format and enforces the schema's own
 * rules on the way out.
 *
 * <p>The rules are the ones already declared on the descriptor, so a producer cannot forget to
 * call a validator: validation is not the application's code path any more, it is the
 * serializer's, and a message that violates its contract is rejected instead of written. Because
 * the schema comes from a descriptor set the deployment packages rather than from a registry
 * lookup, there is no per-message network hop and no registry to be down.</p>
 *
 * <p>With {@code protomolt.message.type} set the serializer is pinned to one type. Unset, it
 * accepts any type declared in the descriptor set — several event types sharing one topic — with
 * each type validated against its own packaged contract. The descriptor set stays the boundary
 * either way: a message whose type it does not declare is not part of this producer's contract
 * and is refused.</p>
 *
 * @see ProtoMoltSerdeConfig
 */
public class ProtoMoltProtobufSerializer implements Serializer<Message> {

    /** Bound on the caller-descriptor trust cache; a producer usually has a handful at most. */
    private static final int MAX_TRUSTED_CALLERS = 64;
    /** Bound on the packaged-versus-latest verdict cache, sized the same way. */
    private static final int MAX_COMPAT_VERDICTS = 64;

    private List<FileDescriptor> files;
    private ProtoValidator validator;
    private ai.pipestream.proto.quality.QualityScorer quality;
    private Descriptor pinnedType;
    private int configuredSchemaId;
    private boolean validateOnWrite;
    private boolean qualityOnWrite;
    private Double qualityMin;
    private SchemaIds schemaIds;
    private String subjectOverride;
    private String subjectStrategy;
    private boolean isKey;
    private SerdeMetricsListener metrics;
    private SerdeMapper mapper;
    private boolean latestCompatibilityStrict;
    private ai.pipestream.proto.compat.CompatibilityChecker compatChecker;
    // Verdicts on (packaged file, registry file) pairs; both sides are cached instances.
    private final ConcurrentMap<SchemaPair, ai.pipestream.proto.compat.CompatibilityResult>
            compatVerdicts = new ConcurrentHashMap<>();
    // Packaged types by full name, resolved once per type; absent types marked by MISSING.
    private final ConcurrentMap<String, Descriptor> packagedByName = new ConcurrentHashMap<>();
    // Caller descriptors already proven byte-identical to their packaged schema (see below).
    private final ConcurrentMap<Descriptor, Boolean> sameSchema = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        ProtoMoltSerdeConfig config = new ProtoMoltSerdeConfig(configs);
        files = Descriptors.load(config, getClass().getClassLoader());
        String pinned = config.getString(ProtoMoltSerdeConfig.MESSAGE_TYPE);
        pinnedType = pinned != null ? SerdeDescriptors.messageType(files, pinned) : null;
        configuredSchemaId = config.getInt(ProtoMoltSerdeConfig.USE_SCHEMA_ID);
        validateOnWrite = config.getBoolean(ProtoMoltSerdeConfig.VALIDATE_ON_WRITE);
        qualityOnWrite = config.getBoolean(ProtoMoltSerdeConfig.QUALITY_ON_WRITE);
        qualityMin = config.getDouble(ProtoMoltSerdeConfig.QUALITY_MIN);
        validator = ProtoValidator.create();
        quality = ai.pipestream.proto.quality.QualityScorer.create();
        metrics = SerdeMetricsListeners.load(getClass().getClassLoader());
        schemaIds = SchemaIds.create(config.getString(ProtoMoltSerdeConfig.SCHEMA_REGISTRY_URL),
                config.getLong(ProtoMoltSerdeConfig.REGISTRY_RETRY_BACKOFF_MS), metrics);
        subjectOverride = config.getString(ProtoMoltSerdeConfig.SUBJECT);
        subjectStrategy = config.getString(ProtoMoltSerdeConfig.SUBJECT_NAME_STRATEGY);
        latestCompatibilityStrict =
                config.getBoolean(ProtoMoltSerdeConfig.LATEST_COMPATIBILITY_STRICT);
        compatChecker = ai.pipestream.proto.compat.CompatibilityChecker.create();
        mapper = SerdeMapper.parse(config.getList(ProtoMoltSerdeConfig.MAP_ON_WRITE),
                ProtoMoltSerdeConfig.MAP_ON_WRITE);
        this.isKey = isKey;
    }

    @Override
    public void close() {
        if (schemaIds != null) {
            schemaIds.close();
        }
    }

    @Override
    public byte[] serialize(String topic, Message data) {
        if (data == null) {
            return null;
        }
        Descriptor packaged = packagedTypeFor(topic, data);
        if (mapper != null) {
            // Normalize before judging: validation and quality see the mapped message, and the
            // mapped message is what reaches the topic. Only mapping failures are translated;
            // anything else is a bug and propagates untouched.
            try {
                data = mapper.apply(data);
            } catch (ai.pipestream.proto.mapper.MappingException
                     | ai.pipestream.proto.cel.CelEvaluationException e) {
                throw new SerializationException("A mapping rule could not be applied to a "
                        + packaged.getFullName() + " bound for " + topic + ": "
                        + e.getMessage(), e);
            }
        }
        byte[] payload = data.toByteArray();
        Message contract = validateOnWrite || qualityOnWrite
                ? asPackagedType(data, payload, packaged)
                : null;
        if (validateOnWrite) {
            ValidationResult result = validator.validate(contract);
            if (!result.valid()) {
                metrics.onValidationRejected(topic, packaged.getFullName(), true,
                        result.violations().stream()
                                .map(ValidationResult.Violation::ruleId).toList());
                throw new SerializationException("Message violates the schema's declared rules, "
                        + "so it was not written to " + topic + ": " + describe(result));
            }
        }
        if (qualityOnWrite) {
            measureQuality(topic, packaged, contract);
        }
        Frame frame = frameFor(topic, packaged);
        byte[] framed = ConfluentWireFormat.frame(frame.id(), frame.index(), payload);
        metrics.onSerialized(topic, packaged.getFullName());
        return framed;
    }

    /** The packaged descriptor declaring {@code data}'s type: the contract this producer ships. */
    private Descriptor packagedTypeFor(String topic, Message data) {
        String fullName = data.getDescriptorForType().getFullName();
        if (pinnedType != null) {
            if (!fullName.equals(pinnedType.getFullName())) {
                metrics.onTypeRefused(topic, SerdeMetricsListener.REASON_WRONG_TYPE);
                throw new SerializationException("This serializer is configured for "
                        + pinnedType.getFullName() + " but was handed a " + fullName);
            }
            return pinnedType;
        }
        Descriptor packaged = packagedByName.computeIfAbsent(fullName, name -> {
            Descriptor found = SerdeDescriptors.findMessageType(files, name);
            return found != null ? found : MISSING;
        });
        if (packaged == MISSING) {
            metrics.onTypeRefused(topic, SerdeMetricsListener.REASON_NOT_IN_CONTRACT);
            throw new SerializationException(fullName + " is not declared in the configured "
                    + "descriptor set, so it is not part of this producer's contract");
        }
        return packaged;
    }

    /** Sentinel for types proven absent, since a ConcurrentMap cannot hold null. */
    private static final Descriptor MISSING =
            com.google.protobuf.Empty.getDescriptor();

    /**
     * Measures the message against the quality dimensions its schema declares and reports the
     * scores. Quality is a measurement, not a gate — unless {@code protomolt.quality.min} is
     * set, in which case a composite below the floor keeps the record off the topic, the same
     * way a validation failure would.
     */
    private void measureQuality(String topic, Descriptor packaged, Message contract) {
        ai.pipestream.proto.quality.QualityReport report = quality.score(contract);
        if (!report.scored()) {
            return;
        }
        metrics.onQualityScored(topic, packaged.getFullName(), report.composite(),
                report.dimensions());
        if (qualityMin != null && report.composite() < qualityMin) {
            metrics.onQualityRejected(topic, packaged.getFullName(), report.composite());
            throw new SerializationException(String.format(
                    "Message scored %.3f against its schema's quality dimensions, below the "
                            + "configured floor of %.3f, so it was not written to %s: %s",
                    report.composite(), qualityMin, topic, report.dimensions()));
        }
    }

    /** A schema id and the message-index path within the schema that id names. */
    private record Frame(int id, List<Integer> index) {
    }

    /**
     * The id and index stamped into the frame, which must describe the same file.
     *
     * <p>A message-index path is a position in the schema the frame's id names. When the registry
     * supplies the id, the index is therefore computed against the <em>registry's</em> schema:
     * the packaged file can declare the same type at a different position, and a consumer
     * following the id would land on the wrong message. When the registry cannot supply both
     * halves — no id for the subject, or a registered schema that does not declare this type —
     * the frame falls back to the configured id and the packaged index, a pair consistent with
     * itself.</p>
     */
    private Frame frameFor(String topic, Descriptor packaged) {
        if (schemaIds != null) {
            String subject = subjectOverride != null ? subjectOverride
                    : Subjects.of(subjectStrategy, topic, packaged.getFullName(), isKey);
            OptionalInt id = schemaIds.idForSubject(subject);
            if (id.isPresent()) {
                Descriptor writerType = schemaIds.typeInSchema(id.getAsInt(),
                        packaged.getFullName());
                if (writerType != null) {
                    ensureLatestCanRead(topic, packaged, writerType, subject, id.getAsInt());
                    return new Frame(id.getAsInt(), ConfluentWireFormat.indexPath(writerType));
                }
            }
        }
        return new Frame(configuredSchemaId, ConfluentWireFormat.indexPath(packaged));
    }

    /** A packaged file and the registered file whose id would be stamped over its bytes. */
    private record SchemaPair(FileDescriptor packaged, FileDescriptor latest) {
    }

    /**
     * The strict half of use-latest-version semantics. The frame's id names the subject's
     * latest registered schema, and a consumer following the id reads the bytes with
     * <em>that</em> schema — not the packaged one that produced them. When the two have
     * diverged incompatibly (a field's wire type changed, say), stamping the id would hand
     * every reader a schema that misreads the record, silently. So the write is refused,
     * the same way a validation failure refuses it, until the schemas agree again — unless
     * {@code latest.compatibility.strict} is off, which is Confluent's off switch
     * too. The check is binary wire rules only ({@code BACKWARD}: the registered schema must
     * read data the packaged schema wrote), and the verdict is cached per schema pair.
     */
    private void ensureLatestCanRead(String topic, Descriptor packaged, Descriptor latest,
                                     String subject, int schemaId) {
        if (!latestCompatibilityStrict) {
            return;
        }
        if (compatVerdicts.size() >= MAX_COMPAT_VERDICTS) {
            compatVerdicts.clear();
        }
        ai.pipestream.proto.compat.CompatibilityResult verdict = compatVerdicts.computeIfAbsent(
                new SchemaPair(packaged.getFile(), latest.getFile()),
                pair -> compatChecker.check(pair.packaged(), pair.latest(),
                        ai.pipestream.proto.compat.CompatibilityMode.BACKWARD));
        if (!verdict.isCompatible()) {
            metrics.onTypeRefused(topic, SerdeMetricsListener.REASON_INCOMPATIBLE_WITH_LATEST);
            throw new SerializationException("The latest schema registered under subject "
                    + subject + " (id " + schemaId + ") cannot read a " + packaged.getFullName()
                    + " written with the packaged schema, so the record was not written to "
                    + topic + ". Readers follow the frame's id, so stamping it would misread "
                    + "every record. Violations: " + verdict.violations().stream()
                            .map(Object::toString).collect(Collectors.joining("; ")));
        }
    }

    /**
     * The rules enforced are the packaged schema's, not the caller's.
     *
     * <p>A message arrives carrying whatever descriptor the application built it from — often a
     * generated class compiled from a {@code .proto} that may or may not still match the
     * descriptor set this serde was configured with. When the caller's schema is byte-identical
     * to the packaged one (its file and every transitive import), validating the message
     * directly enforces the same contract, so that proof is cached per descriptor and the common
     * case costs nothing per record. A caller whose schema differs — stale protos, or options
     * that lost their extensions — is re-read under the packaged type: one parse, in exchange
     * for the guarantee that the contract enforced is the one this serde was configured with.</p>
     */
    private Message asPackagedType(Message data, byte[] payload, Descriptor packaged) {
        if (sharesPackagedSchema(data.getDescriptorForType(), packaged)) {
            return data;
        }
        try {
            return DynamicMessage.parseFrom(packaged, payload);
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("A " + packaged.getFullName()
                    + " could not be read back under the configured schema: " + e.getMessage(), e);
        }
    }

    /** Whether {@code caller}'s schema is the packaged schema, byte for byte. Cached. */
    private boolean sharesPackagedSchema(Descriptor caller, Descriptor packaged) {
        if (caller == packaged) {
            return true;
        }
        Boolean known = sameSchema.get(caller);
        if (known != null) {
            return known;
        }
        if (sameSchema.size() >= MAX_TRUSTED_CALLERS) {
            sameSchema.clear();
        }
        return sameSchema.computeIfAbsent(caller,
                d -> fileClosure(d.getFile()).equals(fileClosure(packaged.getFile())));
    }

    /**
     * The file and its transitive imports, keyed by file name, in comparable serialized form.
     * Imports matter: rules on an imported message type live in the imported file's options, so
     * equality of the root file alone would not prove the schemas agree. Source info is stripped
     * because descriptor sets built with {@code --include_source_info} differ from runtime
     * descriptors only in that metadata.
     */
    private static Map<String, com.google.protobuf.ByteString> fileClosure(FileDescriptor root) {
        Map<String, com.google.protobuf.ByteString> files = new java.util.HashMap<>();
        java.util.ArrayDeque<FileDescriptor> queue = new java.util.ArrayDeque<>(List.of(root));
        while (!queue.isEmpty()) {
            FileDescriptor file = queue.pop();
            com.google.protobuf.ByteString stripped = file.toProto().toBuilder()
                    .clearSourceCodeInfo().build().toByteString();
            if (files.put(file.getName(), stripped) == null) {
                queue.addAll(file.getDependencies());
            }
        }
        return files;
    }

    /** Every violation, not just the first: a producer fixing them one at a time is a slow loop. */
    private static String describe(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + " " + violation.message()
                        + " (" + violation.ruleId() + ")")
                .collect(Collectors.joining("; "));
    }

    /** Shared config-to-descriptors handling, so both serdes fail the same way. */
    static final class Descriptors {

        private Descriptors() {
        }

        static List<FileDescriptor> load(ProtoMoltSerdeConfig config, ClassLoader loader) {
            List<FileDescriptor> files = loadIfPresent(config, loader);
            if (files == null) {
                throw new ConfigException("Exactly one of "
                        + ProtoMoltSerdeConfig.DESCRIPTOR_SET_RESOURCE + " or "
                        + ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64 + " must be set");
            }
            return files;
        }

        /**
         * The packaged descriptors, or null when neither descriptor source is set. The
         * serializer calls {@link #load} — it writes from the packaged schema, so it cannot
         * do without one; the deserializer tolerates the null when a registry is configured.
         */
        static List<FileDescriptor> loadIfPresent(ProtoMoltSerdeConfig config, ClassLoader loader) {
            String resource = config.getString(ProtoMoltSerdeConfig.DESCRIPTOR_SET_RESOURCE);
            var base64 = config.getPassword(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64);
            if (resource != null && base64 != null) {
                throw new ConfigException("Exactly one of "
                        + ProtoMoltSerdeConfig.DESCRIPTOR_SET_RESOURCE + " or "
                        + ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64 + " must be set");
            }
            if (resource != null) {
                return SerdeDescriptors.fromClasspath(resource, loader);
            }
            return base64 != null ? SerdeDescriptors.fromBase64(base64.value()) : null;
        }
    }
}
