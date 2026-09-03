package ai.protomolt.proto.screening;

import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.meta.FieldMeta;
import ai.protomolt.proto.meta.SensitivityMasker.PayloadResolver;
import ai.protomolt.proto.meta.SensitivityMasker;
import ai.protomolt.proto.types.ScreeningConfig;
import ai.protomolt.proto.types.ScreeningPolicy;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Applies one screening mount to a message: every string field whose
 * {@code meta.v1} sensitivity equals the mount's screened class runs through
 * the engine, and each detection at or above the threshold acts by policy —
 * mask the span, tag the finding, or (explicit opt-in only) refuse the
 * message. What the schema declares is <em>that</em> a field is screened;
 * the verdicts here are runtime facts carried with the model version and
 * threshold as evidence, never as schema validity.
 *
 * <p>Masked spans are replaced with {@code ***}, the same literal the
 * sensitivity masker's REDACT strategy uses, so a masked value reads
 * identically wherever it was masked. Below-threshold detections are
 * dropped, never recorded: an unactioned finding would itself be evidence
 * of the text's content. Findings never carry the detected text for the
 * same reason.
 *
 * <p>The walk covers singular and repeated string fields, recurses into
 * singular, repeated, and map-valued nested messages, and screens
 * string-valued map entries under the map field's own sensitivity. A packed
 * {@code google.protobuf.Any} payload is opened through the sensitivity
 * masker's {@link PayloadResolver} seam, screened as any other message, and
 * repacked under the same type URL only when a span was masked; a payload
 * whose type the resolver cannot answer is reported in
 * {@link Verdict#unresolvedPaths()}, never failed and never passed over in
 * silence — its fields are exactly the ones nobody screened.
 */
public final class Screener {

    /** The masker's REDACT literal, shared so masked values read identically. */
    private static final String MASKED = "***";

    private static final String ANY_TYPE = "google.protobuf.Any";

    private final ScreeningEngine engine;
    private final ScreeningConfig config;
    private final PayloadResolver resolver;

    /**
     * @param engine the detection engine
     * @param config the mount: screened class, threshold, and policy
     */
    public Screener(ScreeningEngine engine, ScreeningConfig config) {
        this(engine, config, null);
    }

    /**
     * @param engine the detection engine
     * @param config the mount: screened class, threshold, and policy
     * @param resolver resolves packed payload types; null selects the default,
     *        the screened message's own file and its transitive imports
     */
    public Screener(ScreeningEngine engine, ScreeningConfig config, PayloadResolver resolver) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.config = Objects.requireNonNull(config, "config");
        this.resolver = resolver;
        if (config.getSensitivityClass().isEmpty()) {
            throw new IllegalArgumentException("config.sensitivity_class must be present");
        }
        if (config.getPolicy() == ScreeningPolicy.SCREENING_POLICY_UNSPECIFIED
                || config.getPolicy() == ScreeningPolicy.UNRECOGNIZED) {
            throw new IllegalArgumentException("config.policy must be a defined policy");
        }
    }

    /**
     * The screening outcome: the (possibly masked) message and the findings.
     * A non-empty {@link #unresolvedPaths()} means packed payloads went
     * unscreened; a caller treating screening as a boundary decides what an
     * unopened payload means rather than assume the output is clean.
     *
     * @param message the message after policy application
     * @param findings one entry per acted-on detection
     * @param unresolvedPaths packed payloads whose type the resolver could not answer
     */
    public record Verdict(Message message, List<Finding> findings,
                          List<String> unresolvedPaths) {
    }

    /**
     * One acted-on detection, carried as evidence: the field it happened on,
     * the entity type, the model's confidence, and the model version and
     * threshold that produced the verdict. Never the detected text.
     *
     * @param path dotted proto-name path of the screened field
     * @param type the detected entity type
     * @param confidence the model's confidence
     * @param modelVersion the model version that produced the detection
     * @param threshold the mount's threshold at verdict time
     * @param policy the policy that acted
     */
    public record Finding(String path, String type, double confidence,
                          String modelVersion, double threshold, ScreeningPolicy policy) {
    }

    /** Thrown when the mount's policy is REFUSE and a detection is at threshold. */
    public static final class ScreeningRefusedException extends RuntimeException {
        private final transient Finding finding;

        ScreeningRefusedException(Finding finding) {
            super("screening refused the message: " + finding.type() + " detected at "
                    + finding.path() + " by model " + finding.modelVersion()
                    + " (threshold " + finding.threshold() + ")");
            this.finding = finding;
        }

        public Finding finding() {
            return finding;
        }
    }

    /**
     * Screens one message under the mount.
     *
     * @param message the message to screen
     * @return the verdict: the message after policy application plus findings
     * @throws ScreeningRefusedException when the policy is REFUSE and a
     *         detection is at or above the threshold
     */
    public Verdict screen(Message message) {
        Objects.requireNonNull(message, "message");
        List<Finding> findings = new ArrayList<>();
        List<String> unresolved = new ArrayList<>();
        Pass pass = new Pass(findings, unresolved,
                resolver != null ? resolver : SensitivityMasker.importedTypes(message));
        Message screened = walk(message, "", pass);
        return new Verdict(screened, List.copyOf(findings), List.copyOf(unresolved));
    }

    /** One screen()'s shared state: findings, unresolved payloads, the resolver. */
    private record Pass(List<Finding> findings, List<String> unresolved,
                        PayloadResolver resolver) {
    }

    private Message walk(Message message, String prefix, Pass pass) {
        List<Finding> findings = pass.findings();
        Message.Builder builder = null;
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            FieldDescriptor field = entry.getKey();
            String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            boolean screened = screenedClass(field);
            if (field.isMapField()) {
                builder = walkMap(message, builder, field, path, screened, pass);
            } else if (field.isRepeated()) {
                int count = message.getRepeatedFieldCount(field);
                for (int i = 0; i < count; i++) {
                    String elementPath = path + "[" + i + "]";
                    if (field.getJavaType() == FieldDescriptor.JavaType.STRING && screened) {
                        String masked = screenValue(
                                (String) message.getRepeatedField(field, i),
                                elementPath, findings);
                        if (masked != null) {
                            builder = builder(message, builder);
                            builder.setRepeatedField(field, i, masked);
                        }
                    } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                        Message nested = (Message) message.getRepeatedField(field, i);
                        Message walked = walkNested(nested, elementPath, pass);
                        if (walked != nested) {
                            builder = builder(message, builder);
                            builder.setRepeatedField(field, i, walked);
                        }
                    }
                }
            } else if (field.getJavaType() == FieldDescriptor.JavaType.STRING && screened) {
                String masked = screenValue((String) entry.getValue(), path, findings);
                if (masked != null) {
                    builder = builder(message, builder);
                    builder.setField(field, masked);
                }
            } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                Message nested = (Message) entry.getValue();
                Message walked = walkNested(nested, path, pass);
                if (walked != nested) {
                    builder = builder(message, builder);
                    builder.setField(field, walked);
                }
            }
        }
        return builder == null ? message : builder.build();
    }

    /** Nested dispatch: a packed payload opens through the resolver, the rest recurse. */
    private Message walkNested(Message nested, String path, Pass pass) {
        if (ANY_TYPE.equals(nested.getDescriptorForType().getFullName())) {
            return screenAny(nested, path, pass);
        }
        return walk(nested, path, pass);
    }

    /**
     * Opens one packed payload, screens it, and repacks under the same type URL
     * only when a span was masked — a payload nothing changed keeps its exact
     * bytes. An unanswerable type or bytes that disagree with the type URL are
     * reported, never failed.
     */
    private Message screenAny(Message any, String path, Pass pass) {
        FieldDescriptor urlField = any.getDescriptorForType().findFieldByName("type_url");
        FieldDescriptor valueField = any.getDescriptorForType().findFieldByName("value");
        if (urlField == null || valueField == null) {
            return any;
        }
        String typeUrl = (String) any.getField(urlField);
        ByteString packed = (ByteString) any.getField(valueField);
        if (typeUrl.isEmpty() || packed.isEmpty()) {
            return any;
        }
        String typeName = typeUrl.substring(typeUrl.lastIndexOf('/') + 1);
        Descriptor payloadType = pass.resolver().find(typeName);
        if (payloadType == null) {
            pass.unresolved().add(path);
            return any;
        }
        Message payload;
        try {
            payload = DynamicMessage.parseFrom(payloadType, packed);
        } catch (InvalidProtocolBufferException e) {
            pass.unresolved().add(path);
            return any;
        }
        Message screened = walk(payload, path, pass);
        if (screened == payload) {
            return any;
        }
        return any.toBuilder().setField(valueField, screened.toByteString()).build();
    }

    /** Map entries screen their values under the map field's own sensitivity. */
    private Message.Builder walkMap(Message message, Message.Builder builder,
            FieldDescriptor field, String path, boolean screened, Pass pass) {
        List<Finding> findings = pass.findings();
        FieldDescriptor keyField = field.getMessageType().findFieldByName("key");
        FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
        int count = message.getRepeatedFieldCount(field);
        for (int i = 0; i < count; i++) {
            Message mapEntry = (Message) message.getRepeatedField(field, i);
            String entryPath = path + "[" + mapEntry.getField(keyField) + "]";
            if (valueField.getJavaType() == FieldDescriptor.JavaType.STRING && screened) {
                String masked = screenValue(
                        (String) mapEntry.getField(valueField), entryPath, findings);
                if (masked != null) {
                    builder = builder(message, builder);
                    builder.setRepeatedField(field, i,
                            mapEntry.toBuilder().setField(valueField, masked).build());
                }
            } else if (valueField.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
                Message nested = (Message) mapEntry.getField(valueField);
                Message walked = walkNested(nested, entryPath, pass);
                if (walked != nested) {
                    builder = builder(message, builder);
                    builder.setRepeatedField(field, i,
                            mapEntry.toBuilder().setField(valueField, walked).build());
                }
            }
        }
        return builder;
    }

    /**
     * Screens one string value. Returns the masked value when the policy
     * masked at least one span, null when the value is unchanged.
     */
    private String screenValue(String value, String path, List<Finding> findings) {
        List<ScreeningEngine.Detection> acted = new ArrayList<>();
        for (ScreeningEngine.Detection detection : engine.detect(value)) {
            if (detection.confidence() >= config.getThreshold()) {
                acted.add(detection);
            }
        }
        if (acted.isEmpty()) {
            return null;
        }
        for (ScreeningEngine.Detection detection : acted) {
            Finding finding = new Finding(path, detection.type(), detection.confidence(),
                    engine.modelVersion(), config.getThreshold(), config.getPolicy());
            if (config.getPolicy() == ScreeningPolicy.SCREENING_POLICY_REFUSE) {
                throw new ScreeningRefusedException(finding);
            }
            findings.add(finding);
        }
        if (config.getPolicy() != ScreeningPolicy.SCREENING_POLICY_MASK) {
            return null;
        }
        // Right to left, so earlier offsets stay valid; a token name finder
        // never produces overlapping spans within one value.
        acted.sort(Comparator.comparingInt(ScreeningEngine.Detection::begin).reversed());
        StringBuilder masked = new StringBuilder(value);
        for (ScreeningEngine.Detection detection : acted) {
            masked.replace(detection.begin(), detection.end(), MASKED);
        }
        return masked.toString();
    }

    private boolean screenedClass(FieldDescriptor field) {
        return DescriptorMetadata.field(field)
                .map(FieldMeta::getSensitivity)
                .filter(config.getSensitivityClass()::equals)
                .isPresent();
    }

    private static Message.Builder builder(Message message, Message.Builder existing) {
        return existing == null ? message.toBuilder() : existing;
    }
}
