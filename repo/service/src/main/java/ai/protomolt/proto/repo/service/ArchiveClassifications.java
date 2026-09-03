package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.characterize.Characterizer;
import ai.protomolt.proto.asset.characterize.Classifications;
import ai.protomolt.proto.asset.v1.Attribution;
import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.time.Instant;
import java.util.stream.Collectors;

import static ai.protomolt.proto.repo.service.GrpcErrors.invalidArgument;

/**
 * The archive doors' side of the classification state machine: declared
 * facts and origins validate here against the contract's own rules (the
 * annotation is the claim's contract — {@link ProtoValidator} enforces it
 * literally), the declaration-names-its-file door rule is applied, and
 * resolution delegates to the asset family's one state machine.
 */
final class ArchiveClassifications {

    /** The stored state-column value of an entry nothing has classified. */
    static final String UNCLASSIFIED = "UNCLASSIFIED";

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();
    private static final String STATE_PREFIX = "CLASSIFICATION_STATE_";

    private ArchiveClassifications() {
    }

    /**
     * Validates a declared format at the door: the arm must be set, the
     * fact must satisfy its format's own rules, and — the door's rule — a
     * declaration must carry the format's filename, because the claim is
     * about a named file. Characterization's identified facts may omit the
     * name; a producer's declaration may not.
     *
     * @param present whether the request carried a declaration
     * @param declared the declared fact
     * @return the validated fact, or null when none was declared
     */
    static FormatFact declared(boolean present, FormatFact declared) {
        if (!present) {
            return null;
        }
        if (declared.getFormatCase() == FormatFact.FormatCase.FORMAT_NOT_SET) {
            throw invalidArgument("declared must name a format");
        }
        require(declared, "declared");
        Message arm = (Message) declared.getField(
                declared.getDescriptorForType().findFieldByNumber(
                        declared.getFormatCase().getNumber()));
        FieldDescriptor filename = arm.getDescriptorForType().findFieldByName("filename");
        if (filename != null && ((String) arm.getField(filename)).isBlank()) {
            throw invalidArgument("a declaration names its file: declared."
                    + declared.getFormatCase().name().toLowerCase() + ".filename is required");
        }
        return declared;
    }

    /**
     * Validates an object-store origin at the door — strict by the
     * no-assumed-defaults rule.
     *
     * @param present whether the request carried an origin
     * @param origin the origin
     * @return the validated origin, or null when none was given
     */
    static ObjectStoreOrigin origin(boolean present, ObjectStoreOrigin origin) {
        if (!present) {
            return null;
        }
        require(origin, "origin");
        return origin;
    }

    /**
     * Resolves an entry's classification: characterize the primary
     * rendition's bytes (when available) and run the state machine.
     *
     * @param declared the validated declaration, or null
     * @param origin the validated origin, or null
     * @param primaryPrefix the primary rendition's first bytes, or null
     *        when no bytes were available to read
     * @param filename the entry's filename, or null
     * @param writtenBy the write's attribution, or null
     * @return the resolved classification
     */
    static Classification classify(FormatFact declared, ObjectStoreOrigin origin,
                                   byte[] primaryPrefix, String filename,
                                   WriteAttribution writtenBy) {
        Characterizer.Identification identification = primaryPrefix != null
                ? Characterizer.identify(primaryPrefix, filename)
                : new Characterizer.Identification(null, java.util.List.of());
        return Classifications.resolve(declared, identification,
                attribution(writtenBy), origin, Instant.now());
    }

    /** The state-column value of a classification. */
    static String stateName(Classification classification) {
        return classification.getState().name().substring(STATE_PREFIX.length());
    }

    /** The proto state for a stored state-column value. */
    static ClassificationState stateOf(String columnValue) {
        return ClassificationState.valueOf(STATE_PREFIX + columnValue);
    }

    /** Serializes a classification for the ledger's JSONB column. */
    static String toJson(Classification classification) {
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace()
                    .print(classification);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("classification does not print as JSON", e);
        }
    }

    /** Parses a stored classification; null column = never classified. */
    static Classification fromJson(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            Classification.Builder builder = Classification.newBuilder();
            JsonFormat.parser().merge(json, builder);
            return builder.build();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("stored classification does not parse", e);
        }
    }

    private static Attribution attribution(WriteAttribution writtenBy) {
        if (writtenBy == null
                || (writtenBy.getModule().isBlank() && writtenBy.getActor().isBlank())) {
            return null;
        }
        return Attribution.newBuilder()
                .setModule(writtenBy.getModule())
                .setActor(writtenBy.getActor())
                .build();
    }

    /** Refuses with every violation named, exactly as the contract states them. */
    private static void require(Message message, String field) {
        ValidationResult result = VALIDATOR.validate(message);
        if (!result.valid()) {
            throw invalidArgument(field + " fails its contract: " + result.violations().stream()
                    .map(v -> (v.path().isBlank() ? v.ruleId() : v.path()) + " — " + v.message())
                    .collect(Collectors.joining("; ")));
        }
    }
}
