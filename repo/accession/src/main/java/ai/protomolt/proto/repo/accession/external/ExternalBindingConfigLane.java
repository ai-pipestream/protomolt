package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.registry.ConfigDocumentStore;
import ai.protomolt.proto.registry.ConfigSupport;
import ai.protomolt.proto.registry.InvalidConfigException;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import java.util.stream.Collectors;

/**
 * The optional config-lane copy of binding revisions. The canonical record is the binding
 * document in {@code DocumentService}; this lane holds a gated JSON envelope of each committed
 * revision because the config lane's contract is JSON. With no store configured, gating and
 * mirroring do nothing.
 */
final class ExternalBindingConfigLane {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private final ConfigDocumentStore configStore;

    /** @param configStore the config-lane store, or {@code null} when the host has none */
    ExternalBindingConfigLane(ConfigDocumentStore configStore) {
        this.configStore = configStore;
    }

    boolean configured() {
        return configStore != null;
    }

    /**
     * Checks, before the canonical save, that the config lane will take this binding, so a
     * binding the lane would refuse is never committed. Writes nothing.
     *
     * <p>The check is made on the protobuf itself: the binding's bytes are read with the type
     * the lane serves, and that message is validated against the lane's own rules. Nothing is
     * turned into JSON to be read back.
     */
    void gate(ExternalSystemBinding binding) {
        if (configStore == null) {
            return;
        }
        String typeName = ExternalSystemBinding.getDescriptor().getFullName();
        Descriptor laneType;
        try {
            laneType = ConfigSupport.resolveType(configStore, typeName)
                    .orElseThrow(() -> Status.INVALID_ARGUMENT
                            .withDescription("Config-lane gate rejected binding: type '" + typeName
                                    + "' is not served by the config lane; register its schema first")
                            .asRuntimeException());
        } catch (RegistryStoreException storeFailure) {
            throw Status.UNAVAILABLE
                    .withDescription("ConfigDocumentStore unavailable: " + storeFailure.getMessage())
                    .withCause(storeFailure)
                    .asRuntimeException();
        }
        requireAdmissible(binding, laneType);
    }

    /**
     * Refuses a binding the lane's schema cannot hold in full or whose rules it breaks. A field
     * the lane's type does not declare would be dropped from the mirrored document, so it is
     * refused instead of silently lost.
     */
    static void requireAdmissible(ExternalSystemBinding binding, Descriptor laneType) {
        DynamicMessage asLaneType;
        try {
            asLaneType = DynamicMessage.parseFrom(laneType, binding.toByteString());
        } catch (InvalidProtocolBufferException unreadable) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Config-lane gate rejected binding: it is not a valid "
                            + laneType.getFullName() + " in the config lane's schema: "
                            + unreadable.getMessage())
                    .withCause(unreadable)
                    .asRuntimeException();
        }
        if (!asLaneType.getUnknownFields().asMap().isEmpty()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Config-lane gate rejected binding: the config lane's schema for "
                            + laneType.getFullName() + " does not declare field number(s) "
                            + asLaneType.getUnknownFields().asMap().keySet()
                            + "; register the current schema first")
                    .asRuntimeException();
        }
        ValidationResult validated = VALIDATOR.validate(asLaneType);
        if (!validated.valid()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Config-lane gate rejected binding: "
                            + validated.violations().stream()
                                    .map(violation -> violation.path() + ": " + violation.message())
                                    .collect(Collectors.joining("; ")))
                    .asRuntimeException();
        }
    }

    /**
     * Mirrors one committed binding revision to its own immutable config-lane document,
     * named by {@link ExternalPublicationCoordinates#configRevisionDocumentName}. Runs only
     * after the canonical save succeeded, so a refused or lost save never leaves a mirror,
     * and two revisions never overwrite each other. A mirror failure leaves the canonical
     * revision in place and is reported as {@code UNAVAILABLE}; {@link ExternalBindingStore#mirrorRevision}
     * repairs it from the canonical revision.
     */
    void mirror(ExternalSystemBinding committed) {
        if (configStore == null) {
            return;
        }
        String configName = ExternalPublicationCoordinates.configRevisionDocumentName(
                committed.getAccountId(), committed.getBindingId(), committed.getBindingRevision());
        try {
            String envelope = toConfigEnvelopeJson(committed);
            ConfigSupport.gate(configStore, envelope);
            configStore.putConfig(configName, envelope);
        } catch (InvalidConfigException | RegistryStoreException mirrorFailure) {
            throw Status.UNAVAILABLE
                    .withDescription("ExternalSystemBinding '" + committed.getBindingId()
                            + "' revision " + committed.getBindingRevision()
                            + " is saved, but its config-lane mirror failed; call mirrorRevision"
                            + " to repair it: " + mirrorFailure.getMessage())
                    .withCause(mirrorFailure)
                    .asRuntimeException();
        }
    }

    /**
     * The config lane stores JSON documents, so the mirror is written as one. This is output
     * only: the canonical binding is the protobuf document, and nothing reads the mirror back.
     */
    private static String toConfigEnvelopeJson(ExternalSystemBinding binding) {
        try {
            String configJson = JsonFormat.printer()
                    .omittingInsignificantWhitespace()
                    .print(binding);
            return "{\"" + ConfigSupport.MESSAGE_TYPE + "\":\""
                    + ExternalSystemBinding.getDescriptor().getFullName()
                    + "\",\"" + ConfigSupport.CONFIG + "\":" + configJson + "}";
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException("Failed to serialize ExternalSystemBinding", impossible);
        }
    }
}
