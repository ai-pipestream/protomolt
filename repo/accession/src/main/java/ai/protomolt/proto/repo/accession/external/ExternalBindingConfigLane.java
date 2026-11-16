package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.registry.ConfigDocumentStore;
import ai.protomolt.proto.registry.ConfigSupport;
import ai.protomolt.proto.registry.InvalidConfigException;
import ai.protomolt.proto.registry.RegistryStoreException;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;

/**
 * The optional config-lane copy of binding revisions. The canonical record is the binding
 * document in {@code DocumentService}; this lane holds a gated JSON envelope of each committed
 * revision because the config lane's contract is JSON. With no store configured, gating and
 * mirroring do nothing.
 */
final class ExternalBindingConfigLane {

    private final ConfigDocumentStore configStore;

    /** @param configStore the config-lane store, or {@code null} when the host has none */
    ExternalBindingConfigLane(ConfigDocumentStore configStore) {
        this.configStore = configStore;
    }

    boolean configured() {
        return configStore != null;
    }

    /**
     * Checks the config-lane envelope before the canonical save, so a binding the config
     * lane would refuse is never committed. Writes nothing.
     */
    void gate(ExternalSystemBinding binding) {
        if (configStore == null) {
            return;
        }
        try {
            ConfigSupport.gate(configStore, toConfigEnvelopeJson(binding));
        } catch (InvalidConfigException invalid) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Config-lane gate rejected binding: " + invalid.getMessage())
                    .withCause(invalid)
                    .asRuntimeException();
        } catch (RegistryStoreException storeFailure) {
            throw Status.UNAVAILABLE
                    .withDescription("ConfigDocumentStore unavailable: " + storeFailure.getMessage())
                    .withCause(storeFailure)
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
