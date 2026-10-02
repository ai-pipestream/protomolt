package ai.protomolt.proto.actions;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.Optional;

/** Named legacy action contracts. Descriptor-native actions do not need this provider. */
public interface ActionContractProvider {
    /** Returns the contract with this name, or empty when the provider does not define it. */
    Optional<Descriptor> find(String messageName);
}
