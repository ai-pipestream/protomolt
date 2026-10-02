package ai.protomolt.proto.actions.schema;

import ai.protomolt.proto.actions.ActionContractProvider;
import ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.Optional;

/** Contracts for the protobuf toolkit action family. */
public final class ToolkitContracts implements ActionContractProvider {
    @Override
    public Optional<Descriptor> find(String messageName) {
        return Optional.ofNullable(ProtoMoltServiceSchema.file().findMessageTypeByName(messageName));
    }
}
