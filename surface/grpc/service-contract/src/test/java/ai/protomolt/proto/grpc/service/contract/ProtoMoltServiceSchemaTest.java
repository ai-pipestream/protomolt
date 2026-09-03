package ai.protomolt.proto.grpc.service.contract;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shape of the service contract itself, as opposed to the behaviour of any one verb.
 *
 * <p>These assertions guard properties that are easy to lose one verb at a time. A response
 * that reverts to an untyped structure still compiles, still serves, and still passes every
 * behavioural test, because the transcoder will carry any JSON at all. The only thing that
 * notices is a check on the contract.
 */
class ProtoMoltServiceSchemaTest {

    private static final String STRUCT = "google.protobuf.Struct";

    /**
     * No method may answer with a bare structure. A structure as the whole response means the
     * reply has no declared contract: nothing states which fields exist, no rule can be
     * attached to them, and a generated client gets an untyped bag.
     *
     * <p>Carrying a structure inside a named response message is a different matter and stays
     * allowed, because some payloads genuinely have no protobuf shape. A JSON Schema document
     * and an engine-specific index artifact are both defined elsewhere than in this contract.
     */
    @Test
    void noMethodAnswersWithAnUntypedStructure() {
        for (MethodDescriptor method : ProtoMoltServiceSchema.service().getMethods()) {
            assertThat(method.getOutputType().getFullName())
                    .as("response type of %s", method.getName())
                    .isNotEqualTo(STRUCT);
        }
    }

    /** Every method must also accept a declared request type, for the same reason. */
    @Test
    void noMethodAcceptsAnUntypedStructure() {
        for (MethodDescriptor method : ProtoMoltServiceSchema.service().getMethods()) {
            assertThat(method.getInputType().getFullName())
                    .as("request type of %s", method.getName())
                    .isNotEqualTo(STRUCT);
        }
    }

    /**
     * The service workspace replies carry the real profile contract rather than a rendering of
     * it. ServiceProfile is defined in its own module and imported here, so a caller reading
     * one of these replies gets the same type the workspace stores.
     */
    @Test
    void workspaceRepliesCarryTheDeclaredProfileType() {
        for (String response : new String[] {
                "ServiceRegisterResponse", "ServiceInspectResponse", "ServiceRefreshResponse"}) {
            Descriptor descriptor = ProtoMoltServiceSchema.file().findMessageTypeByName(response);
            assertThat(descriptor).as(response).isNotNull();
            FieldDescriptor profile = descriptor.findFieldByName("profile");
            assertThat(profile).as("%s.profile", response).isNotNull();
            assertThat(profile.getMessageType().getFullName())
                    .as("%s.profile type", response)
                    .isEqualTo("ai.protomolt.proto.grpc.profile.v1.ServiceProfile");
        }
    }

    /**
     * The definition imports first-party contracts, so its own text no longer compiles alone.
     * Anything handing these sources to a compiler needs the closure, and the accessor must
     * therefore report every file the definition names.
     */
    @Test
    void theSourceClosureCoversEveryFirstPartyImport() {
        Map<String, String> sources = ProtoMoltServiceSchema.protoSources();
        assertThat(sources).containsKey(ProtoMoltServiceSchema.RESOURCE_PATH);

        for (String line : ProtoMoltServiceSchema.protoSource().lines().toList()) {
            String trimmed = line.strip();
            if (!trimmed.startsWith("import ")) {
                continue;
            }
            String path = trimmed.substring(trimmed.indexOf('"') + 1, trimmed.lastIndexOf('"'));
            if (path.startsWith("google/protobuf/")) {
                // Well-known types are supplied by the compiler, not carried as source.
                continue;
            }
            assertThat(sources).as("closure covers %s", path).containsKey(path);
        }
    }

    /** The closure has to compile as a unit, which is the point of shipping it together. */
    @Test
    void theServiceCompilesFromItsOwnSourceClosure() {
        assertThat(ProtoMoltServiceSchema.service().getMethods()).isNotEmpty();
        assertThat(ProtoMoltServiceSchema.file().getDependencies())
                .extracting(file -> file.getName())
                .contains("ai/protomolt/proto/grpc/profile/v1/service_profile.proto");
    }
}
