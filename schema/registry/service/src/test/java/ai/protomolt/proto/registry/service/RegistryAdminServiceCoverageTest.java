package ai.protomolt.proto.registry.service;

import ai.protomolt.proto.schema.registry.v1.RegistryAdminProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every request message the registry admin verbs accept is one method's input on
 * {@code RegistryAdminService}.
 *
 * <p>The verbs and the service are two ways to reach the same service, so a request
 * message reachable through one and not the other is a contract that exists in half the
 * places it should. This catches a message added for a verb and never wired to a method.
 */
class RegistryAdminServiceCoverageTest {

    private static FileDescriptor file() {
        return RegistryAdminProto.getDescriptor();
    }

    @Test
    void everyRequestMessageIsAMethodInput() {
        ServiceDescriptor service = file().findServiceByName("RegistryAdminService");
        assertThat(service).as("RegistryAdminService").isNotNull();

        Set<String> methodInputs = service.getMethods().stream()
                .map(MethodDescriptor::getInputType)
                .map(Descriptor::getFullName)
                .collect(Collectors.toSet());

        List<String> unreachable = file().getMessageTypes().stream()
                .map(Descriptor::getFullName)
                .filter(name -> name.endsWith("Request"))
                .filter(name -> !methodInputs.contains(name))
                .toList();

        assertThat(unreachable)
                .as("request messages the verbs accept but no method takes")
                .isEmpty();
        assertThat(service.getMethods()).hasSize(3);
    }
}
