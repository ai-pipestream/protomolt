package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.Person;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rules must survive a descriptor whose options were parsed without the validate.v1 extensions
 * registered — the shape every dynamically loaded descriptor set has unless its loader knew to
 * register them. The annotations are still in the options, just as unknown fields; a validator
 * that cannot see them pronounces every message clean, which is the failure that reports
 * success.
 */
class UnregisteredExtensionsTest {

    @Test
    void enforcesRulesCarriedOnlyAsUnknownFields() throws Exception {
        Descriptor blind = relinkWithoutExtensions(Person.getDescriptor().getFile())
                .findMessageTypeByName("Person");
        // The options really did lose their typed extensions in the round trip.
        assertThat(blind.getFields().stream()
                .anyMatch(f -> !f.getOptions().getUnknownFields().asMap().isEmpty()))
                .as("relinked descriptor carries the rules as unknown fields")
                .isTrue();

        DynamicMessage tooShort = DynamicMessage.newBuilder(blind)
                .setField(blind.findFieldByName("name"), "A")
                .setField(blind.findFieldByName("age"), 10)
                .setField(blind.findFieldByName("email"), "a@b.co")
                .build();

        ValidationResult result = ProtoValidator.create().validate(tooShort);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations()).anyMatch(v -> v.ruleId().equals("string.min_len"));
    }

    /**
     * Round-trips {@code file} and its transitive imports through bytes with no extension
     * registry, so every custom option lands as an unknown field, then relinks the chain.
     */
    private static FileDescriptor relinkWithoutExtensions(FileDescriptor file) throws Exception {
        List<FileDescriptor> dependencies = new ArrayList<>();
        for (FileDescriptor dependency : file.getDependencies()) {
            dependencies.add(relinkWithoutExtensions(dependency));
        }
        FileDescriptorProto blind = FileDescriptorProto.parseFrom(file.toProto().toByteArray());
        return FileDescriptor.buildFrom(blind, dependencies.toArray(new FileDescriptor[0]));
    }
}
