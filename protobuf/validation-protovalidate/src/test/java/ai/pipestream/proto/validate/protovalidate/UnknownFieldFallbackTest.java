package ai.pipestream.proto.validate.protovalidate;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import ai.pipestream.proto.validate.protovalidate.testdata.AnnotatedUser;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rules must survive descriptors that were linked WITHOUT the buf.validate extension registry:
 * a {@link FileDescriptorSet} parsed with plain {@code parseFrom} keeps every
 * {@code (buf.validate.*)} option only as unknown fields, and the rule source has to fall back
 * to reparsing the option bytes instead of silently dropping the rules.
 */
class UnknownFieldFallbackTest {

    @Test
    void rulesSurviveDescriptorSetParsedWithoutExtensionRegistry() throws Exception {
        Descriptor relinked = relinkWithoutRegistry(AnnotatedUser.getDescriptor());

        // The reparsed options must be visible to the rule source itself...
        ProtovalidateRuleSource source = new ProtovalidateRuleSource();
        assertThat(source.fieldConstraints(relinked.findFieldByName("name")))
                .as("field rules must be recovered from unknown fields")
                .hasValueSatisfying(c -> assertThat(c.required()).isTrue());
        assertThat(source.messageConstraints(relinked))
                .as("message rules must be recovered from unknown fields")
                .hasValueSatisfying(c -> assertThat(c.cel()).isNotEmpty());

        // ...and enforced end to end by the validator.
        ProtoValidator validator = ProtoValidator.forMessageType(relinked, List.of(source));
        DynamicMessage shortName = DynamicMessage.newBuilder(relinked)
                .setField(relinked.findFieldByName("name"), "ab")
                .build();
        ValidationResult result = validator.validate(shortName);
        assertThat(result.violations())
                .anyMatch(v -> v.path().equals("name") && v.ruleId().equals("string.min_len"));

        DynamicMessage missingName = DynamicMessage.newBuilder(relinked).build();
        assertThat(validator.validate(missingName).violations())
                .anyMatch(v -> v.path().equals("name") && v.ruleId().equals("required"));
    }

    /**
     * Serializes the file (and its transitive imports) into a {@link FileDescriptorSet}, parses it
     * back WITHOUT any extension registry, and relinks. All custom options become unknown fields.
     */
    private static Descriptor relinkWithoutRegistry(Descriptor descriptor) throws Exception {
        List<FileDescriptor> files = new ArrayList<>();
        collectFiles(descriptor.getFile(), files);
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder();
        for (FileDescriptor file : files) {
            set.addFile(file.toProto());
        }
        FileDescriptorSet reparsed = FileDescriptorSet.parseFrom(set.build().toByteArray());

        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        for (FileDescriptorProto proto : reparsed.getFileList()) {
            byName.put(proto.getName(), proto);
        }
        Map<String, FileDescriptor> built = new HashMap<>();
        FileDescriptor target = null;
        for (FileDescriptorProto proto : reparsed.getFileList()) {
            target = link(proto, byName, built);
        }
        return target.findMessageTypeByName(descriptor.getName());
    }

    private static void collectFiles(FileDescriptor file, List<FileDescriptor> out) {
        for (FileDescriptor dep : file.getDependencies()) {
            collectFiles(dep, out);
        }
        if (out.stream().noneMatch(f -> f.getName().equals(file.getName()))) {
            out.add(file);
        }
    }

    private static FileDescriptor link(
            FileDescriptorProto proto,
            Map<String, FileDescriptorProto> byName,
            Map<String, FileDescriptor> built) throws Exception {
        FileDescriptor done = built.get(proto.getName());
        if (done != null) {
            return done;
        }
        List<FileDescriptor> deps = new ArrayList<>();
        for (String dep : proto.getDependencyList()) {
            deps.add(link(byName.get(dep), byName, built));
        }
        FileDescriptor file = FileDescriptor.buildFrom(proto, deps.toArray(new FileDescriptor[0]));
        built.put(proto.getName(), file);
        return file;
    }
}
