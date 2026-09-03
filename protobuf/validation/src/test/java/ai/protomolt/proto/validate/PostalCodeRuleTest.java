package ai.protomolt.proto.validate;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.validate.spi.PostalCodeCatalog;
import ai.protomolt.proto.validate.spi.TaxonomyCatalog;
import ai.protomolt.proto.validate.spi.ValidationRuleSources;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The postal-code grammar check over a mounted pack: a region the pack
 * carries enforces its masks on a non-empty postal code, with the pack
 * version as evidence; a region the pack does not carry stays unchecked
 * (the data-free default, deliberately not the taxonomy rule's fail-closed
 * stance, because nothing in the schema declares the binding). The
 * descriptor is built dynamically: the check binds by type name alone.
 */
class PostalCodeRuleTest {

    private static final Descriptors.Descriptor ADDRESS = buildAddress();

    private static final PostalCodeCatalog PACK = regionCode -> switch (regionCode) {
        case "US" -> Optional.of(new PostalCodeCatalog.Mounted(
                "US", "v3", List.of("NNNNN", "NNNNN-NNNN")));
        case "GB" -> Optional.of(new PostalCodeCatalog.Mounted(
                "GB", "v3", List.of("AN NAA", "AANA NAA")));
        default -> Optional.empty();
    };

    private static final ProtoValidator VALIDATOR = ProtoValidator.create(
            ValidationRuleSources.defaults(), TaxonomyCatalog.empty(), PACK);

    private static Descriptors.Descriptor buildAddress() {
        DescriptorProtos.FileDescriptorProto file =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("google/type/postal_address_pack_test.proto")
                        .setSyntax("proto3")
                        .setPackage("google.type")
                        .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                                .setName("PostalAddress")
                                .addField(field("revision", 1,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                                .addField(field("region_code", 2,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                                .addField(field("postal_code", 3,
                                        DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)))
                        .build();
        try {
            return Descriptors.FileDescriptor
                    .buildFrom(file, new Descriptors.FileDescriptor[0])
                    .findMessageTypeByName("PostalAddress");
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder field(
            String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static DynamicMessage address(String region, String code) {
        return DynamicMessage.newBuilder(ADDRESS)
                .setField(ADDRESS.findFieldByName("region_code"), region)
                .setField(ADDRESS.findFieldByName("postal_code"), code)
                .build();
    }

    @Test
    void aMountedRegionEnforcesItsMasks() {
        assertThat(VALIDATOR.validate(address("US", "94105")).valid()).isTrue();
        assertThat(VALIDATOR.validate(address("US", "94105-1234")).valid()).isTrue();
        assertThat(VALIDATOR.validate(address("GB", "N1 9AA")).valid()).isTrue();

        ValidationResult result = VALIDATOR.validate(address("US", "9410"));
        assertThat(result.violations()).singleElement().satisfies(violation -> {
            assertThat(violation.path()).isEqualTo("postal_code");
            assertThat(violation.ruleId()).isEqualTo("postal.code_grammar");
            assertThat(violation.message()).contains("US").contains("v3");
        });
    }

    @Test
    void anUnmountedRegionAndAnEmptyCodeStayUnchecked() {
        // No pack entry for DE: the data-free default, unchecked.
        assertThat(VALIDATOR.validate(address("DE", "not-a-postal-code")).valid()).isTrue();
        // A mounted region with no code: presence is the address's business.
        assertThat(VALIDATOR.validate(address("US", "")).valid()).isTrue();
        // The default validator mounts nothing: everything stays unchecked.
        assertThat(ProtoValidator.create().validate(address("US", "9410")).valid()).isTrue();
    }
}
