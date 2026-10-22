package ai.protomolt.proto.repo.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentFilter;
import ai.protomolt.proto.repo.v2.DocumentLink;
import ai.protomolt.proto.repo.v2.DynamicTarget;
import ai.protomolt.proto.repo.v2.LinkField;
import ai.protomolt.proto.repo.v2.LinksProto;
import ai.protomolt.proto.repo.v2.StaticTarget;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The §17 link schema rules and extraction, over descriptors built the way a
 * retained schema is: from descriptor bytes, where the link option arrives as an
 * unknown field of FieldOptions.
 */
class DocumentLinksTest {
    private static final String LINK = ".ai.protomolt.proto.repo.v2.DocumentLink";
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder().setAccountId("a").setGraphId("g")
            .setGraphAddressId("ds").setDocId("d1").build();

    @Test void readsTheOptionFromRetainedBytesAndWalksNestedElements() throws Exception {
        var file = retained(DescriptorProto.newBuilder().setName("Section")
                        .addField(link("see", 1, false, LinkField.newBuilder().setRelation("references")))
                        .addField(link("parts", 2, true, LinkField.newBuilder().setRelation("hasPart")
                                .addTargetTypes("acme.v1.Section"))),
                DescriptorProto.newBuilder().setName("Report")
                        .addField(message("intro", 1, ".acme.v1.Section", false))
                        .addField(message("sections", 2, ".acme.v1.Section", true))
                        .addField(map("labels", 3, ".acme.v1.Report.LabelsEntry"))
                        .addField(link("cites", 4, false, LinkField.newBuilder().setRelation("isPartOf")))
                        .addNestedType(entry("LabelsEntry", ".acme.v1.Label")),
                DescriptorProto.newBuilder().setName("Label").addField(FieldDescriptorProto.newBuilder().setName("text")
                        .setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)));
        var report = file.findMessageTypeByName("Report");
        var section = file.findMessageTypeByName("Section");
        var see = section.findFieldByName("see");
        assertThat(see.getOptions().hasExtension(LinksProto.link)).isFalse();
        assertThat(DocumentLinks.linkField(see)).contains(LinkField.newBuilder().setRelation("references").build());
        assertThat(DocumentLinks.declaresLinks(report)).isTrue();

        var first = sectionValue(section, target("t1"), target("t2"), target("t3"));
        var second = sectionValue(section, target("t4"));
        var value = DynamicMessage.newBuilder(report)
                .setField(report.findFieldByName("intro"), first)
                .addRepeatedField(report.findFieldByName("sections"), second)
                .addRepeatedField(report.findFieldByName("sections"), second)
                .setField(report.findFieldByName("cites"), dynamicLink(report.findFieldByName("cites").getMessageType(),
                        DocumentLink.newBuilder().setDynamicTarget(DynamicTarget.newBuilder()
                                .setFilter(DocumentFilter.newBuilder().setCategory("x"))).build()))
                .build();

        var links = DocumentLinks.extract(value, "structured_data");
        assertThat(links).extracting(DocumentLinks.Link::fieldPath, DocumentLinks.Link::ordinal).containsExactly(
                org.assertj.core.groups.Tuple.tuple("structured_data.intro.see", 0),
                org.assertj.core.groups.Tuple.tuple("structured_data.intro.parts", 0),
                org.assertj.core.groups.Tuple.tuple("structured_data.intro.parts", 1),
                org.assertj.core.groups.Tuple.tuple("structured_data.sections[0].see", 0),
                org.assertj.core.groups.Tuple.tuple("structured_data.sections[1].see", 0),
                org.assertj.core.groups.Tuple.tuple("structured_data.cites", 0));
        assertThat(links.get(2).link().getStaticTarget().getAddress().getDocId()).isEqualTo("t3");
        assertThat(links.get(2).field().getTargetTypesList()).containsExactly("acme.v1.Section");
        assertThat(links.getLast().link().hasDynamicTarget()).isTrue();
        assertThat(DocumentLinks.extract(DynamicMessage.getDefaultInstance(report), "")).isEmpty();
    }

    @Test void aLinkFieldWithoutTheOptionIsRefused() throws Exception {
        var file = retained(DescriptorProto.newBuilder().setName("Report").addField(message("cites", 1, LINK, false)));
        assertThatThrownBy(() -> DocumentLinks.requireLinkSchema(file.findMessageTypeByName("Report")))
                .isInstanceOf(DocumentLinks.SchemaRefused.class).hasMessageContaining("acme.v1.Report.cites")
                .hasMessageContaining("needs the link option");
    }

    @Test void theOptionOnAnotherTypeIsRefused() throws Exception {
        var field = FieldDescriptorProto.newBuilder().setName("title").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setOptions(FieldOptions.newBuilder().setExtension(LinksProto.link,
                        LinkField.newBuilder().setRelation("references").build()));
        var file = retained(DescriptorProto.newBuilder().setName("Report").addField(field));
        assertThatThrownBy(() -> DocumentLinks.requireLinkSchema(file.findMessageTypeByName("Report")))
                .isInstanceOf(DocumentLinks.SchemaRefused.class).hasMessageContaining("needs a " + DocumentLinks.LINK_TYPE);
    }

    @Test void noLinkSitsUnderAMapValue() throws Exception {
        var file = retained(DescriptorProto.newBuilder().setName("Report").addField(map("cites", 1, ".acme.v1.Report.CitesEntry"))
                .addNestedType(entry("CitesEntry", LINK)));
        assertThatThrownBy(() -> DocumentLinks.requireLinkSchema(file.findMessageTypeByName("Report")))
                .isInstanceOf(DocumentLinks.SchemaRefused.class).hasMessageContaining("cannot sit under a map value");
        // Deeper: the map value's own message holds the link two levels down.
        var deep = retained(DescriptorProto.newBuilder().setName("Leaf")
                        .addField(link("see", 1, false, LinkField.newBuilder().setRelation("references"))),
                DescriptorProto.newBuilder().setName("Middle").addField(message("leaves", 1, ".acme.v1.Leaf", true)),
                DescriptorProto.newBuilder().setName("Report").addField(message("leaf", 1, ".acme.v1.Leaf", false))
                        .addField(map("appendix", 2, ".acme.v1.Report.AppendixEntry"))
                        .addNestedType(entry("AppendixEntry", ".acme.v1.Middle")));
        assertThatThrownBy(() -> DocumentLinks.requireLinkSchema(deep.findMessageTypeByName("Report")))
                .isInstanceOf(DocumentLinks.SchemaRefused.class).hasMessageContaining("acme.v1.Report.appendix")
                .hasMessageContaining("cannot sit under a map value");
        var annotated = map("cites", 1, ".acme.v1.Report.CitesEntry").setOptions(FieldOptions.newBuilder()
                .setExtension(LinksProto.link, LinkField.newBuilder().setRelation("references").build()));
        var other = retained(DescriptorProto.newBuilder().setName("Report").addField(annotated)
                .addNestedType(entry("CitesEntry", ".acme.v1.Report")));
        assertThatThrownBy(() -> DocumentLinks.requireLinkSchema(other.findMessageTypeByName("Report")))
                .isInstanceOf(DocumentLinks.SchemaRefused.class).hasMessageContaining("a map field cannot be a link");
    }

    @Test void anOptionThatFailsItsOwnRulesIsRefused() throws Exception {
        for (var bad : List.of(LinkField.getDefaultInstance(), LinkField.newBuilder().setRelation("part of").build(),
                LinkField.newBuilder().setRelation("references").addTargetTypes("not a type").build())) {
            var file = retained(DescriptorProto.newBuilder().setName("Report").addField(link("cites", 1, false, bad.toBuilder())));
            assertThatThrownBy(() -> DocumentLinks.requireLinkSchema(file.findMessageTypeByName("Report")))
                    .as(bad.toString()).isInstanceOf(DocumentLinks.SchemaRefused.class).hasMessageContaining("the link option fails");
        }
    }

    @Test void aMessageWithoutLinksDeclaresNone() throws Exception {
        var file = retained(DescriptorProto.newBuilder().setName("Plain")
                .addField(message("self", 1, ".acme.v1.Plain", false)));
        assertThat(DocumentLinks.declaresLinks(file.findMessageTypeByName("Plain"))).isFalse();
    }

    // ------------------------------------------------------------------ fixtures

    /** Builds the file, then rebuilds it from its bytes as a retained schema is. */
    private static FileDescriptor retained(DescriptorProto.Builder... messages) throws Exception {
        var proto = FileDescriptorProto.newBuilder().setName("acme/v1/report.proto").setPackage("acme.v1")
                .setSyntax("proto3").addDependency(LinksProto.getDescriptor().getName());
        for (var message : messages) proto.addMessageType(message);
        var bytes = FileDescriptor.buildFrom(proto.build(), new FileDescriptor[]{LinksProto.getDescriptor()})
                .toProto().toByteString();
        return FileDescriptor.buildFrom(FileDescriptorProto.parseFrom(bytes), new FileDescriptor[]{LinksProto.getDescriptor()});
    }

    private static FieldDescriptorProto.Builder link(String name, int number, boolean repeated, LinkField.Builder field) {
        return message(name, number, LINK, repeated).setOptions(FieldOptions.newBuilder().setExtension(LinksProto.link, field.build()));
    }

    private static FieldDescriptorProto.Builder message(String name, int number, String type, boolean repeated) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setTypeName(type)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setLabel(repeated
                        ? FieldDescriptorProto.Label.LABEL_REPEATED : FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static FieldDescriptorProto.Builder map(String name, int number, String entryType) {
        return message(name, number, entryType, true);
    }

    private static DescriptorProto.Builder entry(String name, String valueType) {
        return DescriptorProto.newBuilder().setName(name).setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(message("value", 2, valueType, false));
    }

    private static Message sectionValue(Descriptor section, DocumentLink see, DocumentLink... parts) {
        var value = DynamicMessage.newBuilder(section).setField(section.findFieldByName("see"),
                dynamicLink(section.findFieldByName("see").getMessageType(), see));
        for (var part : parts)
            value.addRepeatedField(section.findFieldByName("parts"), dynamicLink(section.findFieldByName("parts").getMessageType(), part));
        return value.build();
    }

    private static Message dynamicLink(Descriptor type, DocumentLink link) {
        try {
            return DynamicMessage.parseFrom(type, link.toByteString());
        } catch (com.google.protobuf.InvalidProtocolBufferException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static DocumentLink target(String doc) {
        return DocumentLink.newBuilder().setStaticTarget(StaticTarget.newBuilder()
                .setAddress(ADDRESS.toBuilder().setDocId(doc))).build();
    }
}
