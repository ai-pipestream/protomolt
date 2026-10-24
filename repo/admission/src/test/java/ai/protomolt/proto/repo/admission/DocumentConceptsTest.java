package ai.protomolt.proto.repo.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptsProto;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

/**
 * Concept tag extraction (requirements §17) over descriptors built the way a
 * retained schema is: from descriptor bytes, where the concept_schemes binding
 * arrives as an unknown field of FieldOptions.
 */
class DocumentConceptsTest {
    private static final String REF = ".ai.protomolt.proto.types.v1.ConceptRef";
    private static final FileDescriptor FILE = retained();
    private static final Descriptor REPORT = FILE.findMessageTypeByName("Report");
    private static final Descriptor SECTION = FILE.findMessageTypeByName("Section");

    @Test void readsTheBindingFromRetainedBytes() {
        var subject = REPORT.findFieldByName("subject");
        assertThat(subject.getOptions().hasExtension(ValidateProto.field)).isFalse();
        assertThat(DocumentConcepts.tagField(subject)).isTrue();
        assertThat(DocumentConcepts.tagField(REPORT.findFieldByName("mapped"))).isFalse();
        assertThat(DocumentConcepts.tagField(REPORT.findFieldByName("title"))).isFalse();
        assertThat(DocumentConcepts.declaresTags(REPORT)).isTrue();
        assertThat(DocumentConcepts.declaresTags(FILE.findMessageTypeByName("Plain"))).isFalse();
    }

    @Test void extractsBoundValuesAtTheirOccurrenceIdentity() {
        var first = DynamicMessage.newBuilder(SECTION)
                .addRepeatedField(SECTION.findFieldByName("tags"), ref("subject", "physics"))
                .addRepeatedField(SECTION.findFieldByName("tags"), ref("region", "emea")).build();
        var second = DynamicMessage.newBuilder(SECTION)
                .addRepeatedField(SECTION.findFieldByName("tags"), ref("subject", "chemistry")).build();
        var report = DynamicMessage.newBuilder(REPORT)
                .setField(REPORT.findFieldByName("title"), "t")
                .setField(REPORT.findFieldByName("subject"), ref("subject", "science"))
                .addRepeatedField(REPORT.findFieldByName("sections"), first)
                .addRepeatedField(REPORT.findFieldByName("sections"), second)
                // An unbound ConceptRef is not a tag, and an Any is not entered.
                .setField(REPORT.findFieldByName("mapped"), ref("subject", "unbound"))
                .setField(REPORT.findFieldByName("payload"), Any.pack(ref("subject", "packed")))
                .build();
        assertThat(DocumentConcepts.extract(report, "structured_data")).extracting(DocumentConcepts.Tag::fieldPath,
                DocumentConcepts.Tag::ordinal, DocumentConcepts.Tag::schemeId, DocumentConcepts.Tag::conceptId).containsExactly(
                tuple("structured_data.subject", 0, "subject", "science"),
                tuple("structured_data.sections[0].tags", 0, "subject", "physics"),
                tuple("structured_data.sections[0].tags", 1, "region", "emea"),
                tuple("structured_data.sections[1].tags", 0, "subject", "chemistry"));
        assertThat(DocumentConcepts.extract(DynamicMessage.getDefaultInstance(REPORT), "")).isEmpty();
    }

    @Test void aForgedConceptRefWithEmptyIdsIsRefused() {
        // Same fields as the canonical type but none of its rules, so nothing upstream catches empty ids.
        var report = forged(FieldDescriptorProto.Type.TYPE_STRING);
        var forgedRef = report.findFieldByName("subject").getMessageType();
        var value = DynamicMessage.newBuilder(report)
                .setField(report.findFieldByName("subject"), DynamicMessage.getDefaultInstance(forgedRef)).build();
        assertThat(DocumentConcepts.tagField(report.findFieldByName("subject"))).isTrue();
        assertThatThrownBy(() -> DocumentConcepts.extract(value, "structured_data"))
                .isInstanceOf(DocumentConcepts.TagRefused.class)
                .hasMessageContaining("forged.v1.Report.subject")
                .hasMessageContaining("required");
    }

    @Test void aForgedConceptRefWithAnInt64SchemeIdIsRefused() {
        var report = forged(FieldDescriptorProto.Type.TYPE_INT64);
        var forgedRef = report.findFieldByName("subject").getMessageType();
        var value = DynamicMessage.newBuilder(report).setField(report.findFieldByName("subject"),
                DynamicMessage.newBuilder(forgedRef).setField(forgedRef.findFieldByName("scheme_id"), 7L)
                        .setField(forgedRef.findFieldByName("concept_id"), "physics").build()).build();
        assertThatThrownBy(() -> DocumentConcepts.extract(value, "structured_data"))
                .isInstanceOf(DocumentConcepts.TagRefused.class)
                .hasMessageContaining("forged.v1.Report.subject")
                .hasMessageContaining("string scheme_id = 1");
    }

    @Test void aForgedConceptRefWithAnExtraFieldIsRefused() {
        var report = forged(FieldDescriptorProto.Type.TYPE_STRING, FieldDescriptorProto.newBuilder().setName("note")
                .setNumber(3).setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        var forgedRef = report.findFieldByName("subject").getMessageType();
        var value = DynamicMessage.newBuilder(report).setField(report.findFieldByName("subject"),
                DynamicMessage.newBuilder(forgedRef).setField(forgedRef.findFieldByName("scheme_id"), "subject")
                        .setField(forgedRef.findFieldByName("concept_id"), "physics").build()).build();
        assertThatThrownBy(() -> DocumentConcepts.extract(value, "structured_data"))
                .isInstanceOf(DocumentConcepts.TagRefused.class)
                .hasMessageContaining("no other fields");
    }

    @Test void unknownFieldsOnConceptRefAreRefused() throws Exception {
        var subject = REPORT.findFieldByName("subject");
        var withUnknown = DynamicMessage.newBuilder(subject.getMessageType())
                .mergeFrom(ref("subject", "physics").toByteString())
                .setUnknownFields(com.google.protobuf.UnknownFieldSet.newBuilder()
                        .addField(77, com.google.protobuf.UnknownFieldSet.Field.newBuilder().addVarint(1).build())
                        .build())
                .build();
        var report = DynamicMessage.newBuilder(REPORT).setField(subject, withUnknown).build();
        assertThatThrownBy(() -> DocumentConcepts.extract(report, "structured_data"))
                .isInstanceOf(DocumentConcepts.TagRefused.class)
                .hasMessageContaining("carries fields ai.protomolt.proto.types.v1.ConceptRef does not declare");
    }

    @Test void schemaChecksAndExtractionAreRememberedAndCancellable() throws Exception {
        int length = 2048;
        var bound = FieldOptions.newBuilder().setExtension(ValidateProto.field,
                FieldRules.newBuilder().addConceptSchemes("subject").build());
        var proto = FileDescriptorProto.newBuilder().setName("acme/v1/chain.proto").setPackage("acme.v1").setSyntax("proto3")
                .addDependency(ConceptsProto.getDescriptor().getName())
                .addDependency(ValidateProto.getDescriptor().getName());
        for (int i = 0; i < length - 1; i++) {
            proto.addMessageType(DescriptorProto.newBuilder().setName("C" + i)
                    .addField(message("next", 1, ".acme.v1.C" + (i + 1), false)));
        }
        proto.addMessageType(DescriptorProto.newBuilder().setName("C" + (length - 1))
                .addField(message("subject", 1, REF, false).setOptions(bound)));
        var deps = new FileDescriptor[]{ConceptsProto.getDescriptor(), ValidateProto.getDescriptor()};
        var bytes = FileDescriptor.buildFrom(proto.build(), deps).toProto().toByteString();
        var head = FileDescriptor.buildFrom(FileDescriptorProto.parseFrom(bytes), deps).findMessageTypeByName("C0");

        var runs = new int[1];
        var checks = new DocumentConcepts.SchemaChecks(() -> runs[0]++);
        assertThat(checks.declares(head)).isTrue();
        int firstRuns = runs[0];
        assertThat(firstRuns).isPositive().isLessThanOrEqualTo(3 * length + 8);
        assertThat(checks.declares(head)).isTrue();
        assertThat(runs[0]).isEqualTo(firstRuns);

        var stopped = new java.util.concurrent.CancellationException("deadline expired");
        var limited = new int[1];
        assertThatThrownBy(() -> new DocumentConcepts.SchemaChecks(() -> {
            if (++limited[0] > 50) throw stopped;
        }).declares(head)).isSameAs(stopped);

        var report = DynamicMessage.newBuilder(REPORT)
                .setField(REPORT.findFieldByName("subject"), ref("subject", "science"))
                .build();
        var extractRuns = new int[1];
        assertThat(DocumentConcepts.extract(report, "structured_data", () -> extractRuns[0]++)).hasSize(1);
        assertThat(extractRuns[0]).isPositive();
        assertThatThrownBy(() -> DocumentConcepts.extract(report, "structured_data", () -> {
            throw stopped;
        })).isSameAs(stopped);
    }

    /**
     * forged.v1.Report, whose bound subject field uses a ConceptRef from a forged
     * ai/protomolt/proto/types/v1/concepts.proto: no rules, scheme_id of {@code schemeType}.
     */
    private static Descriptor forged(FieldDescriptorProto.Type schemeType, FieldDescriptorProto.Builder... extra) {
        var ref = DescriptorProto.newBuilder().setName("ConceptRef")
                .addField(FieldDescriptorProto.newBuilder().setName("scheme_id").setNumber(1).setType(schemeType)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder().setName("concept_id").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        for (var field : extra) ref.addField(field);
        var concepts = FileDescriptorProto.newBuilder().setName(ConceptsProto.getDescriptor().getName())
                .setPackage("ai.protomolt.proto.types.v1").setSyntax("proto3").addMessageType(ref).build();
        var bound = FieldOptions.newBuilder().setExtension(ValidateProto.field,
                FieldRules.newBuilder().addConceptSchemes("subject").build());
        var report = FileDescriptorProto.newBuilder().setName("forged/v1/report.proto").setPackage("forged.v1").setSyntax("proto3")
                .addDependency(concepts.getName()).addDependency(ValidateProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Report")
                        .addField(message("subject", 1, REF, false).setOptions(bound)))
                .build();
        try {
            var forgedConcepts = FileDescriptor.buildFrom(concepts, new FileDescriptor[0]);
            return FileDescriptor.buildFrom(report, new FileDescriptor[]{forgedConcepts, ValidateProto.getDescriptor()})
                    .findMessageTypeByName("Report");
        } catch (Exception invalid) {
            throw new IllegalStateException(invalid);
        }
    }

    private static Message ref(String scheme, String concept) {
        return ConceptRef.newBuilder().setSchemeId(scheme).setConceptId(concept).build();
    }

    /** acme.v1: Report and Section bind ConceptRef fields; Report.mapped does not; Plain has none. */
    private static FileDescriptor retained() {
        var bound = FieldOptions.newBuilder().setExtension(ValidateProto.field,
                FieldRules.newBuilder().addConceptSchemes("subject").addConceptSchemes("region").build());
        var proto = FileDescriptorProto.newBuilder().setName("acme/v1/tagged.proto").setPackage("acme.v1").setSyntax("proto3")
                .addDependency(ConceptsProto.getDescriptor().getName()).addDependency(ValidateProto.getDescriptor().getName())
                .addDependency(AnyProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Section")
                        .addField(message("tags", 1, REF, true).setOptions(bound)))
                .addMessageType(DescriptorProto.newBuilder().setName("Report")
                        .addField(FieldDescriptorProto.newBuilder().setName("title").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(message("subject", 2, REF, false).setOptions(bound))
                        .addField(message("sections", 3, ".acme.v1.Section", true))
                        .addField(message("mapped", 4, REF, false))
                        .addField(message("payload", 5, ".google.protobuf.Any", false)))
                .addMessageType(DescriptorProto.newBuilder().setName("Plain").addField(message("mapped", 1, REF, false)))
                .build();
        var dependencies = new FileDescriptor[]{ConceptsProto.getDescriptor(), ValidateProto.getDescriptor(), AnyProto.getDescriptor()};
        try {
            var bytes = FileDescriptor.buildFrom(proto, dependencies).toProto().toByteString();
            return FileDescriptor.buildFrom(FileDescriptorProto.parseFrom(bytes), dependencies);
        } catch (Exception invalid) {
            throw new IllegalStateException(invalid);
        }
    }

    private static FieldDescriptorProto.Builder message(String name, int number, String type, boolean repeated) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setTypeName(type)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setLabel(repeated
                        ? FieldDescriptorProto.Label.LABEL_REPEATED : FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }
}
