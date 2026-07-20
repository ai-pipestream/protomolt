package ai.pipestream.proto.index.spi;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Catalog keys come in two shapes — {@code Message.field} and bare {@code field} — and the
 * qualified one has to win, or a bare key meant for one message silently retunes every message
 * that happens to declare a field of the same name.
 */
class CatalogIndexingHintSourceTest {

    @Test
    void qualifiedKeyWinsOverABareNameForTheSameField() throws Exception {
        FieldDescriptor title = field("title");
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("title", ResolvedFieldHint.of(IndexFieldKind.TEXT))
                .put("ai.pipestream.test.Doc", "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));

        assertThat(catalog.resolve(title)).get()
                .extracting(ResolvedFieldHint::type)
                .isEqualTo(IndexFieldKind.KEYWORD);
    }

    @Test
    void bareNameAppliesWhenNoQualifiedKeyIsRegistered() throws Exception {
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("title", ResolvedFieldHint.of(IndexFieldKind.TEXT));

        assertThat(catalog.resolve(field("title"))).get()
                .extracting(ResolvedFieldHint::type)
                .isEqualTo(IndexFieldKind.TEXT);
    }

    /** A qualified key naming another message must not leak onto this one via the bare fallback. */
    @Test
    void qualifiedKeyForAnotherMessageDoesNotApply() throws Exception {
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("ai.pipestream.other.Doc", "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));

        assertThat(catalog.resolve(field("title"))).isEmpty();
    }

    @Test
    void unknownFieldResolvesEmpty() throws Exception {
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put("ai.pipestream.test.Doc", "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));

        assertThat(catalog.resolve(field("body"))).isEmpty();
    }

    @Test
    void twoArgumentPutComposesTheSameKeyAsTheDottedOne() throws Exception {
        CatalogIndexingHintSource composed = new CatalogIndexingHintSource()
                .put("ai.pipestream.test.Doc", "title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
        CatalogIndexingHintSource dotted = new CatalogIndexingHintSource()
                .put("ai.pipestream.test.Doc.title", ResolvedFieldHint.of(IndexFieldKind.KEYWORD));

        assertThat(composed.resolve(field("title"))).isEqualTo(dotted.resolve(field("title")));
        assertThat(dotted.resolve(field("title"))).get()
                .extracting(ResolvedFieldHint::type)
                .isEqualTo(IndexFieldKind.KEYWORD);
    }

    private static FieldDescriptor field(String name) throws Exception {
        return descriptor().findFieldByName(name);
    }

    private static Descriptor descriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("catalog_doc.proto")
                .setPackage("ai.pipestream.test")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("title")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("body")
                                .setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Doc");
    }
}
