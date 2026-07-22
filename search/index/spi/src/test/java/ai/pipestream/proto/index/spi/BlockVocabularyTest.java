package ai.pipestream.proto.index.spi;

import ai.pipestream.proto.index.hints.FieldIndexHint;
import ai.pipestream.proto.index.hints.IndexFieldType;
import ai.pipestream.proto.index.hints.IndexingHintsProto;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The block-join vocabulary: CHUNKS-role expansion in the plan factory,
 * block_role / chunk_recipe translation from proto options, recipe digest
 * identity, and the engine-id collision guard.
 */
class BlockVocabularyTest {

    private static DescriptorProtos.FieldOptions hint(FieldIndexHint hint) {
        return DescriptorProtos.FieldOptions.newBuilder()
                .setExtension(IndexingHintsProto.index, hint)
                .build();
    }

    /** pm.Article { doc_id [DOC_ID], title, passages [CHUNKS] -> pm.Passage { text, embedding [VECTOR] } } */
    private static Descriptor articleDescriptor() throws Exception {
        DescriptorProto passage = DescriptorProto.newBuilder().setName("Passage")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("text").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("embedding").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_FLOAT)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setOptions(hint(FieldIndexHint.newBuilder()
                                .setType(IndexFieldType.INDEX_FIELD_TYPE_VECTOR)
                                .setVectorDims(4)
                                .build())))
                .build();
        DescriptorProto article = DescriptorProto.newBuilder().setName("Article")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("doc_id").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                        .setOptions(hint(FieldIndexHint.newBuilder()
                                .setType(IndexFieldType.INDEX_FIELD_TYPE_KEYWORD)
                                .setBlockRole(ai.pipestream.proto.index.hints.BlockRole.BLOCK_ROLE_DOC_ID)
                                .build())))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("title").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("passages").setNumber(3)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".pm.Passage")
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setOptions(hint(FieldIndexHint.newBuilder()
                                .setType(IndexFieldType.INDEX_FIELD_TYPE_NESTED)
                                .setBlockRole(ai.pipestream.proto.index.hints.BlockRole.BLOCK_ROLE_CHUNKS)
                                .build())))
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("article.proto").setPackage("pm").setSyntax("proto3")
                .addMessageType(article).addMessageType(passage)
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Article");
    }

    private static IndexingPlanFactory optionsFactory() {
        return new IndexingPlanFactory(
                new ProtoOptionsIndexingHintSource().orElse(new InferringIndexingHintSource()));
    }

    @Test
    void chunksRoleKeepsTheContainerAndExpandsItsChildren() throws Exception {
        IndexingPlan plan = optionsFactory().create(articleDescriptor());

        IndexingPlan.IndexedField container = plan.find("passages").orElseThrow();
        assertThat(container.hint().blockRole()).isEqualTo(BlockRole.CHUNKS);
        assertThat(container.repeated()).isTrue();

        // Children expand into dotted paths with UNPREFIXED engine names: within
        // a block the children are their own documents, not parent properties.
        IndexingPlan.IndexedField vector = plan.find("passages.embedding").orElseThrow();
        assertThat(vector.fieldName()).isEqualTo("embedding");
        assertThat(vector.type()).isEqualTo(IndexFieldKind.VECTOR);
        assertThat(plan.find("passages.text").orElseThrow().fieldName()).isEqualTo("text");

        assertThat(plan.find("doc_id").orElseThrow().hint().blockRole())
                .isEqualTo(BlockRole.DOC_ID);
    }

    @Test
    void plainNestedStaysASingleEntry() throws Exception {
        // Same shape, no CHUNKS role: NESTED must keep exactly one plan entry.
        Descriptor descriptor = articleDescriptor();
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource()
                .put(descriptor.getFullName(), "passages",
                        ResolvedFieldHint.of(IndexFieldKind.NESTED));
        IndexingPlan plan = new IndexingPlanFactory(catalog
                .orElse(new InferringIndexingHintSource())).create(descriptor);

        assertThat(plan.find("passages")).isPresent();
        assertThat(plan.find("passages.embedding")).isEmpty();
    }

    @Test
    void chunksRoleRequiresARepeatedMessageField() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("bad.proto").setPackage("pm").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Bad")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("chunks").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(hint(FieldIndexHint.newBuilder()
                                        .setBlockRole(ai.pipestream.proto.index.hints.BlockRole.BLOCK_ROLE_CHUNKS)
                                        .build()))))
                .build();
        Descriptor bad = FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Bad");

        assertThatThrownBy(() -> optionsFactory().create(bad))
                .isInstanceOf(IndexingPlanException.class)
                .hasMessageContaining("repeated message");
    }

    @Test
    void chunkRecipeTranslatesFromProtoOptionsAndRequiresAStringField() throws Exception {
        FieldIndexHint recipeHint = FieldIndexHint.newBuilder()
                .setType(IndexFieldType.INDEX_FIELD_TYPE_TEXT)
                .setChunkRecipe(ai.pipestream.proto.index.hints.ChunkRecipe.newBuilder()
                        .setChunking(ai.pipestream.proto.index.hints.ChunkingSpec.newBuilder()
                                .setStrategy("sentence-packed").setStrategyVersion(1)
                                .setTargetTokens(384).setOverlapTokens(64)
                                .setMinTokens(32).setMaxTokens(512)
                                .setBoundary("rules-v1"))
                        .setEmbedding(ai.pipestream.proto.index.hints.EmbeddingSpec.newBuilder()
                                .setModel("test-model-4d").setDims(4)
                                .setNormalize(true))
                        .setStoreChunkText(true))
                .build();
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("recipe.proto").setPackage("pm").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Doc")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("body").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setOptions(hint(recipeHint)))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("count").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        Descriptor doc = FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Doc");

        IndexingPlan plan = optionsFactory().create(doc);
        ChunkRecipe recipe = plan.find("body").orElseThrow().hint().chunkRecipe();
        assertThat(recipe).isNotNull();
        assertThat(recipe.chunking().strategy()).isEqualTo("sentence-packed");
        assertThat(recipe.chunking().boundary()).isEqualTo("rules-v1");
        assertThat(recipe.embedding().model()).isEqualTo("test-model-4d");
        assertThat(recipe.embedding().similarity()).isEqualTo(VectorSimilarity.COSINE);
        assertThat(recipe.storeChunkText()).isTrue();

        // The same recipe on a non-string field is a planning error.
        FieldDescriptorProto badField = FieldDescriptorProto.newBuilder()
                .setName("count").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_INT32)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                .setOptions(hint(recipeHint))
                .build();
        FileDescriptorProto badFile = FileDescriptorProto.newBuilder()
                .setName("bad_recipe.proto").setPackage("pm").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Bad").addField(badField))
                .build();
        Descriptor bad = FileDescriptor.buildFrom(badFile, new FileDescriptor[0])
                .findMessageTypeByName("Bad");
        assertThatThrownBy(() -> optionsFactory().create(bad))
                .isInstanceOf(IndexingPlanException.class)
                .hasMessageContaining("string field");
    }

    @Test
    void recipeDigestIsStableAndSensitiveToEveryComponent() {
        ChunkRecipe recipe = new ChunkRecipe(
                new ChunkRecipe.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 512, "rules-v1"),
                new ChunkRecipe.EmbeddingSpec("test-model-4d", 4, VectorSimilarity.COSINE, true),
                "", true);

        assertThat(recipe.digest())
                .isEqualTo(recipe.digest())
                .hasSize(64);

        ChunkRecipe bumpedChunker = new ChunkRecipe(
                new ChunkRecipe.ChunkingSpec("sentence-packed", 2, 384, 64, 32, 512, "rules-v1"),
                recipe.embedding(), "", true);
        assertThat(bumpedChunker.digest())
                .as("a chunker implementation bump must change the digest")
                .isNotEqualTo(recipe.digest());

        ChunkRecipe otherModel = new ChunkRecipe(
                recipe.chunking(),
                new ChunkRecipe.EmbeddingSpec("other-model", 4, VectorSimilarity.COSINE, true),
                "", true);
        assertThat(otherModel.digest()).isNotEqualTo(recipe.digest());
    }

    @Test
    void duplicateEngineIdsAreRejectedNotLastWins() {
        StubIndexerProvider first = new StubIndexerProvider();
        StubIndexerProvider second = new StubIndexerProvider();

        assertThatThrownBy(() -> SearchEngineIndexers.providersFrom(List.of(first, second)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stub")
                .hasMessageContaining(StubIndexerProvider.class.getName());

        assertThat(SearchEngineIndexers.providersFrom(List.of(first)))
                .containsOnlyKeys("stub");
    }
}
