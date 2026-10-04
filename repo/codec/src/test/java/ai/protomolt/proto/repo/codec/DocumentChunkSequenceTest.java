package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.*;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DocumentChunkSequenceTest {
    private static Document fragment(String... ids) {
        var parent = SearchMetadata.newBuilder();
        for (var id : ids) parent.addSemanticResults(SemanticProcessingResult.newBuilder().setResultId(id));
        return Document.newBuilder().setDocId("doc").setSearchMetadata(parent).build();
    }
    @Test void splitOutputWithSuffixAndGeneratedNameCollisionsIsAccepted() throws Exception {
        var full = fragment("a", "a", "b", "a#2", "a", "", "set-5", "a", "", "");
        var parts = DocumentPartCodec.split(full, PartLayouts.document()).stream()
                .filter(part -> part.part() == DocumentPart.DOCUMENT_PART_CHUNKS).toList();
        assertThat(parts).extracting(PartObject::subKey)
                .containsExactly("a", "b", "a#2", "a#3", "set-5", "a#4", "set-8", "set-9");
        var sequence = new DocumentChunkSequence("doc", 10);
        for (var part : parts) sequence.accept(part.subKey(), Document.parseFrom(part.bytes()));
        assertThat(DocumentPartCodec.assemble(DocumentPartCodec.split(full, PartLayouts.document()).stream()
                .map(PartObject::bytes).toList(), Document.getDefaultInstance())).isEqualTo(full);
    }
    @Test void rejectsSplitRunsMixedRunsAndWrongNames() {
        var sequence = new DocumentChunkSequence("doc", 10);
        sequence.accept("a", fragment("a"));
        assertThatThrownBy(() -> sequence.accept("a#2", fragment("a"))).hasMessageContaining("Adjacent");
        assertThatThrownBy(() -> sequence.accept("b", fragment("b"))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> new DocumentChunkSequence("doc", 10).accept("a", fragment("a", "b")))
                .hasMessageContaining("distinct");
        assertThatThrownBy(() -> new DocumentChunkSequence("doc", 10).accept("wrong", fragment("a")))
                .hasMessageContaining("subkey");
    }
    @Test void blankNamesUseGlobalElementPositionsAndBoundsAreAggregate() {
        var sequence = new DocumentChunkSequence("doc", 3);
        sequence.accept("a", fragment("a", "a"));
        sequence.accept("set-2", fragment(""));
        assertThatThrownBy(() -> sequence.accept("b", fragment("b"))).hasMessageContaining("bound");
        var wrong = new DocumentChunkSequence("doc", 3);
        wrong.accept("a", fragment("a", "a"));
        assertThatThrownBy(() -> wrong.accept("set-0", fragment(""))).hasMessageContaining("subkey");
    }
    @Test void sequenceAlsoRejectsUnownedContent() {
        var bad = fragment("a").toBuilder().setOwnership(OwnershipContext.newBuilder().setAccountId("other")).build();
        assertThatThrownBy(() -> new DocumentChunkSequence("doc", 1).accept("a", bad)).hasMessageContaining("unowned");
    }
    @Test void nameAllocatorMatchesLegacyFirstUnusedRuleAcrossCollisions() {
        // Independent compatibility oracle: the old list-based allocator, before extraction.
        var seen = new ArrayList<String>();
        var names = new ChunkRunNames();
        var random = new java.util.Random(173);
        for (int i = 0; i < 5000; i++) {
            String raw = "run-" + random.nextInt(20) + (i % 3 == 0 ? "#" + (2 + random.nextInt(20)) : "");
            String expected = raw;
            int suffix = 2;
            while (seen.contains(expected)) expected = raw + "#" + suffix++;
            seen.add(expected);
            assertThat(names.claim(raw)).isEqualTo(expected);
        }
    }
}
