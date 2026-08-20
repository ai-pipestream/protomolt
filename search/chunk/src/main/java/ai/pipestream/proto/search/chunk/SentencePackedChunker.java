package ai.pipestream.proto.search.chunk;

import ai.pipestream.proto.search.index.spi.ChunkingPolicy;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@code sentence-packed} chunker, implementation version 1: executes a
 * {@link ChunkingPolicy.ChunkingSpec} deterministically. Sentences from the
 * boundary rule set the spec pins (see {@link BoundaryRules} for the carried
 * sets) pack greedily toward {@code targetTokens}; the next chunk re-includes
 * trailing sentences of the previous one up to {@code overlapTokens}; a
 * sentence larger than {@code maxTokens} splits at token boundaries; an
 * undersized trailing chunk merges into its predecessor unless the merge
 * would break the max, because the never-emit-above-max rule outranks the
 * never-emit-below-min rule.
 *
 * <p>Determinism is the whole point: the same text under the same spec
 * yields byte-identical chunks on every JDK and every release carrying
 * this strategy version. Any behavior change bumps
 * {@link #STRATEGY_VERSION}, which changes the policy digest and re-chunks
 * corpora explicitly rather than silently.
 */
public final class SentencePackedChunker {

    /** The strategy id this chunker executes. */
    public static final String STRATEGY = "sentence-packed";

    /** The pinned implementation version; bumps on ANY behavior change. */
    public static final int STRATEGY_VERSION = 1;

    /** The boundary rule set new policies pin by default. */
    public static final String BOUNDARY = SentenceRules.ID;

    /** Creates the chunker; it is stateless and thread-safe. */
    public SentencePackedChunker() {
    }

    /**
     * Chunks the text under the spec.
     *
     * @param text the source text; blank yields no chunks
     * @param spec the chunking configuration; its strategy, version, and
     *        boundary must name exactly this implementation, refused loudly
     *        otherwise so a policy never silently executes on the wrong
     *        chunker
     * @return the derived chunks, in order
     */
    public List<Chunk> chunk(String text, ChunkingPolicy.ChunkingSpec spec) {
        BoundaryRules rules = requireCompatible(spec);
        if (text == null || text.isBlank()) {
            return List.of();
        }
        List<SentenceRules.Sentence> sentences = split(text, spec.maxTokens(), rules);
        List<List<SentenceRules.Sentence>> packs = pack(sentences, spec);
        mergeUndersizedTail(packs, spec);
        List<Chunk> chunks = new ArrayList<>(packs.size());
        for (List<SentenceRules.Sentence> pack : packs) {
            int start = pack.getFirst().start();
            int end = pack.getLast().end();
            chunks.add(new Chunk(
                    chunks.size(),
                    text.substring(start, end),
                    start,
                    end,
                    rules.tokens(text, start, end).size()));
        }
        return List.copyOf(chunks);
    }

    private static BoundaryRules requireCompatible(ChunkingPolicy.ChunkingSpec spec) {
        if (spec == null) {
            throw new IllegalArgumentException("spec must not be null");
        }
        if (!STRATEGY.equals(spec.strategy())) {
            throw new IllegalArgumentException("this chunker executes strategy " + STRATEGY
                    + ", the policy names " + spec.strategy());
        }
        if (spec.strategyVersion() != STRATEGY_VERSION) {
            throw new IllegalArgumentException("this chunker is " + STRATEGY + " version "
                    + STRATEGY_VERSION + ", the policy pins version " + spec.strategyVersion());
        }
        if (spec.targetTokens() <= 0) {
            throw new IllegalArgumentException("targetTokens must be positive");
        }
        if (spec.overlapTokens() < 0 || spec.overlapTokens() >= spec.targetTokens()) {
            throw new IllegalArgumentException(
                    "overlapTokens must be non-negative and below targetTokens");
        }
        if (spec.maxTokens() != 0 && spec.maxTokens() < spec.targetTokens()) {
            throw new IllegalArgumentException(
                    "maxTokens must be 0 (unlimited) or at least targetTokens");
        }
        if (spec.minTokens() < 0) {
            throw new IllegalArgumentException("minTokens must not be negative");
        }
        return BoundaryRules.forId(spec.boundary());
    }

    /** Segments, splitting any sentence above the max at token boundaries. */
    private static List<SentenceRules.Sentence> split(
            String text, int maxTokens, BoundaryRules rules) {
        List<SentenceRules.Sentence> sentences = new ArrayList<>();
        for (SentenceRules.Sentence sentence : rules.segment(text)) {
            List<BoundaryRules.TokenSpan> tokens =
                    rules.tokens(text, sentence.start(), sentence.end());
            if (maxTokens == 0 || tokens.size() <= maxTokens) {
                sentences.add(sentence);
                continue;
            }
            for (int i = 0; i < tokens.size(); i += maxTokens) {
                int windowEnd = Math.min(i + maxTokens, tokens.size());
                sentences.add(new SentenceRules.Sentence(
                        tokens.get(i).start(), tokens.get(windowEnd - 1).end(), windowEnd - i));
            }
        }
        return sentences;
    }

    private static List<List<SentenceRules.Sentence>> pack(
            List<SentenceRules.Sentence> sentences, ChunkingPolicy.ChunkingSpec spec) {
        List<List<SentenceRules.Sentence>> packs = new ArrayList<>();
        List<SentenceRules.Sentence> current = new ArrayList<>();
        int currentTokens = 0;
        for (SentenceRules.Sentence sentence : sentences) {
            if (!current.isEmpty() && currentTokens + sentence.tokens() > spec.targetTokens()) {
                packs.add(current);
                List<SentenceRules.Sentence> next = new ArrayList<>();
                int nextTokens = 0;
                // Overlap: trailing sentences of the closed chunk, in their
                // original order, up to the budget, never the whole chunk.
                for (int i = current.size() - 1; i > 0; i--) {
                    SentenceRules.Sentence candidate = current.get(i);
                    if (nextTokens + candidate.tokens() > spec.overlapTokens()) {
                        break;
                    }
                    next.add(0, candidate);
                    nextTokens += candidate.tokens();
                }
                current = next;
                currentTokens = nextTokens;
            }
            current.add(sentence);
            currentTokens += sentence.tokens();
        }
        if (!current.isEmpty()) {
            packs.add(current);
        }
        return packs;
    }

    private static void mergeUndersizedTail(
            List<List<SentenceRules.Sentence>> packs, ChunkingPolicy.ChunkingSpec spec) {
        if (packs.size() < 2 || spec.minTokens() <= 0) {
            return;
        }
        List<SentenceRules.Sentence> tail = packs.getLast();
        int tailTokens = tail.stream().mapToInt(SentenceRules.Sentence::tokens).sum();
        if (tailTokens >= spec.minTokens()) {
            return;
        }
        List<SentenceRules.Sentence> previous = packs.get(packs.size() - 2);
        int previousTokens = previous.stream().mapToInt(SentenceRules.Sentence::tokens).sum();
        if (spec.maxTokens() != 0 && previousTokens + tailTokens > spec.maxTokens()) {
            return;
        }
        // The tail's fresh sentences append after the previous chunk; the
        // overlap prefix it shares with that chunk is dropped, not doubled.
        for (SentenceRules.Sentence sentence : tail) {
            if (!previous.contains(sentence)) {
                previous.add(sentence);
            }
        }
        packs.removeLast();
    }
}
