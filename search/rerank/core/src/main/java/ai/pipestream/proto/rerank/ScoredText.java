package ai.pipestream.proto.rerank;

/**
 * One candidate of a {@link RerankProvider#rank(String, java.util.List, int)} result: the
 * index the text held in the scored input list, the text itself, and the score the provider
 * gave it.
 *
 * @param index position of the text in the scored input list
 * @param text the candidate text
 * @param score relevance score, higher meaning more relevant to the query
 */
public record ScoredText(int index, String text, double score) {
}
