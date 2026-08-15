# Chunking

`protomolt-chunker` executes a chunking policy deterministically. The
policy lives on the shape: a `ChunkingPolicy` (`chunking_policy` in
`indexing_hints.proto`) attaches to a text field through the indexing
hints, is served from the registry with the shape, and its digest pins the
whole derivation the way a schema pin identifies a message shape. Two
corpora agree on chunk boundaries and vector spaces exactly when their
policy digests agree; changing a policy is a data change and never a
rebuild.

## The chunker

`SentencePackedChunker` implements the `sentence-packed` strategy,
implementation version 1, over the pinned boundary rule set `rules-v1`:

- Sentences end after `.?!` runs (with trailing closers) followed by
  whitespace; a blank line always ends a sentence. The rules are
  hand-rolled and frozen under the rule-set id, so a JDK upgrade can never
  silently move a boundary (`java.text.BreakIterator` is deliberately not
  used).
- A token is a maximal non-whitespace run.
- Sentences pack greedily toward `target_tokens`; the next chunk
  re-includes trailing sentences of the previous one up to
  `overlap_tokens`; a sentence above `max_tokens` splits at token
  boundaries; an undersized trailing chunk merges into its predecessor
  unless that would break the max (never-above-max outranks
  never-below-min).

A chunker executes exactly one (strategy, version, boundary) triple and
refuses every other by name, so a policy never silently runs on the wrong
implementation. Any behavior change bumps the strategy version, which
changes the policy digest and re-chunks corpora explicitly.

## Output

Each `Chunk` carries its ordinal (chunk identity downstream is
`<doc_id>#<generation>#<ordinal>`, where the generation is the policy
digest: a policy change is a data change, a new chunk set), the verbatim
source substring, source offsets (overlapping ranges when overlap is
configured), and its token count. Chunks feed the mappers' block scope
(`BLOCK_ROLE_CHUNKS`).

## The derivation

`PolicyDerivation` executes the whole policy: chunk under its chunking
spec, embed every chunk under its `EmbeddingSpec`. The provider is
validated against the spec by model name and dimension before any text is
touched, so a corpus can never be silently derived with the wrong model.
`PolicyDerivation.discover(policy)` resolves the provider from the policy
alone through the [embedding lane](embeddings.md)'s ServiceLoader seam;
model2vec is the product default.
