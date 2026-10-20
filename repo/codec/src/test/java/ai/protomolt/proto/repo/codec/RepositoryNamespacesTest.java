package ai.protomolt.proto.repo.codec;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Admission and cleanup both decide from these predicates whether an object key belongs to a
 * reserved namespace. A key counts only when one whole slash-delimited segment equals the
 * reserved word, at any depth, and keys are never normalized before the check.
 */
class RepositoryNamespacesTest {

    @Test
    void underJoinsAPrefixWithExactlyOneSlash() {
        assertThat(RepositoryNamespaces.under("tenant", "documents/a")).isEqualTo("tenant/documents/a");
        assertThat(RepositoryNamespaces.under("tenant/", "documents/a")).isEqualTo("tenant/documents/a");
        assertThat(RepositoryNamespaces.under("a/b/", "x")).isEqualTo("a/b/x");
    }

    @Test
    void underTreatsNullBlankAndBareSlashPrefixesAsNoPrefix() {
        assertThat(RepositoryNamespaces.under(null, "documents/a")).isEqualTo("documents/a");
        assertThat(RepositoryNamespaces.under("", "documents/a")).isEqualTo("documents/a");
        assertThat(RepositoryNamespaces.under("  ", "documents/a")).isEqualTo("documents/a");
        assertThat(RepositoryNamespaces.under("/", "documents/a")).isEqualTo("documents/a");
    }

    @Test
    void underStripsOnlyOneTrailingSlashAndKeepsInteriorWhitespace() {
        assertThat(RepositoryNamespaces.under("tenant//", "x")).isEqualTo("tenant//x");
        assertThat(RepositoryNamespaces.under(" tenant ", "x")).isEqualTo(" tenant /x");
    }

    @Test
    void eachPredicateMatchesItsReservedSegmentAtAnyDepth() {
        assertThat(RepositoryNamespaces.isDocumentPart("documents")).isTrue();
        assertThat(RepositoryNamespaces.isDocumentPart("documents/acct/doc/part")).isTrue();
        assertThat(RepositoryNamespaces.isDocumentPart("drive/prefix/documents/acct")).isTrue();
        assertThat(RepositoryNamespaces.isDocumentPart("drive/documents")).isTrue();
        assertThat(RepositoryNamespaces.isArchive("tenant/archive/acct/docs/e1")).isTrue();
        assertThat(RepositoryNamespaces.isManagedRaw("nas/.protomolt-managed/raw/blob")).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"documentsx/a", "xdocuments/a", "my-documents", "tenant/documents.bak/a",
            "Documents/a", "document/a", "docs/documen", ""})
    void aSegmentThatOnlyContainsTheWordIsNotTheNamespace(String key) {
        assertThat(RepositoryNamespaces.isDocumentPart(key)).isFalse();
    }

    @Test
    void emptySegmentsFromRepeatedOrEdgeSlashesNeitherMatchNorHideAMatch() {
        assertThat(RepositoryNamespaces.isArchive("//archive//")).isTrue();
        assertThat(RepositoryNamespaces.isArchive("/archive")).isTrue();
        assertThat(RepositoryNamespaces.isArchive("archive/")).isTrue();
        assertThat(RepositoryNamespaces.isArchive("/")).isFalse();
        assertThat(RepositoryNamespaces.isArchive("///")).isFalse();
    }

    @Test
    void keysAreNotNormalizedBeforeMatching() {
        assertThat(RepositoryNamespaces.isArchive("tenant/ archive/x")).isFalse();
        assertThat(RepositoryNamespaces.isArchive("tenant/archive /x")).isFalse();
        assertThat(RepositoryNamespaces.isManagedRaw("tenant/protomolt-managed/x")).isFalse();
        assertThat(RepositoryNamespaces.isManagedRaw("tenant\\.protomolt-managed\\x")).isFalse();
    }

    @Test
    void namespacesAreDistinctAndNullIsInNone() {
        assertThat(RepositoryNamespaces.isArchive("documents/a")).isFalse();
        assertThat(RepositoryNamespaces.isDocumentPart("archive/a")).isFalse();
        assertThat(RepositoryNamespaces.isManagedRaw("archive/documents")).isFalse();
        assertThat(RepositoryNamespaces.isManagedRaw(null)).isFalse();
        assertThat(RepositoryNamespaces.isArchive(null)).isFalse();
        assertThat(RepositoryNamespaces.isDocumentPart(null)).isFalse();
    }
}
