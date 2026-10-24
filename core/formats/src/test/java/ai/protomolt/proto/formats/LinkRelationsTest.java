package ai.protomolt.proto.formats;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LinkRelationsTest {

    @Test
    void bundledTablesAreStrictlySortedSoBinarySearchFindsEveryName() {
        assertStrictlySorted(LinkRelations.DUBLIN_CORE);
        assertStrictlySorted(LinkRelations.IANA);
    }

    @Test
    void theIanaTableIsTheWholeRegistrySnapshot() {
        // link-relations-1.csv as updated 2026-06-12 lists 134 relation names.
        assertThat(LinkRelations.IANA).hasSize(134);
        assertThat(LinkRelations.IANA).contains("about", "canonical", "cite-as",
                "openid2.local_id", "p3pv1", "working-copy", "working-copy-of");
        assertThat(LinkRelations.DUBLIN_CORE).hasSize(15);
    }

    @Test
    void everyBundledNameIsALinkRelation() {
        for (String name : LinkRelations.DUBLIN_CORE) {
            assertThat(Formats.isLinkRelation(name)).as(name).isTrue();
            assertThat(LinkRelations.isDublinCore(name)).as(name).isTrue();
        }
        for (String name : LinkRelations.IANA) {
            assertThat(Formats.isLinkRelation(name)).as(name).isTrue();
            assertThat(LinkRelations.isRegistered(name)).as(name).isTrue();
        }
    }

    @Test
    void theVocabulariesAnswerOnlyForTheirOwnNames() {
        assertThat(LinkRelations.isDublinCore("isPartOf")).isTrue();
        assertThat(LinkRelations.isRegistered("isPartOf")).isFalse();
        assertThat(LinkRelations.isRegistered("canonical")).isTrue();
        assertThat(LinkRelations.isDublinCore("canonical")).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "https://example.com/rel/parent",   // RFC 8288 extension relation
            "http://purl.org/dc/terms/isPartOf", // a Dublin Core term in its URI form
            "HTTPS://example.com/rel/parent",   // scheme comparison is case-insensitive
            "http://h",                          // smallest non-empty authority
    })
    void httpUrisAreExtensionRelations(String value) {
        assertThat(Formats.isLinkRelation(value)).isTrue();
        assertThat(LinkRelations.isDublinCore(value)).isFalse();
        assertThat(LinkRelations.isRegistered(value)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "urn:example:rel:sibling",           // no locator, nothing to link to
            "tag:example.com,2026:rel/derived",
            "javascript:alert(1)",               // schemes with renderer side effects
            "data:text/html,x",
            "file:///etc/hosts",
            "vbscript:msgbox",
            "mailto:a@example.com",
            "http:rel/parent",                   // http without an authority
            "https:rel/parent",                  // https without an authority
            "http:///rel/parent",                // empty authority
            "http://@",                          // userinfo delimiter with empty host
            "http://@/x",                        // empty host after @
            "http://user@/x",                    // userinfo with empty host
            "http://:80/x",                      // port with empty host
            "http://user@:80/x",                 // userinfo and port with empty host
            "https://?q",                        // authority starts at the query
            "https://#f",
    })
    void nonHttpSchemesAndMissingAuthoritiesRefuse(String value) {
        assertThat(Formats.isLinkRelation(value)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            " ",
            "next ",              // registered name with trailing space
            " next",
            "Next",               // registered names match exactly as registered
            "CANONICAL",
            "Working-copy",       // not the registered spelling (working-copy)
            "IsPartOf",           // Dublin Core terms keep their camel case
            "ispartof",
            "is part of",
            "parent",             // unknown bare token, not a URI
            "rel/parent",         // relative path
            "/rel/parent",
            "../parent",
            "//example.com/rel",  // network-path reference, no scheme
            "#fragment",
            "https://example.com/rel with space",
    })
    void unknownTokensAndRelativeReferencesRefuse(String value) {
        assertThat(Formats.isLinkRelation(value)).isFalse();
    }

    @Test
    void dublinCoreInversesPairBothWays() {
        List<List<String>> pairs = List.of(
                List.of("isPartOf", "hasPart"),
                List.of("replaces", "isReplacedBy"),
                List.of("references", "isReferencedBy"),
                List.of("requires", "isRequiredBy"),
                List.of("hasFormat", "isFormatOf"),
                List.of("hasVersion", "isVersionOf"));
        for (List<String> pair : pairs) {
            assertThat(Formats.inverseLinkRelation(pair.get(0))).contains(pair.get(1));
            assertThat(Formats.inverseLinkRelation(pair.get(1))).contains(pair.get(0));
        }
    }

    @Test
    void everyInverseIsABundledDublinCoreTermAndAnInvolution() {
        int paired = 0;
        for (String name : LinkRelations.DUBLIN_CORE) {
            Optional<String> inverse = Formats.inverseLinkRelation(name);
            if (inverse.isPresent()) {
                paired++;
                assertThat(LinkRelations.isDublinCore(inverse.get())).as(name).isTrue();
                assertThat(Formats.inverseLinkRelation(inverse.get())).as(name).contains(name);
            }
        }
        assertThat(paired).isEqualTo(12);
    }

    @ParameterizedTest
    @ValueSource(strings = {"source", "relation", "conformsTo", "next", "related",
            "https://example.com/rel/parent", "ispartof", "",
            "http://purl.org/dc/terms/source", "https://purl.org/dc/terms/conformsTo"})
    void relationsWithoutAPairedInverseAnswerEmpty(String value) {
        assertThat(Formats.inverseLinkRelation(value)).isEmpty();
    }

    @Test
    void uriFormTermsAnswerTheirInverseInTheSamePrefixForm() {
        assertThat(Formats.inverseLinkRelation("http://purl.org/dc/terms/isPartOf"))
                .contains("http://purl.org/dc/terms/hasPart");
        assertThat(Formats.inverseLinkRelation("https://purl.org/dc/terms/hasPart"))
                .contains("https://purl.org/dc/terms/isPartOf");
        assertThat(Formats.inverseLinkRelation("http://purl.org/dc/terms/isVersionOf"))
                .contains("http://purl.org/dc/terms/hasVersion");
        // RFC 3986 scheme and host are case-insensitive; path and term stay case-sensitive.
        assertThat(Formats.inverseLinkRelation("HTTP://purl.org/dc/terms/isPartOf"))
                .contains("http://purl.org/dc/terms/hasPart");
        assertThat(Formats.inverseLinkRelation("https://PURL.ORG/dc/terms/isPartOf"))
                .contains("https://purl.org/dc/terms/hasPart");
        assertThat(Formats.inverseLinkRelation("Http://Purl.Org/dc/terms/hasPart"))
                .contains("http://purl.org/dc/terms/isPartOf");
        assertThat(Formats.inverseLinkRelation("http://purl.org/DC/TERMS/isPartOf"))
                .isEmpty();
        assertThat(Formats.inverseLinkRelation("http://purl.org/dc/terms/IsPartOf"))
                .isEmpty();
        // A different namespace spelling of the same term stays unknown.
        assertThat(Formats.inverseLinkRelation("http://purl.org/dc/elements/1.1/isPartOf"))
                .isEmpty();
    }

    private static void assertStrictlySorted(String[] table) {
        for (int i = 1; i < table.length; i++) {
            assertThat(table[i - 1].compareTo(table[i]))
                    .as("%s before %s", table[i - 1], table[i])
                    .isNegative();
        }
    }
}
