package ai.protomolt.proto.sources;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProtoImportsTest {

    @Test
    void parsesPlainPublicAndWeakImports() {
        String proto = """
                syntax = "proto3";
                import "a/one.proto";
                import public "b/two.proto";
                import weak "c/three.proto";
                message M {}
                """;
        assertThat(ProtoImports.of(proto))
                .containsExactly("a/one.proto", "b/two.proto", "c/three.proto");
    }

    @Test
    void ignoresCommentedOutImports() {
        String proto = """
                syntax = "proto3";
                // import "commented/line.proto";
                /* import "commented/block.proto"; */
                import "real.proto";
                """;
        assertThat(ProtoImports.of(proto)).containsExactly("real.proto");
    }

    @Test
    void ignoresImportsBuriedInMultiLineBlockComments() {
        String proto = """
                /*
                 import "hidden.proto";
                 */
                import "real.proto";
                """;
        assertThat(ProtoImports.of(proto)).containsExactly("real.proto");
    }

    @Test
    void returnsEmptyForNoImports() {
        assertThat(ProtoImports.of("syntax = \"proto3\";\nmessage M {}")).isEmpty();
    }

    @Test
    void toleratesLeadingWhitespaceAndTabs() {
        String proto = """
                syntax = "proto3";
                    import "indented.proto";
                \timport "tabbed.proto";
                """;
        assertThat(ProtoImports.of(proto))
                .containsExactly("indented.proto", "tabbed.proto");
    }

    @Test
    void toleratesCrLfLineEndings() {
        String proto = "syntax = \"proto3\";\r\nimport \"crlf.proto\";\r\nmessage M {}\r\n";
        assertThat(ProtoImports.of(proto)).containsExactly("crlf.proto");
    }

    @Test
    void toleratesCommentsBetweenTheKeywordAndThePath() {
        // Comment stripping runs before the import scan, so a comment in the middle of an
        // import statement collapses to whitespace and the import is still found; the scan's
        // whitespace even spans the line break a trailing line comment leaves behind.
        String proto = """
                syntax = "proto3";
                import /* inline */ "mid.proto";
                import // trailing
                    "next-line.proto";
                """;
        assertThat(ProtoImports.of(proto)).containsExactly("mid.proto", "next-line.proto");
    }

    @Test
    void preservesDuplicatesInDeclarationOrder() {
        String proto = """
                syntax = "proto3";
                import "b.proto";
                import "a.proto";
                import "b.proto";
                """;
        assertThat(ProtoImports.of(proto)).containsExactly("b.proto", "a.proto", "b.proto");
    }

    @Test
    void requiresATerminatingSemicolon() {
        String proto = """
                syntax = "proto3";
                import "unterminated.proto"
                message M {}
                """;
        assertThat(ProtoImports.of(proto)).isEmpty();
    }

    @Test
    void recognizesOnlyDoubleQuotedPaths() {
        // The scanner is deliberately a syntactic pre-pass (see the class javadoc): protoc also
        // accepts single-quoted import paths, but this scan only recognizes double quotes;
        // compilation remains the authoritative check.
        String proto = """
                syntax = "proto3";
                import 'single.proto';
                """;
        assertThat(ProtoImports.of(proto)).isEmpty();
    }

    @Test
    void findsOnlyTheFirstImportOnASingleLine() {
        // The scan anchors imports at line starts; two imports on one line is legal proto but
        // only the first is seen. Compilation catches what the scan misses.
        String proto = "syntax = \"proto3\";\nimport \"first.proto\"; import \"second.proto\";\n";
        assertThat(ProtoImports.of(proto)).containsExactly("first.proto");
    }

    @Test
    void requiresWhitespaceBetweenTheKeywordAndThePath() {
        String proto = """
                syntax = "proto3";
                import"tight.proto";
                import public"tight-modified.proto";
                """;
        assertThat(ProtoImports.of(proto)).isEmpty();
    }

    @Test
    void rejectsAnEmptyPath() {
        assertThat(ProtoImports.of("import \"\";")).isEmpty();
    }

    @Test
    void anUnterminatedBlockCommentReadsAsPlainText() {
        // Without a closing marker the comment never forms, so text after the '/' still
        // scans: the import on the next line is at a line start and is found, while one
        // glued to the same line as the marker is not.
        assertThat(ProtoImports.of("/* unterminated\nimport \"after.proto\";\n"))
                .containsExactly("after.proto");
        assertThat(ProtoImports.of("/* import \"hidden.proto\";")).isEmpty();
    }

    @Test
    void aBlockCommentOnItsOwnLineDoesNotHideTheNextImport() {
        String proto = """
                /* header */
                import "real.proto";
                """;
        assertThat(ProtoImports.of(proto)).containsExactly("real.proto");
    }

    @Test
    void aModifierMayBeFollowedByALineBreak() {
        String proto = "import weak\n    \"broken-line.proto\";\n";
        assertThat(ProtoImports.of(proto)).containsExactly("broken-line.proto");
    }

    @Test
    void aKeywordGluedToTheModifierIsNotAnImport() {
        assertThat(ProtoImports.of("importpublic \"glued.proto\";")).isEmpty();
    }
}
