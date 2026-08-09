package ai.pipestream.proto.sources.publish;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublishModelTest {

    @Test
    void importPathNamingIsIdentity() {
        assertThat(SubjectNamingStrategy.importPath().subjectFor("common/v1/core.proto"))
                .isEqualTo("common/v1/core.proto");
    }

    @Test
    void baseNameNamingStripsDirectoriesAndSuffix() {
        SubjectNamingStrategy naming = SubjectNamingStrategy.baseName();
        assertThat(naming.subjectFor("common/v1/core.proto")).isEqualTo("core");
        assertThat(naming.subjectFor("flat.proto")).isEqualTo("flat");
        assertThat(naming.subjectFor("odd-name")).isEqualTo("odd-name");
    }

    @Test
    void prefixedNamingPrepends() {
        assertThat(SubjectNamingStrategy.prefixed("schemas/").subjectFor("a/b.proto"))
                .isEqualTo("schemas/a/b.proto");
    }

    @Test
    void resultCountsByAction() {
        PublishResult result = new PublishResult(List.of(
                new PublishResult.FileOutcome("a.proto", "a.proto", PublishResult.Action.CREATED, "v1"),
                new PublishResult.FileOutcome("b.proto", "b.proto", PublishResult.Action.UNCHANGED, "v3"),
                new PublishResult.FileOutcome("c.proto", "c.proto", PublishResult.Action.UPDATED, "v2")));
        assertThat(result.created()).isEqualTo(1);
        assertThat(result.unchanged()).isEqualTo(1);
        assertThat(result.updated()).isEqualTo(1);
        assertThat(result.failures()).isEmpty();
        assertThatCode(result::throwIfFailed).doesNotThrowAnyException();
    }

    @Test
    void throwIfFailedSummarizesEveryFailure() {
        PublishResult result = new PublishResult(List.of(
                new PublishResult.FileOutcome("ok.proto", "ok.proto", PublishResult.Action.CREATED, "v1"),
                new PublishResult.FileOutcome("bad.proto", "bad.proto", PublishResult.Action.FAILED, "409 incompatible"),
                new PublishResult.FileOutcome("worse.proto", "worse.proto", PublishResult.Action.FAILED, "422 invalid")));
        assertThatThrownBy(result::throwIfFailed)
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining("2 of 3")
                .hasMessageContaining("bad.proto")
                .hasMessageContaining("409 incompatible")
                .hasMessageContaining("worse.proto");
    }

    @Test
    void dryRunOptionsCarryFlag() {
        assertThat(PublishOptions.dryRunDefaults().dryRun()).isTrue();
        assertThat(PublishOptions.defaults().dryRun()).isFalse();
        PublishOptions custom = PublishOptions.defaults().withNaming(SubjectNamingStrategy.baseName());
        assertThat(custom.naming().subjectFor("x/y.proto")).isEqualTo("y");
    }

    @Test
    void baseNameNamingHandlesDeepPathsAndRepeatedSuffixes() {
        SubjectNamingStrategy naming = SubjectNamingStrategy.baseName();
        assertThat(naming.subjectFor("a/b/c/core.proto")).isEqualTo("core");
        assertThat(naming.subjectFor("core.proto")).isEqualTo("core");
        // Only the final ".proto" suffix is stripped.
        assertThat(naming.subjectFor("x/y.proto.proto")).isEqualTo("y.proto");
        // A name without the suffix is used as-is.
        assertThat(naming.subjectFor("dir/schema")).isEqualTo("schema");
    }

    @Test
    void prefixedNamingWithEmptyPrefixIsIdentity() {
        assertThat(SubjectNamingStrategy.prefixed("").subjectFor("a/b.proto"))
                .isEqualTo("a/b.proto");
    }

    @Test
    void optionsRequireANamingStrategy() {
        assertThatThrownBy(() -> new PublishOptions(null, false))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void withNamingPreservesTheDryRunFlag() {
        PublishOptions custom = PublishOptions.dryRunDefaults()
                .withNaming(SubjectNamingStrategy.prefixed("p/"));
        assertThat(custom.dryRun()).isTrue();
        assertThat(custom.naming().subjectFor("a.proto")).isEqualTo("p/a.proto");
    }

    @Test
    void outcomesListIsImmutable() {
        PublishResult result = new PublishResult(List.of(
                new PublishResult.FileOutcome("a.proto", "a.proto", PublishResult.Action.CREATED, "v1")));
        assertThatThrownBy(() -> result.outcomes().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void wouldWriteIsNeitherAWriteCountNorAFailure() {
        PublishResult result = new PublishResult(List.of(
                new PublishResult.FileOutcome("a.proto", "a.proto", PublishResult.Action.WOULD_WRITE, ""),
                new PublishResult.FileOutcome("b.proto", "b.proto", PublishResult.Action.CREATED, "v1")));
        assertThat(result.created()).isEqualTo(1);
        assertThat(result.updated()).isZero();
        assertThat(result.unchanged()).isZero();
        assertThat(result.failures()).isEmpty();
        assertThatCode(result::throwIfFailed).doesNotThrowAnyException();
    }

    @Test
    void failuresAreReportedInDeclarationOrder() {
        PublishResult result = new PublishResult(List.of(
                new PublishResult.FileOutcome("ok.proto", "ok.proto", PublishResult.Action.UNCHANGED, "v3"),
                new PublishResult.FileOutcome("first.proto", "first.proto", PublishResult.Action.FAILED, "e1"),
                new PublishResult.FileOutcome("second.proto", "second.proto", PublishResult.Action.FAILED, "e2")));
        assertThat(result.failures())
                .extracting(PublishResult.FileOutcome::path)
                .containsExactly("first.proto", "second.proto");
        assertThatThrownBy(result::throwIfFailed)
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining("2 of 3");
    }

    @Test
    void singleFailureMessageNamesTheFileAndDetail() {
        PublishResult result = new PublishResult(List.of(
                new PublishResult.FileOutcome("ok.proto", "ok.proto", PublishResult.Action.CREATED, "v1"),
                new PublishResult.FileOutcome("bad.proto", "bad.proto", PublishResult.Action.FAILED, "boom")));
        assertThatThrownBy(result::throwIfFailed)
                .isInstanceOf(SchemaPublishException.class)
                .hasMessageContaining("1 of 2")
                .hasMessageContaining("bad.proto (boom)");
    }

    @Test
    void publishExceptionCarriesACause() {
        RuntimeException cause = new RuntimeException("io");
        SchemaPublishException exception = new SchemaPublishException("publish failed", cause);
        assertThat(exception).hasMessage("publish failed").hasCause(cause);
    }
}
