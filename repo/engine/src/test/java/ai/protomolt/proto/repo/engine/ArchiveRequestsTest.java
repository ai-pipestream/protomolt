package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.spi.RepositoryException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The archive surface validates in code what its protos annotate. Every refusal must be an
 * INVALID_ARGUMENT naming the offending field, and every length bound must admit its own limit
 * while refusing one character past it.
 */
class ArchiveRequestsTest {

    private static final String SHA = "0123456789abcdef".repeat(4);

    private static AbstractThrowableAssert<?, ? extends Throwable> refused(ThrowingCallable call, String message) {
        return assertThatThrownBy(call)
                .isInstanceOf(RepositoryException.class)
                .hasMessage(message)
                .satisfies(e -> assertThat(((RepositoryException) e).code())
                        .isEqualTo(RepositoryException.Code.INVALID_ARGUMENT));
    }

    private static EntryAddress address(String account, String archive, String entry) {
        return EntryAddress.newBuilder().setAccountId(account).setArchive(archive).setEntryId(entry).build();
    }

    @Test
    void anAddressAtEveryUpperBoundIsReturnedUnchanged() {
        var address = address("a".repeat(200), "b".repeat(100), "e".repeat(500));
        assertThat(ArchiveRequests.address(true, address)).isSameAs(address);
    }

    @Test
    void anAbsentAddressIsRefusedEvenWhenItsSegmentsAreValid() {
        refused(() -> ArchiveRequests.address(false, address("acct", "docs", "e1")), "address is required");
    }

    @Test
    void addressSegmentsAreCheckedInOrderAndNamedByField() {
        refused(() -> ArchiveRequests.address(true, address(" ", "docs", "e1")), "account_id is required");
        refused(() -> ArchiveRequests.address(true, address("acct", "", "e1")), "address.archive is required");
        refused(() -> ArchiveRequests.address(true, address("acct", "Docs", "e1")),
                "address.archive must be a slug of at most 100 characters");
        refused(() -> ArchiveRequests.address(true, address("acct", "docs", " \t")), "address.entry_id is required");
        refused(() -> ArchiveRequests.address(true, address("acct", "docs", "e".repeat(501))),
                "address.entry_id exceeds 500 characters");
        refused(() -> ArchiveRequests.address(true, EntryAddress.getDefaultInstance()), "account_id is required");
    }

    @Test
    void accountIdIsBoundedAt200Characters() {
        assertThat(ArchiveRequests.accountId(" padded ")).isEqualTo(" padded ");
        refused(() -> ArchiveRequests.accountId(""), "account_id is required");
        refused(() -> ArchiveRequests.accountId("a".repeat(201)), "account_id exceeds 200 characters");
    }

    @ParameterizedTest
    @ValueSource(strings = {"Docs", "-docs", "docs-", "do--cs", "do cs", "docs/x", "décs"})
    void archiveNamesThatAreNotSlugsAreRefusedUnderTheCallersFieldName(String name) {
        refused(() -> ArchiveRequests.archiveName(name, "target.archive"),
                "target.archive must be a slug of at most 100 characters");
    }

    @Test
    void archiveNameAdmitsSeparatorsAndRefusesOneCharacterPastTheBound() {
        assertThat(ArchiveRequests.archiveName("a.b_c-9", "archive")).isEqualTo("a.b_c-9");
        refused(() -> ArchiveRequests.archiveName("b".repeat(101), "archive"),
                "archive must be a slug of at most 100 characters");
        refused(() -> ArchiveRequests.archiveName("  ", "archive"), "archive is required");
    }

    @Test
    void aMinimalRenditionNeedsOnlyItsName() {
        var rendition = RenditionDescriptor.newBuilder().setName("original").build();
        assertThat(ArchiveRequests.rendition(true, rendition)).isSameAs(rendition);
        refused(() -> ArchiveRequests.rendition(false, rendition), "rendition is required");
        refused(() -> ArchiveRequests.rendition(true, RenditionDescriptor.getDefaultInstance()),
                "rendition.name is required");
    }

    @Test
    void renditionOptionalFieldsAreValidatedOnlyWhenSet() {
        var full = RenditionDescriptor.newBuilder().setName("Page.v2").setSubKey("p".repeat(200))
                .setMediaType("application/vnd.api+json").setSchemaSubject("s".repeat(300)).build();
        assertThat(ArchiveRequests.rendition(true, full)).isSameAs(full);

        var base = RenditionDescriptor.newBuilder().setName("parsed");
        refused(() -> ArchiveRequests.rendition(true, base.clone().setSubKey("../escape").build()),
                "rendition.sub_key must be path-safe and at most 200 characters");
        refused(() -> ArchiveRequests.rendition(true, base.clone().setSubKey("p".repeat(201)).build()),
                "rendition.sub_key must be path-safe and at most 200 characters");
        refused(() -> ArchiveRequests.rendition(true, base.clone().setMediaType("text/plain; charset=utf-8").build()),
                "rendition.media_type must be a valid media type");
        refused(() -> ArchiveRequests.rendition(true, base.clone().setSchemaSubject("s".repeat(301)).build()),
                "rendition.schema_subject exceeds 300 characters");
    }

    @Test
    void aGrammaticalMediaTypeLongerThan200CharactersIsStillRefused() {
        String longType = "a".repeat(100) + "/" + "b".repeat(100);
        var rendition = RenditionDescriptor.newBuilder().setName("parsed").setMediaType(longType).build();
        refused(() -> ArchiveRequests.rendition(true, rendition), "rendition.media_type must be a valid media type");
    }

    @Test
    void renditionNameRefusesLeadingDotsAndOverlongNames() {
        refused(() -> ArchiveRequests.renditionName("..", "rendition"),
                "rendition must be path-safe and at most 200 characters");
        refused(() -> ArchiveRequests.renditionName(".hidden", "rendition"),
                "rendition must be path-safe and at most 200 characters");
        refused(() -> ArchiveRequests.renditionName("r".repeat(201), "rendition"),
                "rendition must be path-safe and at most 200 characters");
        assertThat(ArchiveRequests.renditionName("r".repeat(200), "rendition")).hasSize(200);
    }

    @Test
    void sha256IsOptionalButMustBeLowercase64HexWhenGiven() {
        assertThat(ArchiveRequests.sha256("", "expected_sha256")).isEmpty();
        assertThat(ArchiveRequests.sha256(SHA, "expected_sha256")).isEqualTo(SHA);
        for (String bad : new String[] {SHA.toUpperCase(), SHA.substring(1), SHA + "0", "g" + SHA.substring(1)}) {
            refused(() -> ArchiveRequests.sha256(bad, "expected_sha256"),
                    "expected_sha256 must be a lowercase hex SHA-256");
        }
    }

    @Test
    void pageSizeZeroTakesTheDefaultAndTheBoundsAreInclusive() {
        assertThat(ArchiveRequests.page(0)).isEqualTo(ArchiveRequests.DEFAULT_PAGE).isEqualTo(100);
        assertThat(ArchiveRequests.page(1)).isEqualTo(1);
        assertThat(ArchiveRequests.page(1000)).isEqualTo(ArchiveRequests.MAX_PAGE).isEqualTo(1000);
        refused(() -> ArchiveRequests.page(1001), "limit must be between 0 and 1000");
        refused(() -> ArchiveRequests.page(-1), "limit must be between 0 and 1000");
        refused(() -> ArchiveRequests.page(Integer.MIN_VALUE), "limit must be between 0 and 1000");
    }

    @Test
    void blankContinuationTokenStartsAtZeroAndANumericTokenIsItsOffset() {
        assertThat(ArchiveRequests.offset("")).isZero();
        assertThat(ArchiveRequests.offset("  ")).isZero();
        assertThat(ArchiveRequests.offset("0")).isZero();
        assertThat(ArchiveRequests.offset("250")).isEqualTo(250L);
        assertThat(ArchiveRequests.offset(Long.toString(Long.MAX_VALUE))).isEqualTo(Long.MAX_VALUE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "abc", "1.5", " 5", "9223372036854775808", "0x10"})
    void foreignContinuationTokensAreRefusedWithoutExposingTheParseFailure(String token) {
        refused(() -> ArchiveRequests.offset(token), "continuation_token is not from this listing")
                .hasNoCause();
    }

    @Test
    void boundedAdmitsItsLimitAndNamesTheFieldPastIt() {
        assertThat(ArchiveRequests.bounded("", 0, "note")).isEmpty();
        assertThat(ArchiveRequests.bounded("x".repeat(64), 64, "note")).hasSize(64);
        refused(() -> ArchiveRequests.bounded("x".repeat(65), 64, "note"), "note exceeds 64 characters");
        refused(() -> ArchiveRequests.bounded("x", 0, "label"), "label exceeds 0 characters");
    }
}
