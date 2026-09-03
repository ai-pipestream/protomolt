package ai.protomolt.proto.seo;

import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.seo.v1.SearchStandard;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SeoIndexing#mappingFor} derives the mapping the seo.v1 annotations declare: the
 * standard's containers expand into dotted leaf paths, and each spot-checked path carries
 * the annotated kind and docValues flags.
 */
class SeoIndexMappingTest {

    private static IndexMapping mapping;

    @BeforeAll
    static void buildMapping() {
        mapping = SeoIndexing.mappingFor(SearchStandard.getDescriptor());
    }

    private static IndexMapping.IndexedField field(String path) {
        return mapping.find(path).orElseThrow(
                () -> new AssertionError("mapping is missing path " + path));
    }

    @Test
    void dublinCoreTitleIsStoredText() {
        assertThat(field("dublin_core.title").type()).isEqualTo(IndexFieldKind.TEXT);
        assertThat(field("dublin_core.title").stored()).isTrue();
    }

    @Test
    void dublinCoreSubjectIsFacetableKeywordAndRepeated() {
        IndexMapping.IndexedField subject = field("dublin_core.subject");
        assertThat(subject.type()).isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(subject.hint().facetable()).isTrue();
        assertThat(subject.repeated()).isTrue();
    }

    @Test
    void dublinCoreDateIsSortableDate() {
        IndexMapping.IndexedField date = field("dublin_core.date");
        assertThat(date.type()).isEqualTo(IndexFieldKind.DATE);
        assertThat(date.hint().sortable()).isTrue();
    }

    @Test
    void dublinCoreLanguageIsKeyword() {
        assertThat(field("dublin_core.language").type()).isEqualTo(IndexFieldKind.KEYWORD);
    }

    @Test
    void dctermsRefinementsExpandUnderTheTermsContainer() {
        IndexMapping.IndexedField modified = field("dublin_core.terms.modified");
        assertThat(modified.type()).isEqualTo(IndexFieldKind.DATE);
        assertThat(modified.hint().sortable()).isTrue();
        assertThat(field("dublin_core.terms.abstract").type()).isEqualTo(IndexFieldKind.TEXT);
    }

    @Test
    void articleHeadlineIsText() {
        assertThat(field("article.headline").type()).isEqualTo(IndexFieldKind.TEXT);
    }

    @Test
    void articleDatePublishedIsSortableDate() {
        IndexMapping.IndexedField published = field("article.date_published");
        assertThat(published.type()).isEqualTo(IndexFieldKind.DATE);
        assertThat(published.hint().sortable()).isTrue();
    }

    @Test
    void articleAuthorOneofBranchesExpand() {
        assertThat(field("article.author_person.name").type()).isEqualTo(IndexFieldKind.TEXT);
        assertThat(field("article.author_organization.name").type()).isEqualTo(IndexFieldKind.TEXT);
    }

    @Test
    void productSkuIsStoredKeyword() {
        IndexMapping.IndexedField sku = field("product.sku");
        assertThat(sku.type()).isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(sku.stored()).isTrue();
    }

    @Test
    void offerFieldsUnderTheRepeatedOffersScopeAreKeywordAndRepeated() {
        IndexMapping.IndexedField currency = field("product.offers.price_currency");
        assertThat(currency.type()).isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(currency.repeated()).isTrue();
        assertThat(currency.fieldName()).isEqualTo("product_offers_price_currency");
        assertThat(field("product.offers.price").type()).isEqualTo(IndexFieldKind.KEYWORD);
    }

    @Test
    void aggregateRatingValueIsSortableDouble() {
        IndexMapping.IndexedField rating = field("product.aggregate_rating.rating_value");
        assertThat(rating.type()).isEqualTo(IndexFieldKind.DOUBLE);
        assertThat(rating.hint().sortable()).isTrue();
    }

    @Test
    void breadcrumbPositionsAreSortableInt64() {
        IndexMapping.IndexedField position = field("breadcrumb_list.items.position");
        assertThat(position.type()).isEqualTo(IndexFieldKind.INT64);
        assertThat(position.hint().sortable()).isTrue();
        assertThat(position.repeated()).isTrue();
    }

    @Test
    void envelopeKeywordsAreFacetableKeyword() {
        IndexMapping.IndexedField keywords = field("keywords");
        assertThat(keywords.type()).isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(keywords.hint().facetable()).isTrue();
        assertThat(keywords.repeated()).isTrue();
    }

    @Test
    void canonicalUrlIsKeyword() {
        assertThat(field("canonical_url").type()).isEqualTo(IndexFieldKind.KEYWORD);
    }

    @Test
    void videoUploadDateIsSortableDate() {
        IndexMapping.IndexedField uploadDate = field("video_object.upload_date");
        assertThat(uploadDate.type()).isEqualTo(IndexFieldKind.DATE);
        assertThat(uploadDate.hint().sortable()).isTrue();
    }

    @Test
    void ratingAndReviewCountsAreInt64() {
        assertThat(field("product.aggregate_rating.rating_count").type())
                .isEqualTo(IndexFieldKind.INT64);
        assertThat(field("product.aggregate_rating.review_count").type())
                .isEqualTo(IndexFieldKind.INT64);
    }
}
