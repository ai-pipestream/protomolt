package ai.pipestream.proto.seo;

import ai.pipestream.proto.seo.v1.Article;
import ai.pipestream.proto.seo.v1.DublinCore;
import ai.pipestream.proto.seo.v1.FaqPage;
import ai.pipestream.proto.seo.v1.Offer;
import ai.pipestream.proto.seo.v1.Organization;
import ai.pipestream.proto.seo.v1.Person;
import ai.pipestream.proto.seo.v1.Product;
import ai.pipestream.proto.seo.v1.SearchStandard;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The validate.v1 rules on the seo.v1 descriptors, exercised through the house
 * {@link ProtoValidator}: well-formed Article/Product/SearchStandard messages pass, and
 * each family of declared rules (required, max_len, pattern, defined_only enum, uri, the
 * cross-field CEL invariant) reports its violation.
 */
class SeoValidationTest {

    private static final Timestamp PUBLISHED = Timestamp.newBuilder().setSeconds(1710460800).build();
    private static final Timestamp EARLIER = Timestamp.newBuilder().setSeconds(1578182400).build();

    private static ValidationResult validate(com.google.protobuf.Message message) {
        return ProtoValidator.forMessageType(message.getDescriptorForType()).validate(message);
    }

    private static Article validArticle() {
        return Article.newBuilder()
                .setHeadline("How search engines read structured data")
                .setAuthorPerson(Person.newBuilder().setName("Ada Lovelace"))
                .setDatePublished(EARLIER)
                .setDateModified(PUBLISHED)
                .addImageUrls("https://example.com/img/wide.jpg")
                .setPublisher(Organization.newBuilder().setName("Example Press"))
                .build();
    }

    private static Product validProduct() {
        return Product.newBuilder()
                .setName("Blue Widget")
                .setSku("SKU-42-BLUE")
                .setGtin("00012345678905")
                .setBrand("Widgets Inc")
                .addOffers(Offer.newBuilder()
                        .setPrice("19.99")
                        .setPriceCurrency("USD")
                        .setAvailability(Offer.Availability.AVAILABILITY_IN_STOCK)
                        .setUrl("https://example.com/widget"))
                .build();
    }

    @Test
    void validArticlePasses() {
        assertThat(validate(validArticle()).valid()).isTrue();
    }

    @Test
    void validProductPasses() {
        assertThat(validate(validProduct()).valid()).isTrue();
    }

    @Test
    void validSearchStandardPasses() {
        SearchStandard standard = SearchStandard.newBuilder()
                .setDublinCore(DublinCore.newBuilder()
                        .setTitle("Deep Learning Handbook")
                        .setLanguage("en-US")
                        .addSubject("machine learning"))
                .addKeywords("transformers")
                .setCanonicalUrl("https://example.com/articles/transformers")
                .setArticle(validArticle())
                .build();

        ValidationResult result = validate(standard);
        assertThat(result.violations()).isEmpty();
        assertThat(result.valid()).isTrue();
    }

    @Test
    void articleWithoutHeadlineViolatesRequired() {
        Article bad = validArticle().toBuilder().clearHeadline().build();
        ValidationResult result = validate(bad);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> {
                    assertThat(v.path()).isEqualTo("headline");
                    assertThat(v.ruleId()).contains("required");
                });
    }

    @Test
    void overlongHeadlineViolatesMaxLen() {
        Article bad = validArticle().toBuilder().setHeadline("x".repeat(111)).build();
        ValidationResult result = validate(bad);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> {
                    assertThat(v.path()).isEqualTo("headline");
                    assertThat(v.ruleId()).contains("max_len");
                });
    }

    @Test
    void dateModifiedBeforeDatePublishedViolatesTheCelInvariant() {
        Article bad = validArticle().toBuilder()
                .setDatePublished(PUBLISHED)
                .setDateModified(EARLIER)
                .build();
        ValidationResult result = validate(bad);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> assertThat(v.ruleId()).isEqualTo("modified-not-before-published"));
    }

    @Test
    void lowercaseCurrencyViolatesThePattern() {
        Product bad = validProduct().toBuilder()
                .setOffers(0, validProduct().getOffers(0).toBuilder().setPriceCurrency("usd"))
                .build();
        ValidationResult result = validate(bad);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> {
                    assertThat(v.path()).contains("price_currency");
                    assertThat(v.ruleId()).contains("pattern");
                });
    }

    @Test
    void unknownAvailabilityNumberViolatesDefinedOnly() {
        Product bad = validProduct().toBuilder()
                .setOffers(0, validProduct().getOffers(0).toBuilder().setAvailabilityValue(99))
                .build();
        ValidationResult result = validate(bad);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> {
                    assertThat(v.path()).contains("availability");
                    assertThat(v.ruleId()).contains("defined_only");
                });
    }

    @Test
    void malformedCanonicalUrlViolatesUri() {
        SearchStandard bad = SearchStandard.newBuilder()
                .setCanonicalUrl("not a url")
                .build();
        ValidationResult result = validate(bad);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> {
                    assertThat(v.path()).isEqualTo("canonical_url");
                    assertThat(v.ruleId()).contains("uri");
                });
    }

    @Test
    void faqPageWithoutQuestionsViolatesMinItems() {
        SearchStandard bad = SearchStandard.newBuilder()
                .setFaqPage(FaqPage.getDefaultInstance())
                .build();
        ValidationResult result = validate(bad);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> {
                    assertThat(v.path()).contains("questions");
                    assertThat(v.ruleId()).contains("min_items");
                });
    }

    @Test
    void invalidGtinViolatesThePattern() {
        Product bad = validProduct().toBuilder().setGtin("1234").build();
        ValidationResult result = validate(bad);
        assertThat(result.valid()).isFalse();
        assertThat(result.violations())
                .anySatisfy(v -> {
                    assertThat(v.path()).isEqualTo("gtin");
                    assertThat(v.ruleId()).contains("pattern");
                });
    }
}
