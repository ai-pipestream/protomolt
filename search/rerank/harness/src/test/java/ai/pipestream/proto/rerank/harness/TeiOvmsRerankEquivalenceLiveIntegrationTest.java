package ai.pipestream.proto.rerank.harness;

import ai.pipestream.proto.rerank.ovms.OvmsRerankProvider;
import ai.pipestream.proto.rerank.tei.TeiRerankProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live equivalence check: TEI and OpenVINO Model Server serving the same reranker model must
 * produce the same ranking, or a runtime cannot mix the two providers. The suite runs only
 * when all three knobs point at live servers:
 * {@value TeiRerankProvider#TARGET_ENVIRONMENT_VARIABLE},
 * {@value OvmsRerankProvider#URL_ENVIRONMENT_VARIABLE}, and
 * {@value OvmsRerankProvider#MODEL_ENVIRONMENT_VARIABLE}; it skips cleanly otherwise.
 */
@Tag("integration")
class TeiOvmsRerankEquivalenceLiveIntegrationTest {

    private static final double THRESHOLD = 0.9;

    /** Noise floor for the tau computation: both servers answer sigmoid probabilities, so
     * score differences below 1e-3 are sub-floor jitter, not relevance disagreements. */
    private static final double SCORE_EPSILON = 1e-3;

    private static final List<QueryDocuments> CASES = List.of(
            new QueryDocuments("What do giant pandas eat?", List.of(
                    "Giant pandas spend most of the day eating bamboo shoots and leaves.",
                    "A panda's jaw muscles are built for crushing tough bamboo stalks.",
                    "The central bank left interest rates unchanged after its June meeting.",
                    "Rising oil prices pushed energy stocks higher on Tuesday.",
                    "Knead the dough for ten minutes, then let it rise overnight.",
                    "A pinch of saffron gives the rice its golden color.",
                    "The committee published its report after the recess.")),
            new QueryDocuments("How do I get a sourdough loaf to rise?", List.of(
                    "Feed the starter twice a day until it doubles within four hours.",
                    "A long cold ferment gives sourdough its tangy flavor and open crumb.",
                    "Steam in the first minutes of baking helps the crust expand.",
                    "Shares of the chipmaker fell after the earnings miss.",
                    "Pandas in the wild climb trees to escape predators.",
                    "The hiking trail crosses the river twice before the summit.")),
            new QueryDocuments("What drives the price of technology stocks?", List.of(
                    "Quarterly earnings and forward guidance move share prices the most.",
                    "Rising interest rates tend to compress the valuation of growth stocks.",
                    "Institutional buying can lift a whole sector for weeks.",
                    "Blanch the green beans before tossing them in the salad.",
                    "The library opens at nine on weekdays.",
                    "A giant panda cub weighs about a hundred grams at birth.")),
            new QueryDocuments("Which spices are essential for a curry?", List.of(
                    "Toast cumin and coriander seeds before grinding them for the masala.",
                    "Turmeric gives the curry its color and a faint bitterness.",
                    "Cardamom pods should be bruised so the seeds release their aroma.",
                    "The index fund tracks the five hundred largest companies.",
                    "Bamboo flowers once every few decades, then the plant dies.",
                    "The ferry to the island runs every hour in summer.")),
            new QueryDocuments("Where do red pandas live in the wild?", List.of(
                    "Red pandas inhabit the temperate forests of the Himalayas.",
                    "They sleep in tree branches and descend to forage at dusk.",
                    "The bond market rallied after the inflation report came in cool.",
                    "Simmer the stock for three hours and skim the foam.",
                    "The museum's new wing opens to the public in March.",
                    "Quarterly revenue beat analyst expectations by a wide margin.")),
            new QueryDocuments("What is an index fund?", List.of(
                    "An index fund holds every stock in a market index in proportion to its weight.",
                    "Because they are passively managed, index funds charge very low fees.",
                    "Let the bread cool on a rack before slicing it.",
                    "Pandas communicate with scent marks on tree trunks.",
                    "The novel follows three generations of a family in exile.",
                    "A slow cooker turns tough cuts tender over eight hours.")));

    @Test
    void teiAndOvmsServingTheSameRerankerCertify() {
        String teiTarget = System.getenv(TeiRerankProvider.TARGET_ENVIRONMENT_VARIABLE);
        String ovmsUrl = System.getenv(OvmsRerankProvider.URL_ENVIRONMENT_VARIABLE);
        String ovmsModel = System.getenv(OvmsRerankProvider.MODEL_ENVIRONMENT_VARIABLE);
        assumeTrue(teiTarget != null && !teiTarget.isBlank(),
                "Set " + TeiRerankProvider.TARGET_ENVIRONMENT_VARIABLE
                        + " to the TEI server's host:port to run this test");
        assumeTrue(ovmsUrl != null && !ovmsUrl.isBlank(),
                "Set " + OvmsRerankProvider.URL_ENVIRONMENT_VARIABLE
                        + " to the OVMS server's base URL to run this test");
        assumeTrue(ovmsModel != null && !ovmsModel.isBlank(),
                "Set " + OvmsRerankProvider.MODEL_ENVIRONMENT_VARIABLE
                        + " to the servable name to run this test");

        System.setProperty(TeiRerankProvider.TARGET_PROPERTY, teiTarget);
        System.setProperty(OvmsRerankProvider.URL_PROPERTY, ovmsUrl);
        System.setProperty(OvmsRerankProvider.MODEL_PROPERTY, ovmsModel);
        try (TeiRerankProvider tei = new TeiRerankProvider()) {
            OvmsRerankProvider ovms = new OvmsRerankProvider();
            RerankEquivalenceReport report = new RerankEquivalence()
                    .compare(tei, ovms, CASES, THRESHOLD, SCORE_EPSILON);
            assertThat(report.certified()).as("%s", report).isTrue();
            assertThat(report.top1Agreement()).as("%s", report).isEqualTo(CASES.size());
        } finally {
            System.clearProperty(TeiRerankProvider.TARGET_PROPERTY);
            System.clearProperty(OvmsRerankProvider.URL_PROPERTY);
            System.clearProperty(OvmsRerankProvider.MODEL_PROPERTY);
        }
    }
}
