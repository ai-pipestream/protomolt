package ai.protomolt.proto.parse.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.parse.service.RoutingRules.RoutingContext;
import ai.protomolt.proto.parse.v1.PlannedParse;
import ai.protomolt.proto.parse.v1.RoutingRule;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the rule-set semantics: the CEL bindings the routing.proto header
 * declares, set (not first-match) routing, deterministic ordering, and the
 * fail-at-boot contract for broken guards.
 */
class RoutingRulesTest {

    private static final RoutingContext PDF_CONTEXT = new RoutingContext(
            "application/pdf", "text/plain", "report.pdf", "pdf", 1024, "acct-1");

    private static RoutingRule rule(String id, String when, String parser, int priority) {
        return RoutingRule.newBuilder()
                .setRuleId(id)
                .setWhen(when)
                .setParserName(parser)
                .setPriority(priority)
                .build();
    }

    @Test
    void everyDeclaredBindingIsEvaluable() {
        RoutingRules rules = RoutingRules.of(List.of(
                rule("r-mime", "mime_type == 'application/pdf'", "p1", 60),
                rule("r-declared", "declared_mime_type == 'text/plain'", "p2", 50),
                rule("r-filename", "filename.endsWith('.pdf')", "p3", 40),
                rule("r-extension", "extension == 'pdf'", "p4", 30),
                rule("r-size", "size_bytes < 52428800", "p5", 20),
                rule("r-account", "account_id == 'acct-1'", "p6", 10)));
        assertThat(rules.plan(PDF_CONTEXT))
                .extracting(PlannedParse::getParserName)
                .containsExactly("p1", "p2", "p3", "p4", "p5", "p6");
    }

    @Test
    void routingIsASetEveryMatchingRuleContributes() {
        RoutingRules rules = RoutingRules.of(List.of(
                rule("r-pdf", "mime_type == 'application/pdf'", "docling", 0),
                rule("r-all", "true", "tika", 0),
                rule("r-images", "mime_type.startsWith('image/')", "ocr", 0)));
        List<PlannedParse> planned = rules.plan(PDF_CONTEXT);
        assertThat(planned).hasSize(2);
        assertThat(planned)
                .extracting(PlannedParse::getMatchedRuleId)
                .containsExactly("r-all", "r-pdf");
    }

    @Test
    void planOrdersByDescendingPriorityWithRuleIdTiebreak() {
        RoutingRules rules = RoutingRules.of(List.of(
                rule("z-low", "true", "low", 1),
                rule("b-tie", "true", "tie-b", 5),
                rule("a-tie", "true", "tie-a", 5),
                rule("m-high", "true", "high", 9)));
        assertThat(rules.plan(PDF_CONTEXT))
                .extracting(PlannedParse::getParserName)
                .containsExactly("high", "tie-a", "tie-b", "low");
    }

    @Test
    void emptyGuardMatchesNothing() {
        RoutingRules rules = RoutingRules.of(List.of(rule("r-empty", "", "never", 0)));
        assertThat(rules.plan(PDF_CONTEXT)).isEmpty();
    }

    @Test
    void aBrokenGuardFailsAtConstructionNamingTheRule() {
        assertThatThrownBy(() -> RoutingRules.of(List.of(
                rule("r-ok", "true", "fine", 0),
                rule("r-broken", "mime_type ==", "never", 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("r-broken");
        // An unknown variable is just as much a boot failure as a syntax error.
        assertThatThrownBy(() -> RoutingRules.of(List.of(
                rule("r-unknown-var", "no_such_binding == 'x'", "never", 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("r-unknown-var");
    }

    @Test
    void fromJsonRoundTripsProto3JsonIncludingTheParserConfig() {
        String json = """
                [
                  {
                    "ruleId": "r-pdf",
                    "when": "mime_type == 'application/pdf'",
                    "parserName": "docling",
                    "parserConfig": {"ocr": true, "dpi": 300},
                    "priority": 10
                  },
                  {
                    "rule_id": "r-any",
                    "when": "true",
                    "parser_name": "tika",
                    "priority": 5
                  }
                ]
                """;
        RoutingRules rules = RoutingRules.fromJson(json);
        List<PlannedParse> planned = rules.plan(PDF_CONTEXT);
        assertThat(planned).hasSize(2);
        PlannedParse docling = planned.getFirst();
        assertThat(docling.getParserName()).isEqualTo("docling");
        assertThat(docling.getMatchedRuleId()).isEqualTo("r-pdf");
        // The parser_config passes through verbatim.
        assertThat(docling.getParserConfig()).isEqualTo(Struct.newBuilder()
                .putFields("ocr", Value.newBuilder().setBoolValue(true).build())
                .putFields("dpi", Value.newBuilder().setNumberValue(300).build())
                .build());
        assertThat(planned.get(1).getParserName()).isEqualTo("tika");
    }

    @Test
    void malformedJsonFailsLoudly() {
        assertThatThrownBy(() -> RoutingRules.fromJson("{\"not\": \"an array\"}"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> RoutingRules.fromJson("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
