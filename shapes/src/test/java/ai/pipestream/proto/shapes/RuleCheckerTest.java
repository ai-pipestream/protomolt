package ai.pipestream.proto.shapes;

import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Static rule checking in both dialects: paths, shapes, literals, wildcards, CEL
 * type-checking, and the filter-must-be-bool rule.
 */
class RuleCheckerTest {

    private static final String PROTO = """
            syntax = "proto3";
            package check.test;
            import "google/protobuf/struct.proto";
            message Order {
              string id = 1;
              int64 qty = 2;
              repeated string tags = 3;
              Address ship_to = 4;
              google.protobuf.Struct extras = 5;
              map<string, string> attrs = 6;
            }
            message Address {
              string city = 1;
            }
            message Customer {
              string id = 1;
              string name = 2;
              Address home = 3;
            }
            message Summary {
              string order_id = 1;
              string customer_name = 2;
              repeated string tags = 3;
              Address address = 4;
              int64 total = 5;
            }
            """;

    private static Descriptor order;
    private static Descriptor customer;
    private static Descriptor summary;

    private final RuleChecker checker = new RuleChecker();

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("check/test/check.proto", PROTO, "test").build());
        Descriptor[] types = new Descriptor[3];
        var file = compiled.descriptorFor("check/test/check.proto").orElseThrow();
        order = file.findMessageTypeByName("Order");
        customer = file.findMessageTypeByName("Customer");
        summary = file.findMessageTypeByName("Summary");
    }

    private static Map<String, Descriptor> sources() {
        return Map.of("order", order, "customer", customer);
    }

    @Test
    void cleanScopedRulesProduceNoFindings() {
        List<RuleChecker.Finding> findings = checker.checkScoped(sources(), summary,
                List.of("order_id = order.id",
                        "customer_name = customer.name",
                        "tags += order.tags",
                        "address = customer.home",
                        "total = 42",
                        "-customer_name"),
                List.of(new CelMappingRule("order.qty > 0", "order.qty * 2", "total")),
                List.of("customer.name != '' && order.qty >= 1"));
        assertThat(findings).isEmpty();
    }

    @Test
    void badPathsAndShapesAreFound() {
        List<RuleChecker.Finding> findings = checker.checkScoped(sources(), summary,
                List.of("order_id = warehouse.id",          // unknown source
                        "order_id = order.nope",            // unknown field
                        "nope = order.id",                  // unknown target
                        "order_id = order.tags",            // repeated -> singular
                        "customer_name = customer.home",    // message -> scalar
                        "address = order.ship_to.city",     // scalar -> message
                        "order_id += order.id"),            // += onto singular
                List.of(), List.of());
        assertThat(findings).hasSize(7);
        assertThat(findings.get(0).error()).contains("unknown source 'warehouse'");
        assertThat(findings.get(1).error()).contains("no field 'nope'");
        assertThat(findings.get(2).error()).contains("no field 'nope' on check.test.Summary");
        assertThat(findings.get(3).error()).contains("repeated source");
        assertThat(findings.get(4).error()).contains("between message and scalar");
        assertThat(findings.get(5).error()).contains("between message and scalar");
        assertThat(findings.get(6).error()).contains("+= needs a repeated target");
    }

    @Test
    void structAnyAndMapPathsAreUnverifiableNotErrors() {
        List<RuleChecker.Finding> findings = checker.checkScoped(sources(), summary,
                List.of("order_id = order.extras.anything.at.all",
                        "customer_name = order.attrs.some_key"),
                List.of(), List.of());
        assertThat(findings).isEmpty();
    }

    @Test
    void filtersMustTypeCheckToBool() {
        List<RuleChecker.Finding> findings = checker.checkScoped(sources(), summary,
                List.of(), List.of(),
                List.of("order.qty > 0",       // bool: fine
                        "order.qty + 1",       // int: not a filter
                        "order.missing == 1"));// does not compile
        assertThat(findings).hasSize(2);
        assertThat(findings.get(0).kind()).isEqualTo("filter");
        assertThat(findings.get(0).index()).isEqualTo(1);
        assertThat(findings.get(0).error()).contains("must be a boolean");
        assertThat(findings.get(1).index()).isEqualTo(2);
    }

    @Test
    void celRulesCheckFilterSelectorTargetAndFallback() {
        List<RuleChecker.Finding> findings = checker.checkScoped(sources(), summary,
                List.of(),
                List.of(new CelMappingRule("order.qty", null, "total"),          // filter not bool
                        new CelMappingRule(null, "customer.nope", "total"),      // bad selector
                        new CelMappingRule(null, "order.qty", "missing_field"),  // bad target
                        new CelMappingRule(null, null, "total",
                                List.of("total = order.tags"))),                 // bad fallback
                List.of());
        assertThat(findings).hasSize(4);
        assertThat(findings.get(0).error()).startsWith("filter:");
        assertThat(findings.get(1).error()).startsWith("selector:");
        assertThat(findings.get(2).error()).startsWith("target:");
        assertThat(findings.get(3).error()).startsWith("fallback");
    }

    /**
     * The text dialect quotes strings with double quotes only; a single-quoted source is
     * read as a field path at runtime, so waving it through here would hide the failure.
     */
    @Test
    void literalsMatchTheRuntimeDialect() {
        List<RuleChecker.Finding> findings = checker.checkScoped(sources(), summary,
                List.of("order_id = \"fixed\"",
                        "total = -42",
                        "order_id = null",
                        "order_id = 'fixed'"),
                List.of(), List.of());
        assertThat(findings).hasSize(1);
        assertThat(findings.get(0).index()).isEqualTo(3);
        assertThat(findings.get(0).error()).contains("unknown source ''fixed''");
    }

    /** A path of nothing but separators is an invalid path, not an internal error. */
    @Test
    void separatorOnlyPathsAreReportedAsInvalid() {
        List<RuleChecker.Finding> findings = checker.checkScoped(sources(), summary,
                List.of("-.", "order_id = order.."),
                List.of(), List.of());
        assertThat(findings).hasSize(2);
        assertThat(findings.get(0).error()).contains("names no fields");
        assertThat(findings.get(1).error()).contains("names no fields");
    }

    @Test
    void inPlaceModeUsesUnscopedPathsAndTheSingleVariable() {
        List<RuleChecker.Finding> clean = checker.checkInPlace("input", order,
                List.of("id = ship_to.city", "tags += id", "-extras"),
                List.of(new CelMappingRule("input.qty > 1", "input.id + '!'", "id")),
                List.of("input.qty >= 0"));
        assertThat(clean).isEmpty();

        List<RuleChecker.Finding> broken = checker.checkInPlace("input", order,
                List.of("id = tags",                 // repeated -> singular (runtime trap)
                        "qty = customer.name"),      // 'customer' is a field path here, not a scope
                List.of(), List.of());
        assertThat(broken).hasSize(2);
        assertThat(broken.get(0).error()).contains("repeated source");
        assertThat(broken.get(1).error()).contains("no field 'customer'");
    }
}
