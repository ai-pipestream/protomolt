package ai.pipestream.proto.validate.conformance;

import ai.pipestream.proto.validate.conformance.testdata.v1.Cases.Person;
import ai.pipestream.proto.validate.conformance.testdata.v1.Cases.Signup;
import build.buf.validate.FieldPath;
import build.buf.validate.FieldPathElement;
import build.buf.validate.FieldRules;
import build.buf.validate.Violation;
import buf.validate.conformance.harness.Harness.TestResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * In-build conformance harness (phase 1): drives {@link ConformanceRunner} over a curated set of
 * buf.validate-annotated cases and reports a pass-rate the same way the authoritative executor
 * would — comparing the structured {@code field} and {@code rule} paths, {@code rule_id} and
 * {@code for_key} of every violation.
 *
 * <p>These cases are ours, so the table is an internal rule-coverage signal, not the published buf
 * number; that comes from phase 2 driven by {@code protovalidate-conformance}. The gate is 100% —
 * a regression in the rule ids or paths our validator emits fails the build here.
 */
class ConformanceHarnessTest {

    private static final ConformanceRunner RUNNER = new ConformanceRunner();
    private static final Descriptor PERSON = Person.getDescriptor();
    private static final Descriptor SIGNUP = Signup.getDescriptor();

    private record Case(String name, Message message, TestResult expected) {
    }

    private static Person.Builder validPerson() {
        return Person.newBuilder().setAge(18).setName("ok").addTags("x");
    }

    private static List<Case> cases() {
        List<Case> cases = new ArrayList<>();
        cases.add(new Case("person_valid", validPerson().build(), success()));
        cases.add(new Case("person_age",
                validPerson().setAge(10).build(),
                invalid(viol(PERSON, "int32.gte", "age", false))));
        cases.add(new Case("person_name",
                validPerson().setName("a").build(),
                invalid(viol(PERSON, "string.min_len", "name", false))));
        cases.add(new Case("person_tags_empty",
                Person.newBuilder().setAge(18).setName("ok").build(),
                invalid(viol(PERSON, "repeated.min_items", "tags", false))));
        cases.add(new Case("person_codes_item",
                validPerson().addCodes("ab").build(),
                invalid(viol(PERSON, "string.min_len", "codes[0]", false))));
        cases.add(new Case("person_scores_value",
                validPerson().putScores("a", -1).build(),
                invalid(viol(PERSON, "int32.gte", "scores[\"a\"]", false))));
        cases.add(new Case("person_map_maxpairs",
                validPerson().putLimited("a", 1).putLimited("b", 2).putLimited("c", 3).build(),
                invalid(viol(PERSON, "map.max_pairs", "limited", false))));
        cases.add(new Case("signup_required",
                Signup.newBuilder().build(),
                invalid(viol(SIGNUP, "required", "person", false))));
        cases.add(new Case("signup_nested",
                Signup.newBuilder().setPerson(validPerson().setAge(10)).build(),
                invalid(viol(SIGNUP, "int32.gte", "person.age", false))));
        // Combined range (gt:0, lt:100) collapses to one gt_lt violation.
        cases.add(new Case("range_valid", validPerson().setRanged(50).build(), success()));
        cases.add(new Case("range_low",
                validPerson().setRanged(5).build(),
                invalid(viol(PERSON, "int32.gt_lt", "ranged", false))));
        cases.add(new Case("range_high",
                validPerson().setRanged(100).build(),
                invalid(viol(PERSON, "int32.gt_lt", "ranged", false))));
        // Reversed range (gt:100, lt:10): valid region is OUTSIDE [10,100], one _exclusive rule.
        cases.add(new Case("wrapped_below", validPerson().setWrapped(5).build(), success()));
        cases.add(new Case("wrapped_above", validPerson().setWrapped(200).build(), success()));
        cases.add(new Case("wrapped_inside",
                validPerson().setWrapped(50).build(),
                invalid(viol(PERSON, "int32.gt_lt_exclusive", "wrapped", false))));
        return cases;
    }

    @Test
    void curatedPassRate() {
        List<Case> cases = cases();
        int passed = 0;
        StringBuilder report = new StringBuilder("\nconformance pass-rate (curated, phase 1)\n");
        List<String> failures = new ArrayList<>();
        for (Case c : cases) {
            TestResult got = RUNNER.run(c.message);
            boolean ok = matches(c.expected, got);
            if (ok) {
                passed++;
            } else {
                failures.add(c.name);
            }
            report.append(String.format("  %-4s %-22s expected %s%n",
                    ok ? "PASS" : "FAIL", c.name, describe(c.expected)));
            if (!ok) {
                report.append(String.format("       %-22s got      %s%n", "", describe(got)));
            }
        }
        report.append(String.format("  %d/%d passed%n", passed, cases.size()));
        System.out.print(report);

        assertThat(failures)
                .as("curated conformance cases must all pass; see printed table")
                .isEmpty();
    }

    // ---- comparison: mirrors protovalidate-conformance non-strict IsSuccessWith ----

    private static boolean matches(TestResult want, TestResult got) {
        if (want.getResultCase() != got.getResultCase()) {
            return false;
        }
        if (want.getResultCase() == TestResult.ResultCase.SUCCESS) {
            return want.getSuccess() == got.getSuccess();
        }
        if (want.getResultCase() != TestResult.ResultCase.VALIDATION_ERROR) {
            return false;
        }
        List<Violation> w = sorted(want.getValidationError().getViolationsList());
        List<Violation> g = sorted(got.getValidationError().getViolationsList());
        if (w.size() != g.size()) {
            return false;
        }
        for (int i = 0; i < w.size(); i++) {
            if (!w.get(i).getField().equals(g.get(i).getField())
                    || w.get(i).getForKey() != g.get(i).getForKey()
                    || !w.get(i).getRule().equals(g.get(i).getRule())
                    || !w.get(i).getRuleId().equals(g.get(i).getRuleId())) {
                return false;
            }
        }
        return true;
    }

    private static List<Violation> sorted(List<Violation> violations) {
        List<Violation> copy = new ArrayList<>(violations);
        copy.sort(Comparator.comparing(Violation::getRuleId).thenComparing(v -> marshal(v.getField())));
        return copy;
    }

    private static TestResult success() {
        return TestResult.newBuilder().setSuccess(true).build();
    }

    private static TestResult invalid(Violation... violations) {
        build.buf.validate.Violations.Builder v = build.buf.validate.Violations.newBuilder();
        for (Violation violation : violations) {
            v.addViolations(violation);
        }
        return TestResult.newBuilder().setValidationError(v).build();
    }

    private static Violation viol(Descriptor root, String ruleId, String rawPath, boolean forKey) {
        Violation.Builder b = Violation.newBuilder().setRuleId(ruleId);
        if (forKey) {
            b.setForKey(true);
        }
        String prefix = "";
        if (!rawPath.isEmpty()) {
            FieldPath field = FieldPaths.unmarshal(root, rawPath);
            b.setField(field);
            // A rule directly on a repeated item / map entry takes the container's rule-path prefix.
            FieldPathElement last = field.getElements(field.getElementsCount() - 1);
            boolean containerLevel = ruleId.startsWith("repeated.") || ruleId.startsWith("map.");
            if (!containerLevel) {
                prefix = switch (last.getSubscriptCase()) {
                    case INDEX -> "repeated.items.";
                    case BOOL_KEY, INT_KEY, UINT_KEY, STRING_KEY -> forKey ? "map.keys." : "map.values.";
                    default -> "";
                };
            }
        }
        b.setRule(FieldPaths.unmarshal(
                FieldRules.getDescriptor(), prefix + ConformanceRunner.rulePath(ruleId)));
        return b.build();
    }

    // ---- diagnostics ----

    private static String describe(TestResult r) {
        return switch (r.getResultCase()) {
            case SUCCESS -> "valid";
            case VALIDATION_ERROR -> {
                List<String> parts = new ArrayList<>();
                for (Violation v : sorted(r.getValidationError().getViolationsList())) {
                    parts.add(v.getRuleId() + "@" + marshal(v.getField()) + (v.getForKey() ? "#key" : ""));
                }
                yield "invalid " + parts;
            }
            case COMPILATION_ERROR -> "compilation_error: " + r.getCompilationError();
            case RUNTIME_ERROR -> "runtime_error: " + r.getRuntimeError();
            case UNEXPECTED_ERROR -> "unexpected_error: " + r.getUnexpectedError();
            case RESULT_NOT_SET -> "<unset>";
        };
    }

    private static String marshal(FieldPath path) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.getElementsCount(); i++) {
            FieldPathElement e = path.getElements(i);
            if (i > 0) {
                sb.append('.');
            }
            sb.append(e.getFieldName());
            switch (e.getSubscriptCase()) {
                case INDEX -> sb.append('[').append(e.getIndex()).append(']');
                case BOOL_KEY -> sb.append('[').append(e.getBoolKey()).append(']');
                case INT_KEY -> sb.append('[').append(e.getIntKey()).append(']');
                case UINT_KEY -> sb.append('[').append(e.getUintKey()).append(']');
                case STRING_KEY -> sb.append("[\"").append(e.getStringKey()).append("\"]");
                case SUBSCRIPT_NOT_SET -> {
                }
            }
        }
        return sb.toString();
    }
}
