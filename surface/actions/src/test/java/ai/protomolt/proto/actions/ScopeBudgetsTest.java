package ai.protomolt.proto.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.Caller.Budget;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The spending ledger: rate is a fixed one-minute window per principal and scope where
 * admitted requests count and refused ones do not, payload refusals spend nothing, an
 * unbudgeted scope passes untouched, and refusals name the principal, scope, and limit.
 */
class ScopeBudgetsTest {

    private static final Caller BUDGETED = Caller.scoped("meterme",
            Set.of(Scopes.SCHEMA_READ, Scopes.SEARCH_QUERY),
            Map.of(Scopes.SCHEMA_READ, new Budget(2, 100)));

    @Test
    void admittedRequestsCountAndTheWindowRefusesByName() {
        AtomicLong now = new AtomicLong(0);
        ScopeBudgets budgets = new ScopeBudgets(now::get);
        assertThat(budgets.refuse(BUDGETED, Scopes.SCHEMA_READ, 10)).isEmpty();
        assertThat(budgets.refuse(BUDGETED, Scopes.SCHEMA_READ, 10)).isEmpty();
        assertThat(budgets.refuse(BUDGETED, Scopes.SCHEMA_READ, 10).orElseThrow())
                .contains("meterme").contains("2-per-minute").contains(Scopes.SCHEMA_READ);
        // The refusal spent nothing, and the window resets after a minute.
        now.set(60_000);
        assertThat(budgets.refuse(BUDGETED, Scopes.SCHEMA_READ, 10)).isEmpty();
    }

    @Test
    void anUnbudgetedScopeAndTheOperatorPassUntouched() {
        ScopeBudgets budgets = new ScopeBudgets(() -> 0);
        for (int i = 0; i < 10; i++) {
            assertThat(budgets.refuse(BUDGETED, Scopes.SEARCH_QUERY, 1_000_000)).isEmpty();
            assertThat(budgets.refuse(Caller.operator(), Scopes.SCHEMA_READ, 1_000_000))
                    .isEmpty();
        }
    }

    @Test
    void anOversizePayloadRefusesWithoutSpendingTheRate() {
        ScopeBudgets budgets = new ScopeBudgets(() -> 0);
        assertThat(budgets.refuse(BUDGETED, Scopes.SCHEMA_READ, 101).orElseThrow())
                .contains("meterme").contains("101").contains("100-byte")
                .contains(Scopes.SCHEMA_READ);
        // Two admissions remain: the oversize refusal did not count.
        assertThat(budgets.refuse(BUDGETED, Scopes.SCHEMA_READ, 100)).isEmpty();
        assertThat(budgets.refuse(BUDGETED, Scopes.SCHEMA_READ, -1)).isEmpty();
        assertThat(budgets.refuse(BUDGETED, Scopes.SCHEMA_READ, 10)).isPresent();
    }

    @Test
    void aPayloadOnlyBudgetNeverRateLimits() {
        Caller capped = Caller.scoped("capped", Set.of(Scopes.SCHEMA_READ),
                Map.of(Scopes.SCHEMA_READ, new Budget(0, 50)));
        ScopeBudgets budgets = new ScopeBudgets(() -> 0);
        for (int i = 0; i < 10; i++) {
            assertThat(budgets.refuse(capped, Scopes.SCHEMA_READ, 50)).isEmpty();
        }
        assertThat(budgets.refuse(capped, Scopes.SCHEMA_READ, 51)).isPresent();
    }

    @Test
    void everyPrincipalAndScopePairGetsItsOwnWindow() {
        // The key joins the principal name and the scope over a NUL separator. Both halves
        // are load-bearing: two principals on one scope, and one principal on two scopes,
        // are four windows, not one shared allowance and not two.
        Map<String, Budget> two = Map.of(
                Scopes.SCHEMA_READ, new Budget(1, 0),
                Scopes.SEARCH_QUERY, new Budget(1, 0));
        Caller first = Caller.scoped("alice",
                Set.of(Scopes.SCHEMA_READ, Scopes.SEARCH_QUERY), two);
        Caller second = Caller.scoped("alicia",
                Set.of(Scopes.SCHEMA_READ, Scopes.SEARCH_QUERY), two);
        ScopeBudgets budgets = new ScopeBudgets(() -> 0);

        assertThat(budgets.refuse(first, Scopes.SCHEMA_READ, -1)).isEmpty();
        assertThat(budgets.refuse(first, Scopes.SEARCH_QUERY, -1)).isEmpty();
        assertThat(budgets.refuse(second, Scopes.SCHEMA_READ, -1)).isEmpty();
        assertThat(budgets.refuse(second, Scopes.SEARCH_QUERY, -1)).isEmpty();

        // Each of the four is now exhausted on its own.
        assertThat(budgets.refuse(first, Scopes.SCHEMA_READ, -1)).isPresent();
        assertThat(budgets.refuse(first, Scopes.SEARCH_QUERY, -1)).isPresent();
        assertThat(budgets.refuse(second, Scopes.SCHEMA_READ, -1)).isPresent();
        assertThat(budgets.refuse(second, Scopes.SEARCH_QUERY, -1)).isPresent();
    }

    @Test
    void budgetsValidateAtConstruction() {
        assertThatThrownBy(() -> new Budget(0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must cap something");
        assertThatThrownBy(() -> new Budget(-1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Caller.scoped("who", Set.of(Scopes.SCHEMA_READ),
                Map.of(Scopes.SEARCH_QUERY, new Budget(1, 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not one the caller holds");
        assertThatThrownBy(() -> new Caller("op", Set.of(), true,
                Map.of(Scopes.SCHEMA_READ, new Budget(1, 0))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("never budgeted");
    }
}
