package ai.protomolt.proto.actions;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Which ledger a catalog spends on is part of its contract, because a node whose surfaces
 * each hold a private ledger gives one principal a separate allowance per surface while
 * appearing to enforce the budget. The single-argument factory is documented to build a
 * private ledger, for a catalog that is the node's only enforcement point; the
 * two-argument factory takes the ledger the rest of the node already shares.
 */
class CatalogLedgerBoundaryTest {

    @Test
    void catalogsBuiltWithoutALedgerDoNotShareOne() {
        ActionContext context = ActionContext.create();

        ActionCatalog first = ActionCatalog.defaults(context);
        ActionCatalog second = ActionCatalog.defaults(context);

        assertThat(first.budgets()).isNotSameAs(second.budgets());
    }

    @Test
    void catalogsGivenALedgerSpendOnThatOne() {
        ActionContext context = ActionContext.create();
        ScopeBudgets shared = new ScopeBudgets();

        ActionCatalog first = ActionCatalog.defaults(context, shared);
        ActionCatalog second = ActionCatalog.defaults(context, shared);

        assertThat(first.budgets()).isSameAs(shared);
        assertThat(second.budgets()).isSameAs(shared);
    }
}
