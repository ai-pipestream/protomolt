package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.repo.v1.DocumentSecurity;
import java.util.Objects;

/**
 * What a ledger save does with the ledger's access rules. A source's rules with no explicit
 * grant are still rules (everyone they named has been revoked), so "the source has no rules to
 * give" is its own case and never inferred from an empty rule set.
 *
 * @param kind where the rules come from
 * @param rules the rules to write; empty for {@link Kind#KEEP}
 */
record LedgerAccess(Kind kind, DocumentSecurity rules) {

    /** Where the rules a ledger save writes come from. */
    enum Kind {
        /** The source's live revision: written exactly, unless a purge preparation pinned the ledger. */
        LIVE_SOURCE,
        /** A purge preparation: written exactly, and pinned for every later save. */
        PURGE_PREPARATION,
        /** No live rules are available (deleted or purged source): the ledger keeps its own. */
        KEEP
    }

    LedgerAccess {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(rules, "rules");
    }

    static LedgerAccess liveSource(DocumentSecurity rules) {
        return new LedgerAccess(Kind.LIVE_SOURCE, rules);
    }

    static LedgerAccess purgePreparation(DocumentSecurity rules) {
        return new LedgerAccess(Kind.PURGE_PREPARATION, rules);
    }

    static LedgerAccess keep() {
        return new LedgerAccess(Kind.KEEP, DocumentSecurity.getDefaultInstance());
    }
}
