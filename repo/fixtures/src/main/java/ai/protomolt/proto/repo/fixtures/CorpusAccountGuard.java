package ai.protomolt.proto.repo.fixtures;

import ai.protomolt.proto.repo.accession.RepositoryCorpus;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Decides whether the seeder may write the corpus into an account, from the
 * documents the account already holds. An account holding documents the corpus
 * did not create is refused by name: the seeder never writes next to someone
 * else's data. An account that already holds corpus documents and nothing else
 * is reported as seeded and left alone. An empty account is written.
 */
public final class CorpusAccountGuard {

    /** The decision for one account. */
    public enum Decision {
        /** The account holds no documents: write the corpus. */
        WRITE,
        /** The account holds corpus documents and nothing else: nothing to do. */
        SEEDED
    }

    /** The account holds documents the corpus did not create. */
    public static final class ForeignDocuments extends IllegalStateException {
        private final String accountId;
        private final List<String> documentIds;

        ForeignDocuments(String accountId, List<String> documentIds) {
            super("Account " + accountId + " holds " + documentIds.size() + " document(s) the corpus did not create: "
                    + String.join(", ", documentIds) + ". The seeder writes only into accounts it owns.");
            this.accountId = accountId;
            this.documentIds = documentIds;
        }

        public String accountId() {
            return accountId;
        }

        public List<String> documentIds() {
            return documentIds;
        }
    }

    private CorpusAccountGuard() {}

    /**
     * Checks one account.
     *
     * @param corpus the corpus about to be written
     * @param accountId the account
     * @param existing the addresses of every document the account holds, tombstones included
     * @return whether to write or leave the account
     * @throws ForeignDocuments when the account holds a document the corpus does not write
     */
    public static Decision check(RepositoryCorpus corpus, String accountId, List<NodeAddress> existing) {
        Set<NodeAddress> owned = Set.copyOf(CorpusSeeder.addresses(corpus, accountId));
        var foreign = new TreeSet<String>();
        var seeded = new HashSet<NodeAddress>();
        for (var address : existing) {
            if (!address.getAccountId().equals(accountId)) {
                throw new IllegalArgumentException("Listing of " + accountId + " holds a document of " + address.getAccountId());
            }
            var id = address.getDocId();
            if (owned.contains(address)) seeded.add(address);
            else foreign.add(address.getGraphId() + "/" + address.getGraphAddressId() + "/" + id);
        }
        if (!foreign.isEmpty()) throw new ForeignDocuments(accountId, List.copyOf(foreign));
        if (seeded.isEmpty()) return Decision.WRITE;
        if (seeded.size() != owned.size()) {
            throw new IllegalStateException("Account " + accountId + " holds " + seeded.size() + " of the corpus's "
                    + owned.size() + " documents; a partial load is not resumed, clear the account first");
        }
        return Decision.SEEDED;
    }
}
