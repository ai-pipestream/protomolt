package ai.protomolt.proto.account.service.identity;

import java.util.Optional;

/**
 * The IdentityResolver SPI — the seam where external identity systems
 * (Salesforce, Active Directory, OAuth providers) will map their principals
 * onto platform accounts.
 * <p>
 * Deliberately minimal for now: one lookup, in, out. Typed principals and
 * group membership arrive with the ACL proto; until then no RPC consumes
 * this interface — it exists so the adapter boundary is designed and tested
 * before the first real adapter lands.
 */
public interface IdentityResolver {

    /**
     * Resolve an external principal to the platform account it belongs to.
     *
     * @param identity the principal's identifier within its system (a SID,
     *        email, subject claim, or — for the default pass-through — an
     *        account id)
     * @param identityType the principal's type tag ("windows-sid",
     *        "user-principal-name", "account-id", …); blank means the
     *        resolver's own default interpretation
     * @return the account id, or empty when the principal has no account
     */
    Optional<String> resolveAccountId(String identity, String identityType);
}
