package ai.pipestream.proto.account.service.identity;

import java.util.Optional;

/**
 * The default {@link IdentityResolver}: a pass-through for direct account
 * ids. A principal whose (blank or {@code "account-id"}) identity type marks
 * it AS a platform account id resolves to itself; every other identity type
 * is unknown to this resolver and resolves to empty.
 * <p>
 * This is the stand-in, not the goal: Salesforce/AD/OAuth adapters replace
 * it, mapping their principals onto account ids — likely backed by lookup
 * tables in the account store. Nothing is persisted here because there is
 * nothing to persist yet.
 */
public final class DirectAccountIdentityResolver implements IdentityResolver {

    /** The identity type tag meaning "the identity IS an account id". */
    public static final String ACCOUNT_ID_TYPE = "account-id";

    @Override
    public Optional<String> resolveAccountId(String identity, String identityType) {
        if (identity == null || identity.isBlank()) {
            return Optional.empty();
        }
        if (identityType == null || identityType.isBlank()
                || ACCOUNT_ID_TYPE.equals(identityType.trim())) {
            return Optional.of(identity.trim());
        }
        return Optional.empty();
    }
}
