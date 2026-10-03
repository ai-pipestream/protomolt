package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Current document ACL evaluation; callers must separately guard policy revision at commit. */
final class DocumentAccessPolicy {
    private DocumentAccessPolicy() {}

    /** Null inherited rules mean unresolved; an empty list means resolved with no inherited rules. */
    static boolean allows(RepositoryCaller caller, String owningAccount,
            DocumentSecurity security, List<AccessRule> inherited, Access requested) {
        Objects.requireNonNull(caller, "caller");
        if (requested != Access.ACCESS_READ && requested != Access.ACCESS_WRITE)
            throw new IllegalArgumentException("Access check must request READ or WRITE");
        if (owningAccount == null || owningAccount.isBlank())
            throw RepositoryErrors.failedPrecondition("Document ownership is missing");
        if (!caller.processAuthority() && !caller.accountIds().contains(owningAccount)) return false;
        if (security == null) return caller.processAuthority();
        if (!security.getUnknownFields().asMap().isEmpty())
            throw RepositoryErrors.failedPrecondition("Document policy contains unknown fields");

        var rules = new ArrayList<>(security.getPermissionsList());
        if (security.getInheritanceEnabled()) {
            if (inherited == null)
                throw RepositoryErrors.failedPrecondition("Inherited document policy is unresolved");
            rules.addAll(inherited);
        }
        for (AccessRule rule : rules) requireValid(rule);
        // Process authority is an explicit administrative bypass, never an inferred ACL identity.
        if (caller.processAuthority()) return true;
        boolean allowed = false;
        for (AccessRule rule : rules) {
            boolean matches = rule.getIdentityType().equalsIgnoreCase("public")
                    || caller.identities().stream().anyMatch(identity ->
                        identity.getIdentityType().equalsIgnoreCase(rule.getIdentityType())
                        && identity.getIdentity().equalsIgnoreCase(rule.getIdentity()));
            if (!matches) continue;
            if (rule.getAccess() == Access.ACCESS_DENY) return false;
            if (rule.getAccess() == requested) allowed = true;
        }
        return allowed;
    }

    private static void requireValid(AccessRule rule) {
        if (rule == null || rule.getIdentity().isBlank() || rule.getIdentityType().isBlank()
                || !rule.getUnknownFields().asMap().isEmpty()
                || rule.getAccess() == Access.ACCESS_UNSPECIFIED || rule.getAccess() == Access.UNRECOGNIZED
                || (rule.getIdentityType().equalsIgnoreCase("public")
                    && !rule.getIdentity().equalsIgnoreCase("public"))) {
            throw RepositoryErrors.failedPrecondition("Document policy contains a malformed access rule");
        }
    }
}
