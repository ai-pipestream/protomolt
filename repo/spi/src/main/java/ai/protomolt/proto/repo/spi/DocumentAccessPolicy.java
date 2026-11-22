package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.AccessRuleSet;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Current document ACL evaluation; callers must separately guard policy revision at commit. */
public final class DocumentAccessPolicy {
    private DocumentAccessPolicy() {}

    /** Null inherited rules mean unresolved; an empty list means resolved with no inherited rules. */
    public static boolean allows(RepositoryCaller caller, String owningAccount,
            DocumentSecurity security, List<AccessRule> inherited, Access requested) {
        Objects.requireNonNull(caller, "caller");
        if (requested != Access.ACCESS_READ && requested != Access.ACCESS_WRITE)
            throw new IllegalArgumentException("Access check must request READ or WRITE");
        if (owningAccount == null || owningAccount.isBlank())
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Document ownership is missing");
        if (!caller.processAuthority() && !caller.accountIds().contains(owningAccount)) return false;
        if (security == null) return caller.processAuthority();
        if (!security.getUnknownFields().asMap().isEmpty())
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Document policy contains unknown fields");

        var rules = new ArrayList<>(security.getPermissionsList());
        if (security.getInheritanceEnabled()) {
            if (inherited == null)
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Inherited document policy is unresolved");
            rules.addAll(inherited);
        }
        for (AccessRule rule : rules) requireValid(rule);
        for (AccessRuleSet set : security.getRequiredRuleSetsList()) {
            if (!set.getUnknownFields().asMap().isEmpty())
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Document policy contains unknown fields");
            for (AccessRule rule : set.getRulesList()) requireValid(rule);
        }
        // Process authority is an explicit administrative bypass, never an inferred ACL identity.
        if (caller.processAuthority()) return true;
        Grant grant = evaluate(caller, rules, requested);
        if (grant != Grant.ALLOW) return false;
        for (AccessRuleSet set : security.getRequiredRuleSetsList()) {
            if (evaluate(caller, set.getRulesList(), requested) != Grant.ALLOW) return false;
        }
        return true;
    }

    private enum Grant { ALLOW, NONE, DENY }

    private static Grant evaluate(RepositoryCaller caller, List<AccessRule> rules, Access requested) {
        boolean allowed = false;
        for (AccessRule rule : rules) {
            boolean matches = rule.getIdentityType().equalsIgnoreCase("public")
                    || caller.identities().stream().anyMatch(identity ->
                        identity.getIdentityType().equalsIgnoreCase(rule.getIdentityType())
                        && identity.getIdentity().equalsIgnoreCase(rule.getIdentity()));
            if (!matches) continue;
            if (rule.getAccess() == Access.ACCESS_DENY) return Grant.DENY;
            if (rule.getAccess() == requested) allowed = true;
        }
        return allowed ? Grant.ALLOW : Grant.NONE;
    }

    private static void requireValid(AccessRule rule) {
        if (rule == null || rule.getIdentity().isBlank() || rule.getIdentityType().isBlank()
                || !rule.getUnknownFields().asMap().isEmpty()
                || rule.getAccess() == Access.ACCESS_UNSPECIFIED || rule.getAccess() == Access.UNRECOGNIZED
                || (rule.getIdentityType().equalsIgnoreCase("public")
                    && !rule.getIdentity().equalsIgnoreCase("public"))) {
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Document policy contains a malformed access rule");
        }
    }
}
