package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.AccessRuleSet;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.Principal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * A document's read access as search terms, and a caller's identities as the terms they match.
 * A caller may read a document when its terms hit {@link #allow()}, miss {@link #deny()}, and
 * hit the {@code allow} of every gate, the same answer {@link DocumentAccessPolicy} gives for
 * READ. Terms are {@code identity_type:identity}, lower-cased.
 *
 * @param allow terms of the READ rules
 * @param deny terms of every DENY rule, in the rules or in any required set
 * @param gates one entry per required rule set
 */
public record DocumentSecurityTerms(List<String> allow, List<String> deny, List<Gate> gates) {

    /** The term every caller holds, matching rules of identity type {@code public}. */
    public static final String PUBLIC = "public:public";

    /**
     * One required rule set.
     *
     * @param source where the set comes from
     * @param allow terms of the set's READ rules
     */
    public record Gate(String source, List<String> allow) {
        public Gate {
            Objects.requireNonNull(source, "source");
            allow = List.copyOf(allow);
        }
    }

    public DocumentSecurityTerms {
        allow = List.copyOf(allow);
        deny = List.copyOf(deny);
        gates = List.copyOf(gates);
    }

    /**
     * The terms of one document's security.
     *
     * @param security the document's security
     * @param inherited the inherited rules, or null when unresolved (refused if inheritance is on)
     * @return the terms
     */
    public static DocumentSecurityTerms of(DocumentSecurity security, List<AccessRule> inherited) {
        Objects.requireNonNull(security, "security");
        List<AccessRule> rules = DocumentAccessPolicy.validatedRules(security, inherited);
        Set<String> deny = new LinkedHashSet<>();
        Set<String> allow = new LinkedHashSet<>();
        collect(rules, allow, deny);
        List<Gate> gates = new ArrayList<>();
        for (AccessRuleSet set : security.getRequiredRuleSetsList()) {
            Set<String> gateAllow = new LinkedHashSet<>();
            collect(set.getRulesList(), gateAllow, deny);
            gates.add(new Gate(set.getSource(), List.copyOf(gateAllow)));
        }
        return new DocumentSecurityTerms(List.copyOf(allow), List.copyOf(deny), gates);
    }

    /**
     * The terms a caller matches: each of its identities, plus {@link #PUBLIC}.
     *
     * @param identities the caller's identities, already expanded
     * @return the terms
     */
    public static Set<String> callerTerms(Set<Principal> identities) {
        Set<String> terms = new LinkedHashSet<>();
        for (Principal identity : identities) terms.add(term(identity.getIdentityType(), identity.getIdentity()));
        terms.add(PUBLIC);
        return terms;
    }

    /**
     * Whether a caller holding {@code terms} may read the document.
     *
     * @param terms the caller's terms
     * @return true when the terms hit allow and every gate and miss deny
     */
    public boolean reads(Set<String> terms) {
        if (deny.stream().anyMatch(terms::contains)) return false;
        if (allow.stream().noneMatch(terms::contains)) return false;
        return gates.stream().allMatch(gate -> gate.allow().stream().anyMatch(terms::contains));
    }

    private static void collect(List<AccessRule> rules, Set<String> allow, Set<String> deny) {
        for (AccessRule rule : rules) {
            String term = term(rule.getIdentityType(), rule.getIdentity());
            if (rule.getAccess() == Access.ACCESS_DENY) deny.add(term);
            else if (rule.getAccess() == Access.ACCESS_READ) allow.add(term);
        }
    }

    private static String term(String type, String identity) {
        return (type + ":" + identity).toLowerCase(Locale.ROOT);
    }
}
