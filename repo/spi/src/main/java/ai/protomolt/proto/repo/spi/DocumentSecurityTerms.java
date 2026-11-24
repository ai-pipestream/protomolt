package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.AccessRuleSet;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.Principal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A document's read access as search terms, and a caller's identities as the terms they match.
 * A caller may read a document when its terms hit {@link #allow()}, miss {@link #deny()}, and
 * hit the {@code allow} of every gate. For a caller without process authority in the document's
 * owning account, that is the answer {@link DocumentAccessPolicy} gives for READ. The terms carry
 * no account: a search index must also filter on the owning account. A document without
 * security is readable only with process authority and has no terms. Terms are {@code encoded_identity_type:identity} using the case folding of {@link String#equalsIgnoreCase(String)}, where
 * {@code %} and {@code :} inside {@code identity_type} are percent-encoded ({@code %25},
 * {@code %3a}) so the first {@code :} unambiguously separates type from identity.
 *
 * @param allow terms of the READ rules
 * @param deny terms of every DENY rule, in the rules or in any required set
 * @param gates one entry per required rule set
 */
public record DocumentSecurityTerms(List<String> allow, List<String> deny, List<Gate> gates) {

    /** The term every caller holds, matching rules of identity type {@code public}. */
    public static final String PUBLIC = "public:public";

    private static final char TERM_SEPARATOR = ':';
    private static final String PERCENT_ESCAPE = "%25";
    private static final String COLON_ESCAPE = "%3a";

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
     * The terms a caller matches: each of its validated identities, plus {@link #PUBLIC}.
     *
     * @param identities the caller's identities, already expanded
     * @return the terms
     */
    public static Set<String> callerTerms(Set<Principal> identities) {
        Objects.requireNonNull(identities, "identities");
        Set<String> terms = new LinkedHashSet<>();
        for (Principal identity : identities) {
            requireValidPrincipal(identity);
            terms.add(term(identity.getIdentityType(), identity.getIdentity()));
        }
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
        Objects.requireNonNull(terms, "terms");
        if (deny.stream().anyMatch(terms::contains)) {
            return false;
        }
        if (allow.stream().noneMatch(terms::contains)) {
            return false;
        }
        return gates.stream().allMatch(gate -> gate.allow().stream().anyMatch(terms::contains));
    }

    private static void collect(List<AccessRule> rules, Set<String> allow, Set<String> deny) {
        for (AccessRule rule : rules) {
            String term = term(rule.getIdentityType(), rule.getIdentity());
            if (rule.getAccess() == Access.ACCESS_DENY) {
                deny.add(term);
            } else if (rule.getAccess() == Access.ACCESS_READ) {
                allow.add(term);
            }
        }
    }

    private static String term(String type, String identity) {
        AclIdentityKey key = new AclIdentityKey(type, identity);
        return encodeIdentityType(key.type()) + TERM_SEPARATOR + key.identity();
    }

    private static String encodeIdentityType(String lowerType) {
        if (lowerType.indexOf('%') < 0 && lowerType.indexOf(TERM_SEPARATOR) < 0) {
            return lowerType;
        }
        StringBuilder encoded = new StringBuilder(lowerType.length() + 4);
        for (int i = 0; i < lowerType.length(); i++) {
            char ch = lowerType.charAt(i);
            if (ch == '%') {
                encoded.append(PERCENT_ESCAPE);
            } else if (ch == TERM_SEPARATOR) {
                encoded.append(COLON_ESCAPE);
            } else {
                encoded.append(ch);
            }
        }
        return encoded.toString();
    }

    private static void requireValidPrincipal(Principal identity) {
        if (identity == null || identity.getIdentity().isBlank() || identity.getIdentityType().isBlank()
                || !identity.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("ACL identity and identity type are required");
        }
    }
}
