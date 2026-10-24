package ai.protomolt.proto.formats;

import java.util.Arrays;
import java.util.Optional;

/**
 * The {@code link_relation} format: the relation a typed link between documents declares. A
 * relation is a Dublin Core relation term, a name registered in the IANA Link Relations
 * registry, or an absolute URI, the RFC 8288 form of an extension relation type.
 *
 * <p>Both vocabularies are bundled as sorted tables and matched exactly, character by
 * character: the stored form of a relation is its registered spelling, so {@code next} passes
 * and {@code Next} refuses (unless it parses as a URI, which a bare word does not). Dublin Core
 * terms keep their camel case ({@code isPartOf}). Lookups are binary searches over the tables;
 * the URI fallback is the {@link Rfc3986} scanner. No regular expressions.
 *
 * <p>Sources, attributed in {@code NOTICE}:
 * <ul>
 *   <li>DCMI Metadata Terms, the relation terms of the {@code http://purl.org/dc/terms/}
 *       namespace, CC-BY 4.0, Dublin Core Metadata Initiative.</li>
 *   <li>IANA Link Relations registry ({@code link-relations-1.csv}), the registry as updated
 *       2026-06-12.</li>
 * </ul>
 */
final class LinkRelations {

    /** DCMI Metadata Terms relation terms, sorted by {@link String#compareTo}. */
    static final String[] DUBLIN_CORE = {
            "conformsTo", "hasFormat", "hasPart", "hasVersion", "isFormatOf", "isPartOf",
            "isReferencedBy", "isReplacedBy", "isRequiredBy", "isVersionOf", "references",
            "relation", "replaces", "requires", "source",
    };

    /**
     * Every relation name in the IANA Link Relations registry, sorted by
     * {@link String#compareTo}.
     */
    static final String[] IANA = {
            "about", "acl", "alternate", "amphtml", "api-catalog", "appendix",
            "apple-touch-icon", "apple-touch-startup-image", "archives", "author",
            "blocked-by", "bookmark", "c2pa-manifest", "canonical", "chapter", "cite-as",
            "collection", "compression-dictionary", "contents", "convertedfrom",
            "copyright", "create-form", "current", "deprecation", "describedby",
            "describes", "disclosure", "dns-prefetch", "dpp", "duplicate", "edit",
            "edit-form", "edit-media", "enclosure", "external", "first", "geofeed",
            "glossary", "help", "hosts", "hub", "ice-server", "icon", "index",
            "intervalafter", "intervalbefore", "intervalcontains", "intervaldisjoint",
            "intervalduring", "intervalequals", "intervalfinishedby", "intervalfinishes",
            "intervalin", "intervalmeets", "intervalmetby", "intervaloverlappedby",
            "intervaloverlaps", "intervalstartedby", "intervalstarts", "item", "last",
            "latest-version", "license", "linkset", "lrdd", "manifest", "mask-icon", "me",
            "media-feed", "memento", "micropub", "modulepreload", "monitor",
            "monitor-group", "next", "next-archive", "nofollow", "noopener", "noreferrer",
            "opener", "openid2.local_id", "openid2.provider", "original", "p3pv1",
            "payment", "pingback", "preconnect", "predecessor-version", "prefetch",
            "preload", "prerender", "prev", "prev-archive", "preview", "previous",
            "privacy-policy", "profile", "publication", "rdap-active", "rdap-bottom",
            "rdap-down", "rdap-top", "rdap-up", "related", "replies", "restconf",
            "ruleinput", "search", "section", "self", "service", "service-desc",
            "service-doc", "service-meta", "sip-trunking-capability", "sponsored", "start",
            "status", "stylesheet", "subsection", "successor-version", "sunset", "tag",
            "terms-of-service", "timegate", "timemap", "type", "ugc", "up",
            "version-history", "via", "webmention", "working-copy", "working-copy-of",
    };

    private LinkRelations() {
    }

    /**
     * Whether the value is a link relation: a bundled Dublin Core term, a bundled IANA
     * registered name, or an http(s) URI naming an extension relation. Other URI schemes
     * refuse: a relation becomes an href in renderers, so schemes with side effects
     * (javascript, data, vbscript, file) never pass, and locator-less schemes (urn, tag)
     * are not accepted as extension relations either.
     */
    static boolean isLinkRelation(String value) {
        if (value.isEmpty()) {
            return false;
        }
        return isDublinCore(value) || isRegistered(value) || isHttpExtension(value);
    }

    /**
     * An RFC 8288 extension relation this format accepts: an absolute URI whose scheme is
     * http or https (case-insensitive, RFC 3986 schemes compare case-insensitively) with a
     * non-empty authority host.
     */
    private static boolean isHttpExtension(String value) {
        int colon = value.indexOf(':');
        if (colon < 0) {
            return false;
        }
        String scheme = value.substring(0, colon);
        if (!(asciiEqualsIgnoreCase(scheme, "http") || asciiEqualsIgnoreCase(scheme, "https"))) {
            return false;
        }
        return hasAuthorityHost(value.substring(colon + 1)) && Rfc3986.isUri(value);
    }

    private static boolean hasAuthorityHost(String rest) {
        if (!rest.startsWith("//")) {
            return false;
        }
        String afterSlashes = rest.substring(2);
        int end = afterSlashes.length();
        for (int i = 0; i < end; i++) {
            char c = afterSlashes.charAt(i);
            if (c == '/' || c == '?' || c == '#') {
                end = i;
                break;
            }
        }
        String authority = afterSlashes.substring(0, end);
        int at = authority.indexOf('@');
        String hostPort = at < 0 ? authority : authority.substring(at + 1);
        if (hostPort.startsWith("[")) {
            return hostPort.indexOf(']') > 1;
        }
        int portColon = hostPort.indexOf(':');
        String host = portColon < 0 ? hostPort : hostPort.substring(0, portColon);
        return !host.isEmpty(); // an empty host is nothing to link to
    }

    /** Whether the value is one of the bundled Dublin Core relation terms. */
    static boolean isDublinCore(String value) {
        return Arrays.binarySearch(DUBLIN_CORE, value) >= 0;
    }

    /** Whether the value is a name in the bundled IANA Link Relations registry. */
    static boolean isRegistered(String value) {
        return Arrays.binarySearch(IANA, value) >= 0;
    }

    /** The path portion of a Dublin Core term URI, matched case-sensitively. */
    private static final String DUBLIN_CORE_PATH = "/dc/terms/";

    /** The namespace prefixes a Dublin Core term may carry in its URI form. */
    private static final String[] DUBLIN_CORE_PREFIXES = {
            "http://purl.org/dc/terms/", "https://purl.org/dc/terms/",
    };

    /**
     * The Dublin Core inverse of a relation: {@code isPartOf} and {@code hasPart},
     * {@code replaces} and {@code isReplacedBy}, {@code references} and
     * {@code isReferencedBy}, {@code requires} and {@code isRequiredBy}, {@code hasFormat}
     * and {@code isFormatOf}, {@code hasVersion} and {@code isVersionOf}. A term in its
     * {@code http://purl.org/dc/terms/} or {@code https://purl.org/dc/terms/} URI form
     * (scheme and {@code purl.org} authority matched case-insensitively per RFC 3986)
     * answers its inverse in the same prefix form. Empty for every other value, including
     * Dublin Core terms with no paired inverse ({@code source}, {@code relation},
     * {@code conformsTo}).
     */
    static Optional<String> inverse(String value) {
        for (String prefix : DUBLIN_CORE_PREFIXES) {
            if (hasDublinCorePrefix(value, prefix)) {
                return bareInverse(value.substring(prefix.length())).map(name -> prefix + name);
            }
        }
        return bareInverse(value);
    }

    private static boolean hasDublinCorePrefix(String value, String prefix) {
        if (value.length() <= prefix.length()) {
            return false;
        }
        int pathStart = prefix.length() - DUBLIN_CORE_PATH.length();
        return asciiEqualsIgnoreCase(value.substring(0, pathStart), prefix.substring(0, pathStart))
                && value.startsWith(DUBLIN_CORE_PATH, pathStart);
    }

    private static boolean asciiEqualsIgnoreCase(String value, String lowerAscii) {
        if (value.length() != lowerAscii.length()) {
            return false;
        }
        for (int i = 0; i < lowerAscii.length(); i++) {
            char c = value.charAt(i);
            char folded = c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c;
            if (folded != lowerAscii.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static Optional<String> bareInverse(String value) {
        return Optional.ofNullable(switch (value) {
            case "isPartOf" -> "hasPart";
            case "hasPart" -> "isPartOf";
            case "replaces" -> "isReplacedBy";
            case "isReplacedBy" -> "replaces";
            case "references" -> "isReferencedBy";
            case "isReferencedBy" -> "references";
            case "requires" -> "isRequiredBy";
            case "isRequiredBy" -> "requires";
            case "hasFormat" -> "isFormatOf";
            case "isFormatOf" -> "hasFormat";
            case "hasVersion" -> "isVersionOf";
            case "isVersionOf" -> "hasVersion";
            default -> null;
        });
    }
}
