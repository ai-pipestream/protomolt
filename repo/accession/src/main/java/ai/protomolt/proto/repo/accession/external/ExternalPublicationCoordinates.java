package ai.protomolt.proto.repo.accession.external;

import ai.protomolt.proto.formats.Formats;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Deterministic repository coordinates, collision-free binary source keys,
 * circular-publication graph predicates, and non-regex display URL / label
 * sanitizers for external-system bindings and publication ledgers.
 */
public final class ExternalPublicationCoordinates {

    /** Graph prefix reserved for account-scoped versioned {@code ExternalSystemBinding} documents. */
    public static final String BINDINGS_GRAPH_PREFIX = "external-bindings:";

    /** Graph address coordinate used inside {@code external-bindings:<account_id>}. */
    public static final String BINDINGS_GRAPH_ADDRESS_ID = "bindings";

    /** Graph prefix reserved for account-scoped {@code ExternalPublicationLedger} documents. */
    public static final String PUBLICATIONS_GRAPH_PREFIX = "external-publications:";

    /** Default intake datasource ID used when persisting binding documents without a step owner. */
    public static final String DEFAULT_BINDINGS_DATASOURCE_ID = "ds-external-bindings";

    /** Default intake datasource ID used when persisting publication ledgers without a step owner. */
    public static final String DEFAULT_PUBLICATIONS_DATASOURCE_ID = "ds-external-publications";

    /** Config-lane document prefix under {@code ConfigDocumentStore} (strictly {@code [A-Za-z0-9._-]}). */
    public static final String CONFIG_DOCUMENT_PREFIX = "external-binding.";

    /** Version tag byte prefixing {@link #logicalSourceKey(NodeAddress)}. */
    public static final byte LOGICAL_SOURCE_KEY_VERSION = 1;

    private static final String LEDGER_DOC_ID_PREFIX = "k1.";
    private static final int MAX_LEDGER_DOC_ID_LENGTH = 1024;
    private static final int MIN_DISPLAY_URL_LENGTH = 8;
    private static final int MAX_DISPLAY_URL_LENGTH = 2048;
    private static final String HTTPS_SCHEME = "https://";
    private static final String HTTP_SCHEME = "http://";

    private static final Set<String> FORBIDDEN_URL_QUERY_KEYS = Set.of(
            "access_token",
            "bearer",
            "token",
            "id_token",
            "api_key",
            "apikey",
            "client_secret",
            "password",
            "secret",
            "sig",
            "x-amz-signature",
            "x-amz-credential",
            "x-goog-signature");

    private static final Set<String> FORBIDDEN_LABEL_KEY_FRAGMENTS = Set.of(
            "secret",
            "token",
            "password",
            "passwd",
            "bearer",
            "apikey",
            "api_key",
            "api-key",
            "private_key",
            "private-key",
            "credential");

    private ExternalPublicationCoordinates() {
    }

    /**
     * Returns {@code true} if {@code graphId} belongs to the internal external-binding
     * or external-publication ledger namespaces. Publication consumers use this predicate
     * so internal ledger/binding writes never trigger downstream external publication.
     *
     * @param graphId the graph identifier to inspect
     * @return {@code true} when {@code graphId} is an internal external graph
     */
    public static boolean isInternalExternalGraph(String graphId) {
        return graphId != null
                && (graphId.startsWith(BINDINGS_GRAPH_PREFIX)
                        || graphId.startsWith(PUBLICATIONS_GRAPH_PREFIX));
    }

    /**
     * Returns {@code true} if {@code address} targets an internal external-binding
     * or external-publication ledger graph.
     *
     * @param address the repository node address to inspect
     * @return {@code true} when {@code address} lives in an internal external graph
     */
    public static boolean isInternalExternalAddress(NodeAddress address) {
        return address != null && isInternalExternalGraph(address.getGraphId());
    }

    /**
     * Builds the canonical repository address for a versioned {@code ExternalSystemBinding}.
     *
     * @param accountId owning tenant account identifier
     * @param bindingId path-safe binding identifier
     * @return the canonical {@link NodeAddress} in {@code external-bindings:<accountId>}
     */
    public static NodeAddress bindingAddress(String accountId, String bindingId) {
        requireNonBlankCoordinate(accountId, "accountId");
        requireBindingId(bindingId);
        return NodeAddress.newBuilder()
                .setAccountId(accountId)
                .setGraphId(BINDINGS_GRAPH_PREFIX + accountId)
                .setGraphAddressId(BINDINGS_GRAPH_ADDRESS_ID)
                .setDocId(bindingId)
                .build();
    }

    /**
     * Builds the deterministic, collision-free config-lane document name for an
     * account-scoped binding using only {@code [A-Za-z0-9._-]} characters compatible
     * with {@code ConfigDocumentStore}.
     *
     * @param accountId owning tenant account identifier
     * @param bindingId path-safe binding identifier
     * @return the config document name in {@code ConfigDocumentStore}
     */
    public static String configDocumentName(String accountId, String bindingId) {
        requireNonBlankCoordinate(accountId, "accountId");
        requireBindingId(bindingId);
        byte[] accountBytes = accountId.getBytes(StandardCharsets.UTF_8);
        byte[] bindingBytes = bindingId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(
                (2 * Integer.BYTES) + accountBytes.length + bindingBytes.length);
        putSegment(buffer, accountBytes);
        putSegment(buffer, bindingBytes);
        return CONFIG_DOCUMENT_PREFIX
                + Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
    }

    /**
     * Encodes all four {@link NodeAddress} coordinates into a versioned, length-delimited
     * binary tuple {@code [version=1][account_len][account][graph_len][graph][node_len][node][doc_len][doc]}.
     *
     * <p>This encoding is injective for arbitrary UTF-8 strings, Unicode characters,
     * colons, slashes, and separators without delimiter ambiguity or hash truncation.
     *
     * @param sourceAddress the source repository address
     * @return the canonical binary key bytes
     */
    public static byte[] logicalSourceKey(NodeAddress sourceAddress) {
        Objects.requireNonNull(sourceAddress, "sourceAddress");
        byte[] account = sourceAddress.getAccountId().getBytes(StandardCharsets.UTF_8);
        byte[] graph = sourceAddress.getGraphId().getBytes(StandardCharsets.UTF_8);
        byte[] graphAddress = sourceAddress.getGraphAddressId().getBytes(StandardCharsets.UTF_8);
        byte[] doc = sourceAddress.getDocId().getBytes(StandardCharsets.UTF_8);
        int totalLength = 1 + (4 * Integer.BYTES)
                + account.length + graph.length + graphAddress.length + doc.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);
        buffer.put(LOGICAL_SOURCE_KEY_VERSION);
        putSegment(buffer, account);
        putSegment(buffer, graph);
        putSegment(buffer, graphAddress);
        putSegment(buffer, doc);
        return buffer.array();
    }

    /**
     * Decodes a binary key produced by {@link #logicalSourceKey(NodeAddress)} back into
     * its exact {@link NodeAddress}, rejecting unknown versions, negative/truncated lengths,
     * or trailing bytes.
     *
     * @param key the binary source key bytes
     * @return the reconstructed {@link NodeAddress}
     */
    public static NodeAddress decodeLogicalSourceKey(byte[] key) {
        Objects.requireNonNull(key, "key");
        if (key.length < 1 + (4 * Integer.BYTES)) {
            throw new IllegalArgumentException("logicalSourceKey is too short");
        }
        ByteBuffer buffer = ByteBuffer.wrap(key);
        byte version = buffer.get();
        if (version != LOGICAL_SOURCE_KEY_VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported logicalSourceKey version: " + version);
        }
        String accountId = readSegment(buffer);
        String graphId = readSegment(buffer);
        String graphAddressId = readSegment(buffer);
        String docId = readSegment(buffer);
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("logicalSourceKey carries trailing bytes");
        }
        return NodeAddress.newBuilder()
                .setAccountId(accountId)
                .setGraphId(graphId)
                .setGraphAddressId(graphAddressId)
                .setDocId(docId)
                .build();
    }

    /**
     * Computes the collision-free document identifier for an external publication ledger
     * from the bijective Base64url encoding of {@link #logicalSourceKey(NodeAddress)}.
     *
     * @param sourceAddress the non-internal source document address
     * @return the injective ledger {@code doc_id}
     */
    public static String ledgerDocId(NodeAddress sourceAddress) {
        requireSourceAddress(sourceAddress);
        String encoded = LEDGER_DOC_ID_PREFIX
                + Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(logicalSourceKey(sourceAddress));
        if (encoded.length() > MAX_LEDGER_DOC_ID_LENGTH) {
            throw new IllegalArgumentException(
                    "Encoded source address exceeds ledger doc_id ceiling of "
                            + MAX_LEDGER_DOC_ID_LENGTH + " characters");
        }
        return encoded;
    }

    /**
     * Builds the canonical repository address for an {@code ExternalPublicationLedger}
     * scoped to {@code (accountId, bindingId, sourceAddress)}.
     *
     * @param accountId owning tenant account identifier
     * @param bindingId path-safe binding identifier
     * @param sourceAddress canonical source document address within {@code accountId}
     * @return the ledger {@link NodeAddress} in {@code external-publications:<accountId>}
     */
    public static NodeAddress ledgerAddress(
            String accountId, String bindingId, NodeAddress sourceAddress) {
        requireNonBlankCoordinate(accountId, "accountId");
        requireBindingId(bindingId);
        requireSourceAddress(sourceAddress);
        if (!accountId.equals(sourceAddress.getAccountId())) {
            throw new IllegalArgumentException(
                    "sourceAddress.account_id does not match accountId");
        }
        return NodeAddress.newBuilder()
                .setAccountId(accountId)
                .setGraphId(PUBLICATIONS_GRAPH_PREFIX + accountId)
                .setGraphAddressId(bindingId)
                .setDocId(ledgerDocId(sourceAddress))
                .build();
    }

    /**
     * Validates that {@code sourceAddress} has four non-blank coordinates without NUL
     * bytes and does not target an internal external-binding or ledger graph.
     *
     * @param sourceAddress the source document address to validate
     */
    public static void requireSourceAddress(NodeAddress sourceAddress) {
        Objects.requireNonNull(sourceAddress, "sourceAddress");
        requireNonBlankCoordinate(sourceAddress.getAccountId(), "sourceAddress.account_id");
        requireNonBlankCoordinate(sourceAddress.getGraphId(), "sourceAddress.graph_id");
        requireNonBlankCoordinate(
                sourceAddress.getGraphAddressId(), "sourceAddress.graph_address_id");
        requireNonBlankCoordinate(sourceAddress.getDocId(), "sourceAddress.doc_id");
        if (isInternalExternalAddress(sourceAddress)) {
            throw new IllegalArgumentException(
                    "Internal external-binding and external-publication graphs cannot be published as source documents");
        }
    }

    /**
     * Validates that {@code bindingId} is a non-empty path-safe identifier up to 128 chars.
     *
     * @param bindingId the binding identifier to validate
     */
    public static void requireBindingId(String bindingId) {
        if (bindingId == null
                || bindingId.isEmpty()
                || bindingId.length() > 128
                || !Formats.isPathSafeName(bindingId)) {
            throw new IllegalArgumentException(
                    "bindingId must be a path-safe identifier of 1..128 characters");
        }
    }

    /**
     * Validates an optional non-authoritative {@code display_url} using deterministic
     * character scans (zero regex), rejecting non-http(s) schemes, whitespace/control
     * characters, embedded {@code @} userinfo credentials, and secret/bearer query keys.
     *
     * @param rawUrl the candidate display URL
     */
    public static void validateDisplayUrl(String rawUrl) {
        if (rawUrl == null
                || rawUrl.length() < MIN_DISPLAY_URL_LENGTH
                || rawUrl.length() > MAX_DISPLAY_URL_LENGTH) {
            throw new IllegalArgumentException("displayUrl must be 8..2048 characters");
        }
        for (int i = 0; i < rawUrl.length(); i++) {
            char ch = rawUrl.charAt(i);
            if (ch <= 0x20 || ch == 0x7F) {
                throw new IllegalArgumentException(
                        "displayUrl must not contain whitespace or control characters");
            }
        }
        int authorityStart;
        if (rawUrl.startsWith(HTTPS_SCHEME)) {
            authorityStart = HTTPS_SCHEME.length();
        } else if (rawUrl.startsWith(HTTP_SCHEME)) {
            authorityStart = HTTP_SCHEME.length();
        } else {
            throw new IllegalArgumentException("displayUrl must use http:// or https://");
        }

        int authorityEnd = rawUrl.length();
        for (int i = authorityStart; i < rawUrl.length(); i++) {
            char ch = rawUrl.charAt(i);
            if (ch == '/' || ch == '?' || ch == '#') {
                authorityEnd = i;
                break;
            }
        }
        String authority = rawUrl.substring(authorityStart, authorityEnd);
        if (authority.isEmpty()) {
            throw new IllegalArgumentException("displayUrl must specify a host");
        }
        if (rawUrl.indexOf('@') >= 0) {
            throw new IllegalArgumentException(
                    "displayUrl must not contain '@' userinfo or embedded credentials");
        }
        validateAuthorityHostAndPort(authority);

        int queryStart = rawUrl.indexOf('?', authorityEnd);
        if (queryStart >= 0) {
            int fragmentStart = rawUrl.indexOf('#', queryStart + 1);
            int queryEnd = fragmentStart >= 0 ? fragmentStart : rawUrl.length();
            validateQueryParameters(rawUrl, queryStart + 1, queryEnd);
        }
    }

    /**
     * Validates operator labels and non-secret binding text fields so secret keys,
     * bearer tokens, and credentialed URLs are never persisted in binding snapshots.
     *
     * @param labels map of label keys to label values
     */
    public static void validateNonSecretLabels(Map<String, String> labels) {
        if (labels == null || labels.isEmpty()) {
            return;
        }
        if (labels.size() > 64) {
            throw new IllegalArgumentException("labels must contain at most 64 entries");
        }
        for (Map.Entry<String, String> entry : labels.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null
                    || key.isEmpty()
                    || key.length() > 128
                    || !Formats.isPathSafeName(key)) {
                throw new IllegalArgumentException("Invalid label key: " + key);
            }
            String lowerKey = key.toLowerCase(Locale.ROOT);
            for (String forbidden : FORBIDDEN_LABEL_KEY_FRAGMENTS) {
                if (lowerKey.contains(forbidden)) {
                    throw new IllegalArgumentException(
                            "Secret-bearing label key is forbidden: " + key);
                }
            }
            requireNonSecretText(value, "labels." + key, 512);
        }
    }

    /**
     * Rejects strings that carry NUL characters, bearer/basic auth prefixes, or
     * credentialed URLs (`://` with `@` or forbidden query keys).
     *
     * @param text text value to inspect
     * @param fieldName logical field name for error reporting
     * @param maxLength maximum allowed UTF-16 length
     */
    public static void requireNonSecretText(String text, String fieldName, int maxLength) {
        if (text == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        if (text.length() > maxLength) {
            throw new IllegalArgumentException(
                    fieldName + " exceeds max length " + maxLength);
        }
        if (text.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(fieldName + " must not contain NUL bytes");
        }
        String trimmedLower = text.trim().toLowerCase(Locale.ROOT);
        if (trimmedLower.startsWith("bearer ")
                || trimmedLower.startsWith("basic ")
                || trimmedLower.startsWith("-----begin ")) {
            throw new IllegalArgumentException(
                    fieldName + " must not contain credential or key material");
        }
        int schemeSep = text.indexOf("://");
        if (schemeSep >= 0) {
            if (text.indexOf('@', schemeSep + 3) >= 0) {
                throw new IllegalArgumentException(
                        fieldName + " must not contain a URL with embedded credentials");
            }
            int qIndex = text.indexOf('?', schemeSep + 3);
            if (qIndex >= 0) {
                int hashIndex = text.indexOf('#', qIndex + 1);
                int end = hashIndex >= 0 ? hashIndex : text.length();
                validateQueryParameters(text, qIndex + 1, end);
            }
        }
    }

    static void requireNonBlankCoordinate(String value, String fieldName) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(fieldName + " must be non-blank and free of NUL");
        }
        boolean hasNonWhitespace = false;
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isWhitespace(value.charAt(i))) {
                hasNonWhitespace = true;
                break;
            }
        }
        if (!hasNonWhitespace) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static void validateAuthorityHostAndPort(String authority) {
        String host;
        String portText = null;
        if (authority.charAt(0) == '[') {
            int closeBracket = authority.indexOf(']');
            if (closeBracket <= 1) {
                throw new IllegalArgumentException("Malformed IPv6 host in displayUrl");
            }
            host = authority.substring(1, closeBracket);
            if (closeBracket + 1 < authority.length()) {
                if (authority.charAt(closeBracket + 1) != ':') {
                    throw new IllegalArgumentException("Malformed authority in displayUrl");
                }
                portText = authority.substring(closeBracket + 2);
            }
        } else {
            int colon = authority.indexOf(':');
            if (colon >= 0) {
                if (authority.indexOf(':', colon + 1) >= 0) {
                    throw new IllegalArgumentException("Unbracketed IPv6 host in displayUrl");
                }
                host = authority.substring(0, colon);
                portText = authority.substring(colon + 1);
            } else {
                host = authority;
            }
        }
        if (host.isEmpty()) {
            throw new IllegalArgumentException("displayUrl host must not be empty");
        }
        for (int i = 0; i < host.length(); i++) {
            char ch = host.charAt(i);
            boolean validHostChar = (ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '.'
                    || ch == '-'
                    || ch == ':';
            if (!validHostChar) {
                throw new IllegalArgumentException(
                        "Invalid character in displayUrl host: " + ch);
            }
        }
        if (portText != null) {
            if (portText.isEmpty() || portText.length() > 5) {
                throw new IllegalArgumentException("Invalid port in displayUrl");
            }
            int port = 0;
            for (int i = 0; i < portText.length(); i++) {
                char digit = portText.charAt(i);
                if (digit < '0' || digit > '9') {
                    throw new IllegalArgumentException("Non-numeric port in displayUrl");
                }
                port = (port * 10) + (digit - '0');
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("Port out of range in displayUrl");
            }
        }
    }

    private static void validateQueryParameters(String raw, int start, int end) {
        int pos = start;
        while (pos <= end) {
            int amp = raw.indexOf('&', pos);
            int paramEnd = (amp < 0 || amp > end) ? end : amp;
            if (paramEnd > pos) {
                int eq = raw.indexOf('=', pos);
                int keyEnd = (eq < 0 || eq > paramEnd) ? paramEnd : eq;
                String key = percentDecodeAsciiKey(raw.substring(pos, keyEnd))
                        .toLowerCase(Locale.ROOT);
                if (FORBIDDEN_URL_QUERY_KEYS.contains(key)) {
                    throw new IllegalArgumentException(
                            "Forbidden credential/token query parameter in URL: " + key);
                }
            }
            if (amp < 0 || amp >= end) {
                break;
            }
            pos = amp + 1;
        }
    }

    private static String percentDecodeAsciiKey(String key) {
        if (key.indexOf('%') < 0) {
            return key;
        }
        StringBuilder out = new StringBuilder(key.length());
        int i = 0;
        while (i < key.length()) {
            char ch = key.charAt(i);
            if (ch == '%' && i + 2 < key.length()) {
                int hi = Character.digit(key.charAt(i + 1), 16);
                int lo = Character.digit(key.charAt(i + 2), 16);
                if (hi >= 0 && lo >= 0) {
                    out.append((char) ((hi << 4) | lo));
                    i += 3;
                    continue;
                }
            }
            out.append(ch);
            i++;
        }
        return out.toString();
    }

    private static void putSegment(ByteBuffer buffer, byte[] utf8) {
        buffer.putInt(utf8.length);
        buffer.put(utf8);
    }

    private static String readSegment(ByteBuffer buffer) {
        if (buffer.remaining() < Integer.BYTES) {
            throw new IllegalArgumentException("Truncated segment length in logicalSourceKey");
        }
        int length = buffer.getInt();
        if (length < 0 || length > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid segment length in logicalSourceKey: " + length);
        }
        byte[] utf8 = new byte[length];
        buffer.get(utf8);
        return new String(utf8, StandardCharsets.UTF_8);
    }
}
