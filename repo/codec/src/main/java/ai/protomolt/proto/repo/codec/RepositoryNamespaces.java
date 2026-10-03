package ai.protomolt.proto.repo.codec;

/** Reserved storage namespaces shared by repository admission and cleanup. */
public final class RepositoryNamespaces {
    private RepositoryNamespaces() {}

    public static boolean isManagedRaw(String objectKey) {
        return hasSegment(objectKey, ".protomolt-managed");
    }

    /** Includes archive keys written before durable object bindings existed. */
    public static boolean isArchive(String objectKey) {
        return hasSegment(objectKey, "archive");
    }

    /** Includes both historical fixed part keys and current write-attempt keys. */
    public static boolean isDocumentPart(String objectKey) {
        return hasSegment(objectKey, "documents");
    }

    /** Keys are opaque: never decode, normalize, or infer identity from a current drive. */
    private static boolean hasSegment(String objectKey, String reserved) {
        if (objectKey == null) return false;
        int start = 0;
        while (start <= objectKey.length()) {
            int end = objectKey.indexOf('/', start);
            if (end < 0) end = objectKey.length();
            if (end - start == reserved.length() && objectKey.startsWith(reserved, start)) return true;
            if (end == objectKey.length()) break;
            start = end + 1;
        }
        return false;
    }
}
