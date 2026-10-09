package ai.protomolt.proto.repo.service;

import java.util.Map;

/**
 * Operator qualification for managed raw storage. This is configuration, not
 * proof of provider durability: composition must also check capabilities and
 * bind the generation to its nonsecret physical profile in the ledger.
 * Credentials must never be used as either identifier.
 */
public record ManagedStoragePolicy(String backendGeneration, String storageRealm, boolean retentionQualified) {
    public static final String ENV_GENERATION = "DOCUMENT_PLATFORM_MANAGED_BACKEND_GENERATION";
    public static final String ENV_REALM = "DOCUMENT_PLATFORM_MANAGED_STORAGE_REALM";
    public static final String ENV_RETENTION = "DOCUMENT_PLATFORM_MANAGED_RETENTION_QUALIFIED";

    public ManagedStoragePolicy {
        if (retentionQualified) {
            requireIdentifier(backendGeneration, ENV_GENERATION);
            requireIdentifier(storageRealm, ENV_REALM);
        } else if (backendGeneration != null || storageRealm != null) {
            throw new IllegalArgumentException("Managed storage identifiers require explicit retention qualification");
        }
    }

    public static ManagedStoragePolicy disabled() { return new ManagedStoragePolicy(null, null, false); }

    /** Parse strictly: a misspelled qualification must never silently disable a configured deployment. */
    public static ManagedStoragePolicy fromEnvironment(Map<String, String> environment) {
        for (String field : java.util.List.of(ENV_GENERATION, ENV_REALM, ENV_RETENTION)) {
            if (environment.containsKey(field) && (environment.get(field) == null || environment.get(field).isBlank()))
                throw new IllegalArgumentException(field + " must not be blank when present");
        }
        String flag = environment.get(ENV_RETENTION);
        boolean qualified;
        if (flag == null || "false".equalsIgnoreCase(flag.trim())) qualified = false;
        else if ("true".equalsIgnoreCase(flag.trim())) qualified = true;
        else throw new IllegalArgumentException(ENV_RETENTION + " must be true or false");
        return new ManagedStoragePolicy(optional(environment.get(ENV_GENERATION)),
                optional(environment.get(ENV_REALM)), qualified);
    }

    private static String optional(String value) { return value == null || value.isBlank() ? null : value.trim(); }

    private static void requireIdentifier(String value, String field) {
        if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}"))
            throw new IllegalArgumentException(field + " requires a nonsecret identifier of 1 to 128 characters");
    }
}
