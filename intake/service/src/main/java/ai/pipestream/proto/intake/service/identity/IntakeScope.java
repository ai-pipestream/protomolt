package ai.pipestream.proto.intake.service.identity;

import java.util.Set;

/**
 * The authority an API key carries: the account it belongs to and the
 * host-owned limits the key store attached to it. Requests only narrow within
 * this scope, never widen it — a request naming a datasource or drive outside
 * the scope is {@code PERMISSION_DENIED}.
 *
 * <p>Restriction semantics: an empty {@code datasourceIds} / {@code drives} /
 * {@code mimeTypes} set means the key is unrestricted on that axis <em>within
 * its account</em> — the account boundary itself is never optional. A
 * {@code maxPayloadBytes} of zero means the key carries no per-key payload cap
 * (the service's own cap still applies). Key stores SHOULD mint keys with
 * explicit restrictions; the unrestricted forms exist for account-wide keys,
 * not as an implicit default for careless minting.
 *
 * @param accountId account (tenant root) every ingest under this key is owned
 *        by; never blank
 * @param datasourceIds datasources the key may ingest under; empty means any
 *        datasource of the account
 * @param drives drives the key may save to; empty means any drive of the
 *        account
 * @param mimeTypes declared MIME types the key accepts; empty means no
 *        content-type restriction
 * @param maxPayloadBytes per-key payload cap in bytes; zero means no per-key
 *        cap
 */
public record IntakeScope(
        String accountId,
        Set<String> datasourceIds,
        Set<String> drives,
        Set<String> mimeTypes,
        long maxPayloadBytes) {

    public IntakeScope {
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("accountId must not be blank");
        }
        if (maxPayloadBytes < 0) {
            throw new IllegalArgumentException("maxPayloadBytes must not be negative");
        }
        datasourceIds = Set.copyOf(datasourceIds);
        drives = Set.copyOf(drives);
        mimeTypes = Set.copyOf(mimeTypes);
    }

    /** An unrestricted scope for {@code accountId}: any datasource, any drive, any type, no per-key cap. */
    public static IntakeScope unrestricted(String accountId) {
        return new IntakeScope(accountId, Set.of(), Set.of(), Set.of(), 0L);
    }

    /** Whether the key may ingest under {@code datasourceId}. */
    public boolean allowsDatasource(String datasourceId) {
        return datasourceIds.isEmpty() || datasourceIds.contains(datasourceId);
    }

    /** Whether the key may save to {@code drive}. */
    public boolean allowsDrive(String drive) {
        return drives.isEmpty() || drives.contains(drive);
    }

    /**
     * Whether the key accepts a payload declared as {@code mimeType}. A blank
     * declaration passes an unrestricted scope and fails a restricted one — a
     * key restricted by content type demands the caller declare one.
     */
    public boolean allowsMimeType(String mimeType) {
        if (mimeTypes.isEmpty()) {
            return true;
        }
        return mimeType != null && mimeTypes.contains(mimeType);
    }

    /** Whether a payload of {@code sizeBytes} fits the per-key cap. */
    public boolean allowsPayloadSize(long sizeBytes) {
        return maxPayloadBytes == 0L || sizeBytes <= maxPayloadBytes;
    }
}
