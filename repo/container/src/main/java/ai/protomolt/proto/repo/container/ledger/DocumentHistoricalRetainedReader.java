package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryReadControl;

/** Provider-read port for an exact ordinal of an authorized historical revision. */
@FunctionalInterface
public interface DocumentHistoricalRetainedReader {
    /**
     * Return exactly one verified fragment for the complete revision ordinal.
     * Resolve the captured backend generation and provider version, never the
     * current drive or latest object. Missing ordinals fail explicitly.
     *
     * The batch owns a transferred history use and byte reservations through
     * actual provider completion. Keep it open until assessment copying finishes.
     * Closing the batch does not close the history, provider or reader; the host
     * separately closes, drains and releases the capture. Cancellation must not
     * release protection while a provider worker still uses it.
     *
     * Recheck current source authorization before delivering bytes or provider
     * error details. This port grants no publication or recovery authority.
     */
    DocumentRetainedReader.Batch readHistorical(DocumentReadLedger.PinnedHistory history,
            int revisionOrdinal, RepositoryReadControl control);
}
