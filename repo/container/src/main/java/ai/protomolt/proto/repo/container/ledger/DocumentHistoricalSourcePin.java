package ai.protomolt.proto.repo.container.ledger;

import java.util.UUID;

/** Captured pin identity for retention accounting; neither publication nor release authority. */
record DocumentHistoricalSourcePin(UUID reader, UUID pin, UUID object, UUID node,
        UUID revision, long publicationRevision) {}
