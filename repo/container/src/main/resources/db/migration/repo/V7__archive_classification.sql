-- Asset classification on archive entries: the state machine's stored
-- fact. The state rides its own column so listing filters and the exact
-- per-state counts are one indexed aggregate; the full Classification
-- (facts, evidence, attribution, origin) rides beside it as protobuf-JSON.
-- Existing rows predate classification and are exactly that: UNCLASSIFIED,
-- a first-class stored state, never a silent default.

ALTER TABLE archive_entries
    ADD COLUMN classification JSONB,
    ADD COLUMN classification_state TEXT NOT NULL DEFAULT 'UNCLASSIFIED';

ALTER TABLE archive_entries
    ADD CONSTRAINT chk_archive_entries_classification_state CHECK (
        classification_state IN
            ('UNCLASSIFIED', 'DECLARED', 'IDENTIFIED', 'VERIFIED', 'CONFLICTED'));

-- The per-state count and the state-filtered listing.
CREATE INDEX idx_archive_entries_classification
    ON archive_entries (account_id, archive, classification_state);
