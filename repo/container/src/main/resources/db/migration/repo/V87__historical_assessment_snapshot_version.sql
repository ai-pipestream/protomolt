-- Existing snapshots are immutable and retain version 1. New handler snapshots
-- encode declaration kind and historical source node with version 2. This is not
-- historical execution activation or a rewrite of retained evidence.
ALTER TABLE document_assessment_slot_snapshots
 DROP CONSTRAINT document_assessment_slot_snapshots_snapshot_version_check;
ALTER TABLE document_assessment_slot_snapshots
 ADD CONSTRAINT document_assessment_slot_snapshots_snapshot_version_check CHECK(snapshot_version IN (1,2));
