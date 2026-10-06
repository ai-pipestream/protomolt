-- Historical provenance is separate from current reuse. Java execution remains
-- gated until binding, versioned replay, authorization and reference publication
-- are integrated. This SQL guard does not decode canonical command protobuf.
LOCK TABLE document_assessment_slots IN ACCESS EXCLUSIVE MODE;
ALTER TABLE document_assessment_slots ADD COLUMN source_node UUID;
ALTER TABLE document_assessment_slots DROP CONSTRAINT document_assessment_slots_declaration_check;
ALTER TABLE document_assessment_slots DROP CONSTRAINT document_assessment_slots_check;
ALTER TABLE document_assessment_slots ADD CONSTRAINT document_assessment_slots_declaration_check
 CHECK(declaration IN ('NEW_CONTENT','REUSE','HISTORICAL_REUSE'));
ALTER TABLE document_assessment_slots ADD CONSTRAINT document_assessment_slots_source_check CHECK(
 (declaration='NEW_CONTENT' AND source_node IS NULL AND source_revision IS NULL AND source_ordinal IS NULL)
 OR (declaration='REUSE' AND source_node IS NULL AND source_revision IS NOT NULL
     AND source_ordinal IS NOT NULL AND source_ordinal>=0)
 OR (declaration='HISTORICAL_REUSE' AND source_node IS NOT NULL AND source_revision IS NOT NULL
     AND source_ordinal IS NOT NULL AND source_ordinal BETWEEN 0 AND 9999));

CREATE OR REPLACE FUNCTION check_document_assessment_slots(p_owner document_assessment_owners)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE objects UUID[]; matched INTEGER;
BEGIN
 IF (SELECT count(*) FROM document_assessment_slots WHERE assessment_id=p_owner.assessment_id)<>p_owner.expected_slots THEN
  RAISE EXCEPTION 'Assessment requires its complete declared candidate slot set';
 END IF;
 SELECT array_agg(DISTINCT object_id ORDER BY object_id) INTO objects
 FROM document_assessment_slots WHERE assessment_id=p_owner.assessment_id;
 PERFORM lock_repository_retention_set(objects);
 -- Every source lock is now held. Recheck time, current selection and physical
 -- eligibility after waits. Callers already hold the complete logical lock set.
 PERFORM require_repository_operation_write_fence(p_owner.account_id,p_owner.principal,p_owner.operation_id,p_owner.owner_generation);
 IF p_owner.retain_until<=clock_timestamp() THEN RAISE EXCEPTION 'Assessment staging deadline expired'; END IF;
 SELECT count(*) INTO matched FROM document_assessment_slots s
 JOIN document_operation_selection_current c
  ON c.account_id=p_owner.account_id AND c.principal=p_owner.principal AND c.operation_id=p_owner.operation_id
   AND c.owner_generation=p_owner.owner_generation AND c.member_id=s.member_id AND c.selection_revision=s.selection_revision
 JOIN document_operation_selection_attempts selected
  ON selected.account_id=c.account_id AND selected.principal=c.principal AND selected.operation_id=c.operation_id
   AND selected.owner_generation=c.owner_generation AND selected.member_id=c.member_id
   AND selected.selection_revision=c.selection_revision
 JOIN repository_physical_locations l ON l.object_id=s.object_id AND l.source_kind='DOCUMENT_PART'
 JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
  AND o.physical_object_id=l.object_id AND o.verified
 JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.state='VERIFIED' AND a.account_id=p_owner.account_id
 JOIN repository_object_retention r ON r.object_id=l.object_id AND NOT r.retiring AND NOT r.reclaiming
 WHERE s.assessment_id=p_owner.assessment_id
  AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup cleanup WHERE cleanup.attempt_id=a.attempt_id)
  AND CASE s.declaration WHEN 'NEW_CONTENT' THEN
   a.plan_kind='NEW_CONTENT' AND a.lease_until>clock_timestamp() AND a.attempt_id=selected.attempt_id
   AND a.account_id=p_owner.account_id AND a.operation_principal=p_owner.principal
   AND a.operation_id=p_owner.operation_id AND a.operation_generation=p_owner.owner_generation
   AND a.member_id=s.member_id AND o.revision_ordinal=s.revision_ordinal
  WHEN 'REUSE' THEN EXISTS(
   SELECT 1 FROM document_revision_publications h
   JOIN document_revision_current current_ref ON current_ref.revision_id=h.revision_id AND current_ref.node_id=h.node_id
   JOIN documents d ON d.node_id=h.node_id AND d.account_id=p_owner.account_id
   JOIN document_revision_parts part ON part.revision_id=h.revision_id AND part.revision_ordinal=s.source_ordinal
    AND part.object_id=s.object_id
   WHERE h.revision_id=s.source_revision AND h.projection_sealed AND d.status='AVAILABLE' AND d.pending_purge_id IS NULL
    AND EXISTS(SELECT 1 FROM repository_object_references ref WHERE ref.object_id=s.object_id
     AND ref.owner_kind='DOCUMENT_HISTORY' AND ref.owner_id=h.revision_id AND ref.owner_revision=h.publication_revision)
    AND EXISTS(SELECT 1 FROM repository_object_references ref WHERE ref.object_id=s.object_id
     AND ref.owner_kind='DOCUMENT_CURRENT' AND ref.owner_id=h.node_id AND ref.owner_revision=h.publication_revision)
)
WHEN 'HISTORICAL_REUSE' THEN EXISTS(
 SELECT 1 FROM document_revision_publications h
 JOIN documents d ON d.node_id=h.node_id AND d.account_id=p_owner.account_id
 JOIN document_revision_commits committed ON committed.revision_id=h.revision_id AND committed.node_id=h.node_id
  AND committed.account_id=p_owner.account_id AND committed.publication_revision=h.publication_revision
  AND committed.creation_xid=h.projection_xid
 JOIN repository_operation_success success ON success.account_id=committed.account_id
  AND success.principal=committed.principal AND success.operation_id=committed.operation_id
  AND success.owner_generation=committed.owner_generation AND success.creation_xid=committed.creation_xid
 JOIN document_revision_parts part ON part.revision_id=h.revision_id AND part.revision_ordinal=s.source_ordinal
  AND part.object_id=s.object_id AND part.part=o.part AND part.sub_key=o.sub_key
 WHERE h.node_id=s.source_node AND h.revision_id=s.source_revision AND h.native_binding=h.revision_id
  AND h.projection_sealed AND d.status='AVAILABLE' AND d.pending_purge_id IS NULL
  AND EXISTS(SELECT 1 FROM repository_object_references ref WHERE ref.object_id=s.object_id
   AND ref.owner_kind='DOCUMENT_HISTORY' AND ref.owner_id=h.revision_id AND ref.owner_revision=h.publication_revision)
) ELSE false END;
 IF matched<>p_owner.expected_slots THEN
  RAISE EXCEPTION 'Assessment candidate differs from selected verified uploads or retained current sources';
 END IF;
 -- The forward join is insufficient: a caller could omit a selected upload and
 -- lower expected_slots. Every selected member and every selected upload must
 -- appear, while zero-upload reuse members need no synthetic attempt.
 IF EXISTS(
  SELECT 1 FROM document_operation_selection_current c
  JOIN document_operation_selection_attempts selected
   USING(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
  WHERE c.account_id=p_owner.account_id AND c.principal=p_owner.principal AND c.operation_id=p_owner.operation_id
   AND c.owner_generation=p_owner.owner_generation
   AND (NOT EXISTS(SELECT 1 FROM document_assessment_slots s WHERE s.assessment_id=p_owner.assessment_id AND s.member_id=c.member_id)
    OR EXISTS(SELECT 1 FROM document_part_attempt_objects o WHERE o.attempt_id=selected.attempt_id
     AND NOT EXISTS(SELECT 1 FROM document_assessment_slots s WHERE s.assessment_id=p_owner.assessment_id
      AND s.member_id=c.member_id AND s.declaration='NEW_CONTENT' AND s.selection_revision=c.selection_revision
      AND s.revision_ordinal=o.revision_ordinal AND s.object_id=o.physical_object_id)))
 ) THEN RAISE EXCEPTION 'Assessment omits a selected member or uploaded part'; END IF;
END;
$$;
