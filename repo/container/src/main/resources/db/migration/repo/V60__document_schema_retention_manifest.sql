-- Exact-set representation for the forthcoming admission binding. This is a
-- read-only projection, not a relaxation of V59 or a typed admission verdict.
-- Decimal strings match the Java codec without losing BIGINT precision.
CREATE FUNCTION document_schema_retention_manifest_v1(scoped_revision UUID)
RETURNS JSONB LANGUAGE plpgsql STABLE AS $$
DECLARE row_count BIGINT; estimated_bytes BIGINT := 256; contribution BIGINT; result JSONB;
BEGIN
 IF scoped_revision IS NULL OR NOT EXISTS(SELECT 1 FROM document_revision_commits WHERE revision_id=scoped_revision) THEN
  RAISE EXCEPTION 'Schema manifest requires a native revision';
 END IF;
 -- Bound counts and worst-case escaped text before jsonb_agg materializes rows.
 SELECT count(*),COALESCE(sum(256::bigint+6::bigint*octet_length(sub_key)),0)
 INTO row_count,contribution FROM (
  SELECT sub_key FROM document_revision_parts WHERE revision_id=scoped_revision LIMIT 10001) p;
 IF row_count>10000 THEN RAISE EXCEPTION 'Schema manifest part count exceeds bound'; END IF;
 estimated_bytes := estimated_bytes+contribution;
 SELECT count(*) INTO row_count FROM (
  SELECT 1 FROM document_revision_schema_artifacts WHERE revision_id=scoped_revision LIMIT 65) a;
 IF row_count>64 THEN RAISE EXCEPTION 'Schema manifest artifact count exceeds bound'; END IF;
 estimated_bytes := estimated_bytes+68*row_count;
 SELECT count(*),COALESCE(sum(512::bigint+6::bigint*octet_length(type_url)),0)
 INTO row_count,contribution FROM (
  SELECT type_url FROM document_revision_schema_assets WHERE revision_id=scoped_revision LIMIT 65) a;
 IF row_count>64 THEN RAISE EXCEPTION 'Schema manifest association count exceeds bound'; END IF;
 estimated_bytes := estimated_bytes+contribution;
 SELECT count(*) INTO row_count FROM (
  SELECT 1 FROM document_revision_schema_evidence WHERE revision_id=scoped_revision LIMIT 1025) e;
 IF row_count>1024 THEN RAISE EXCEPTION 'Schema manifest root count exceeds bound'; END IF;
 estimated_bytes := estimated_bytes+512*row_count;
 IF estimated_bytes>16777216 THEN RAISE EXCEPTION 'Schema manifest exceeds allocation budget'; END IF;
 SELECT jsonb_build_object('version','1',
  'parts',COALESCE((SELECT jsonb_agg(jsonb_build_object(
   'ordinal',p.revision_ordinal::text,'part',p.part::text,'sub_key',p.sub_key,
   'object_id',p.object_id::text,'size',o.expected_size::text,'sha256',o.expected_sha256)
   ORDER BY p.revision_ordinal)
   FROM document_revision_parts p
   LEFT JOIN repository_physical_locations l ON l.object_id=p.object_id AND l.source_kind='DOCUMENT_PART'
   LEFT JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
    AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
   WHERE p.revision_id=scoped_revision),'[]'::jsonb),
  'artifacts',COALESCE((SELECT jsonb_agg(encode(a.artifact_sha256,'hex') ORDER BY a.artifact_sha256)
   FROM document_revision_schema_artifacts a WHERE a.revision_id=scoped_revision),'[]'::jsonb),
  'assets',COALESCE((SELECT jsonb_agg(jsonb_build_object(
   'type_url',a.type_url,'type_url_sha256',encode(a.type_url_sha256,'hex'),
   'descriptor_sha256',encode(a.descriptor_sha256,'hex'),'metadata_sha256',encode(a.metadata_sha256,'hex'),
   'metadata_codec',a.metadata_codec,'metadata_version',a.metadata_version::text,
   'source_sha256',encode(a.source_sha256,'hex')) ORDER BY a.type_url_sha256,a.descriptor_sha256)
   FROM document_revision_schema_assets a WHERE a.revision_id=scoped_revision),'[]'::jsonb),
  'roots',COALESCE((SELECT jsonb_agg(jsonb_build_object(
   'ordinal',e.revision_ordinal::text,'locator_sha256',encode(e.root_locator_sha256,'hex'),
   'fragment_sha256',encode(e.fragment_sha256,'hex'),'fragment_size',e.fragment_size::text,
   'evidence_codec',e.evidence_codec,'evidence_version',e.evidence_version::text,
   'evidence_sha256',encode(e.evidence_sha256,'hex')) ORDER BY e.revision_ordinal,e.root_locator_sha256)
   FROM document_revision_schema_evidence e WHERE e.revision_id=scoped_revision),'[]'::jsonb)) INTO result;
 IF octet_length(result::text)>16777216 THEN RAISE EXCEPTION 'Schema manifest exceeds byte limit'; END IF;
 RETURN result;
END;
$$;
