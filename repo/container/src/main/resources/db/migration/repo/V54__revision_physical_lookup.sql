-- A revision's exact physical object resolves its full manifest ordinal without
-- scanning every part. Avoid indexing unbounded sub_key text in a B-tree key.
CREATE INDEX document_revision_parts_revision_object
    ON document_revision_parts(revision_id,object_id);
