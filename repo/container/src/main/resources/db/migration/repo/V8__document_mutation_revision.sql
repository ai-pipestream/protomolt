-- Sequence-backed revisions cover SQL updates as well as ORM writes, including
-- policy edits and delete/reinsert of a storage identity. Sequence gaps are valid.
CREATE SEQUENCE document_mutation_revision_seq AS BIGINT;
ALTER TABLE documents ADD COLUMN mutation_revision BIGINT;
UPDATE documents SET mutation_revision = nextval('document_mutation_revision_seq');
ALTER TABLE documents ALTER COLUMN mutation_revision SET NOT NULL;

CREATE FUNCTION assign_document_mutation_revision() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.mutation_revision := nextval('document_mutation_revision_seq');
    RETURN NEW;
END;
$$;

CREATE TRIGGER document_mutation_revision
BEFORE INSERT OR UPDATE ON documents
FOR EACH ROW EXECUTE FUNCTION assign_document_mutation_revision();
