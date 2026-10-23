-- Subject tags (requirements §17).
--
-- The concept tags of each document's current revision: one row per value of a
-- ConceptRef field that the schema binds with concept_schemes, named by the
-- same occurrence identity as a link. Replaced on each accession that writes
-- or sources CORE, cleared by a tombstone or a purge, and rebuilt from the
-- stored CORE part with its retained schema. Only the tagged ids are stored:
-- no labels, no ancestors and no expansion, so a concept scheme change needs
-- no rewrite here; a concept filter expands against the mounted schemes when
-- it runs and matches these pairs.
CREATE TABLE catalog_document_concepts (
    document_id bigint  NOT NULL REFERENCES catalog_documents,
    field_path  text    NOT NULL CHECK (length(field_path) > 0),
    ordinal     integer NOT NULL CHECK (ordinal >= 0),
    scheme_id   text    NOT NULL CHECK (length(scheme_id) > 0),
    concept_id  text    NOT NULL CHECK (length(concept_id) > 0),
    PRIMARY KEY (document_id, field_path, ordinal)
);

-- A concept filter: the documents tagged with any pair of the expanded set.
CREATE INDEX catalog_document_concepts_concept ON catalog_document_concepts (scheme_id, concept_id, document_id);
