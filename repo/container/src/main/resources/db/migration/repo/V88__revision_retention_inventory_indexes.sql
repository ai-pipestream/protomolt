-- Observational inventory only. No immutable-history or schema deletion guard changes.
CREATE INDEX document_read_pin_revision ON document_read_pins(source_revision);
CREATE INDEX document_assessment_slot_source_revision ON document_assessment_slots(source_revision)
 WHERE source_revision IS NOT NULL;
