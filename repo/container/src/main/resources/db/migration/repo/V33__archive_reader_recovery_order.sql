-- Preserve the object-prefix lookup while supporting recovery keyset ordering.
CREATE INDEX archive_read_pin_object_order ON archive_read_pins(object_id,pin_id);
DROP INDEX archive_read_pin_object;
