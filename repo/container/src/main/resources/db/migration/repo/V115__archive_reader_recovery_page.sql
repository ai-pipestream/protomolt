-- Exact-reader recovery uses a bounded ordered page, independent of other readers.
CREATE INDEX archive_read_pin_reader_order ON archive_read_pins(reader_incarnation,pin_id) INCLUDE(object_id);
DROP INDEX archive_read_pin_incarnation;
