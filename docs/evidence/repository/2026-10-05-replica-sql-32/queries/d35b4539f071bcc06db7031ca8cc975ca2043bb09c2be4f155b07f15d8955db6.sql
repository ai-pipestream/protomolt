SELECT p.pin_id,p.reader_incarnation,p.object_id FROM archive_read_pins p
JOIN repository_reader_incarnations r ON r.incarnation=p.reader_incarnation
WHERE r.state='QUIESCED'
 ORDER BY p.object_id,p.pin_id LIMIT $1