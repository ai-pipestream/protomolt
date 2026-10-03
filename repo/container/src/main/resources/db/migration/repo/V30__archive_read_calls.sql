-- One client statement per short admission/release transaction. Existing triggers
-- remain authoritative for direct writes. No provider I/O or expiry is introduced.
CREATE FUNCTION acquire_archive_read_pin(p_pin UUID, p_reader UUID, p_entry UUID, p_version BIGINT, p_object UUID)
RETURNS TABLE(object_id UUID, entry_uuid UUID, account_id TEXT, archive TEXT, backend_generation TEXT,
    bucket TEXT, object_key TEXT, storage_realm TEXT, expected_size BIGINT, sha256 TEXT, provider_version TEXT)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE closed BOOLEAN;
BEGIN
    PERFORM require_repository_read_committed();
    IF p_pin IS NULL OR p_reader IS NULL OR p_entry IS NULL OR p_object IS NULL OR p_version IS NULL OR p_version<=0 THEN
        RAISE EXCEPTION 'Archive read admission requires complete identity and a positive version';
    END IF;
    PERFORM 1 FROM archive_object_uploads u WHERE u.object_id=p_object FOR SHARE;
    IF NOT FOUND THEN RETURN; END IF;
    SELECT r.retiring INTO STRICT closed FROM repository_object_retention r WHERE r.object_id=p_object FOR SHARE;
    IF closed THEN RETURN; END IF;
    SELECT b.object_id,b.entry_uuid,b.account_id,b.archive,b.backend_generation,
        b.bucket,b.object_key,b.storage_realm,u.expected_size,u.sha256,u.provider_version
        INTO object_id,entry_uuid,account_id,archive,backend_generation,
            bucket,object_key,storage_realm,expected_size,sha256,provider_version
        FROM archive_object_bindings b
        JOIN archive_object_uploads u ON u.object_id=b.object_id AND u.state='LIVE'
        JOIN archive_version_object_refs r ON r.object_id=b.object_id AND r.entry_uuid=b.entry_uuid
        WHERE r.entry_uuid=p_entry AND r.version=p_version AND b.object_id=p_object;
    IF NOT FOUND THEN RETURN; END IF;
    INSERT INTO archive_read_pins(pin_id,reader_incarnation,object_id,entry_uuid,version)
        VALUES(p_pin,p_reader,p_object,p_entry,p_version);
    RETURN NEXT;
END;
$$;

CREATE FUNCTION release_archive_read_pin(p_pin UUID, p_reader UUID, p_object UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE deleted_rows BIGINT;
BEGIN
    IF p_pin IS NULL OR p_reader IS NULL OR p_object IS NULL THEN
        RAISE EXCEPTION 'Archive read release requires complete identity';
    END IF;
    PERFORM share_archive_retention_owner(p_object);
    PERFORM 1 FROM repository_object_retention r WHERE r.object_id=p_object FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Repository object retention row is missing'; END IF;
    DELETE FROM archive_read_pins WHERE pin_id=p_pin AND reader_incarnation=p_reader AND object_id=p_object;
    GET DIAGNOSTICS deleted_rows = ROW_COUNT;
    -- A prior release may have committed without its acknowledgement reaching Java.
    IF deleted_rows=0 AND EXISTS(SELECT 1 FROM archive_read_pins WHERE pin_id=p_pin) THEN
        RAISE EXCEPTION 'Archive read pin belongs to another incarnation or object';
    END IF;
    RETURN true;
END;
$$;
