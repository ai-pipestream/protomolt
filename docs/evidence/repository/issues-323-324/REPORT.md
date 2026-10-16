# Slice 1 tests (#323 and #324)

Tests cover EMPTY/DELETE, invalid deletion reasons, invalid CORE actions, mismatched source document with corrected same-ID retry, and ACL narrowing with read/write/outcome denial plus authorized confirmation. No production change.

The first ACL run omitted mandatory ListRevisions.page_size. The fixture was corrected to page_size=10. This was not a production failure.

Final command: ./gradlew :protomolt-repo-accession:test --tests '*SliceOneFollowupsIT' --max-workers=2
Result: 3 tests, zero failures/errors/skips on PostgreSQL and LocalStack. Log: /tmp/pr323-324-final.log.

Requirements §15.12 includes the approved current-read-access condition from #324 comment 10817. SOURCE_PURGED and opaque support belong to #318 and #316.
