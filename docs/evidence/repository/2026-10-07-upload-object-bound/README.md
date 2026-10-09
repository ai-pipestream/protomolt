# Host upload-object limit

Publication input validation and the runtime repository facade accept a smaller
host object maximum. Existing entry points retain the 8 MiB default. Incoming
uploads are checked before checksums, copying, storage selection, receipt lookup
or durable admission. References to stored objects keep their existing rules.

Input tests cover the exact limit, oversized content with an incorrect checksum,
invalid configured limits and empty bodies. The PostgreSQL/LocalStack service
fixture uses a real completed publication: exact-size replay succeeds, smaller
limits reject terminal and fresh requests, and rejected fresh requests create no
SQL operation owner or execution claim. The packaged storage suite passed in
3 minutes 42 seconds. Sol reviewed the code and replay implications.

Reducing an endpoint limit can prevent old full-body requests from replaying
through that endpoint; the stored receipt remains. The Redis document host is
not available yet. See repository-bounded-documents.md for the assembly design.
