# Sourced-part attribution

Requirements §5 requires changed parts to record SaveOwner. DocumentSaves advanced written_in_revision for sourced parts but kept the old writer.

The new regression creates through intake, then sources PARSED from revision 1 through an authorized step save. Real PostgreSQL and LocalStack back the save engine. Before correction at 022acd913, the assertion expected step chunker but received intake ds-1. The faulty statement remained unchanged at reconciliation base 3c9241b52.

Correction: set the sourced entry owner from the authorized request. Existing admission, immutable StackRef and catalog transaction remain in use. Assertions cover persisted and returned owner, revision, zero uploads, reference reuse, unchanged CORE and unchanged history.

Red log: /tmp/pr313-source-owner-red.log. Integrated suite log: /tmp/pr313-source-owner-integrated.log. Accession suite passes. No RustFS, full-container or hosted-CI claim. Broader review remains incomplete.

JUnit totals (tests, failures, errors, skipped): 28, 0, 0, 0.
