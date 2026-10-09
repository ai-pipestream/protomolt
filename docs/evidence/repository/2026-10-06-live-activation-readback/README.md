# Live activation acknowledgment recovery

A real PostgreSQL test commits successor activation, then cancels the caller at
the JDBC commit boundary before its acknowledgment reaches the retained attempt.
With the lease still live, retrying that same attempt attaches its exact successor.
The test requires one installation, one activation, no unactivated supersession,
one retained session, and no retained attempt payload budget. This confirms the
existing live-lease path; it adds no production implementation.

The focused run passed one test with no failures or skips. The initial run reached
all activation assertions but failed the test's drain inspection because admission
had not been closed first. Correcting that test cleanup produced this result; the
initial failure was not a production regression.

These are metadata protocol assertions with synthetic payloads, not provider
publication or performance measurements. Expired-bound-owner reconciliation and
its lost-acknowledgment, authorization and worker-lifetime cases remain open.
