# Competing bound-owner recovery

Two focused PostgreSQL cases passed, with no failures or skips. Both leave a local
attempt after V94 committed but its acknowledgment was lost. Another independent
session manager then reserves, installs and activates the replacement. Natural
expiry makes that foreign bound owner eligible for discovery; the original
retained attempt still rejects it rather than adopting another coordinator's work.

One case first injects a real JDBC commit failure for the original attempt's next
V97. That reservation rolls back, but the exact local proposal remains pending.
The winner's later commit prevents that pending proposal from being confirmed.

Assertions preserve the winner's claim epoch/token/lease and owner generation,
nonce and lease across rejection and cleanup. They check durable reservation and
activation counts, retained local payload capacity on rejection, and release only
after permanent fencing. Removing retry metadata leaves its session retained;
explicit session retirement does not remove the independent winner's session.

No production code changed. These synthetic-payload metadata fixtures do not prove
real provider-worker drain, remote quiescence or performance. Current caller
revocation and terminal/shutdown disposal while pending still need dedicated
acceptance cases. `green.log` and `result.xml` contain the focused evidence.
