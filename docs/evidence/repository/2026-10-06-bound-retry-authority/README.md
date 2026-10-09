# Recovery credential and grant checks

All 6 PostgreSQL cases passed, with no failures or skips. Key revocation, key
rotation and creation-grant revocation occur before reservation or after its commit
with a lost reply. The scoped credential and absent-target grant are installed
before claim admission. Fixture bootstrap uses process authority; activation and
recovery use scoped execution identity, separate from private recovery authority.

Invalid keys return UNAUTHENTICATED; revoked grants return NOT_FOUND. Denial
preserves the claim, owner, leases, local proposal and payload capacity. Counts
show no further reservation, activation or supersession. Private metadata cleanup
preserves SQL state and the separately retained session.

The first run incorrectly expected PERMISSION_DENIED for invalid keys. The
implementation already returned UNAUTHENTICATED. Corrected assertions passed.
Expiry waits use the actual database lease timestamps.

Sol reviewed these tests. No production code changed. Synthetic payload declarations
and direct repository calls test SQL authorization only. Transport authentication,
provider workers, existing-document operations without creation grants and
concurrent authority changes at commit require separate acceptance evidence.
