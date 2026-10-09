# Scoped read authorization during bound recovery

Two focused PostgreSQL cases passed with no failures or skips. They use a scoped
account caller with document READ/WRITE permissions, independently of the process
caller used for private recovery authority. Each leaves an uncertain committed
activation and lets its leases expire naturally.

Removing document permissions blocks bound recovery with NOT_FOUND, both before
the next reservation and after that reservation committed with a lost reply.
Rejection preserves the claim/owner identities, leases, local proposal and retained
payload capacity. Restoring permissions allows reservation confirmation; the
pending committed case confirms the identical durable tuple rather than creating
another reservation.

Policy changes also increment document mutation revision. Restoring permissions
does not refresh the command's optimistic revision condition. Installation can
complete, but activation rejects the now-stale command without a second execution
record. Closed recovery disposal then releases its metadata under exact session
ownership proof.

The initial test setup granted permissions after capturing revision conditions and
therefore failed before reaching the intended scenario. The corrected setup reads
the updated revision; that setup failure was not a production regression.

No production code changed. These are synthetic-payload metadata fixtures, not
provider or semantic-validation qualification. Credential rotation/revocation,
creation-grant revocation, concurrent policy changes at commit, terminal disposal
and real worker lifecycle remain separate acceptance cases. The scoped caller here
has no credential binding and tests document ACL revocation only.
