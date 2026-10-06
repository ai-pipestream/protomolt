# Retained expired bound-owner reconciliation

Implementation passed the focused and packaged regression checks. The focused PostgreSQL cases pass
for an activation whose acknowledgment was lost and whose claim and owner later
expire naturally. They reject changed modes before reservation and proceed through
one new V97 reservation, V93 installation and V94 activation. A second case loses
the new reservation's reply: retirement cannot use fencing of the old claim alone,
ordinary advancement and V98 cannot bypass the pending V97, and the exact retained
proposal is confirmed before continuing. The broader run includes an explicit check
that confirmation preserves the pending claim and owner identity.

`red.log` records the missing-behavior test against an inert new helper before its
implementation. `targeted-green.log` records the two passing cases. The aggregate run passed 41 recovery-attempt tests, 22 mode-journal tests and
one packaged provider regression, with no failures, errors or skips.
`final-green.log`, `focused-green.tar.gz` and `packaged-green.tar.gz` retain
the output and JUnit results. These fixtures use
real PostgreSQL with synthetic payloads for metadata protocol coverage; they do
not establish provider publication or worker lifetime for this branch.

Sol reviewed identity matching, activation proof, current caller authorization,
fixed modes, pending reservation retention and fencing retirement. Follow-up
qualification remains for foreign winners, authorization revocation before pending
confirmation, terminal and shutdown disposal while pending, and provider/worker
ownership throughout the composed transition. No public protobuf contract changed.
