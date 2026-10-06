# Unactivated managed recovery reconciliation

The new-host dispatcher cases cover reserved and installed unactivated successors:
a live claim is not replaced, while natural expiry permits exact supersession and
activation. Local cases cover retained PROPOSED after committed V97 with a lost
acknowledgment, RESERVED, INSTALLED, and retained V98 after a lost reply.

Fixed-mode comparison precedes V98 for both local reconciliation and fresh-host
advancement. A mismatch leaves the claim unchanged; a corrected retry uses the
retained proposal. `proposed-retry-red.log` records the regression where a rejected
uncommitted proposal was mistaken for a foreign successor. `targeted-green.log`
records all eight targeted cases passing after exact predecessor discrimination.

These use PostgreSQL and synthetic payloads for metadata protocol coverage. They
are not real-provider publication or performance qualification. The final run passed: 38 recovery-attempt tests, 22 publication-mode tests, and
one packaged provider integration test, with no failures, errors or skips.
`final-green.log`, `focused-green.tar.gz` and `packaged-green.tar.gz` retain the
results. The revised checks exercise foreign-winner rejection and exclude an
already activated successor from this unactivated path. The packaged test is a
regression of existing provider scenarios, not provider coverage of every newly
added reconciliation branch. Retained activated-owner recovery remains a
separate open task, as do the other repository-composition requirements.
