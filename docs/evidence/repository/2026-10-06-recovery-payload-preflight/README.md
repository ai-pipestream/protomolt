# Recovery payload preflight

Recovery now reserves memory, copies complete resubmitted uploads and verifies
checksums before any ownership succession. The snapshot remains owned through
synchronous publication; the existing upload coordinator still owns separate
worker copies. Normal fresh/live/terminal routing does not allocate this snapshot.

`corruption-green.log` records a successful focused and packaged run before the
additional capacity/mutation assertions. A corrupt payload with unchanged declared
checksum is rejected before reservation, installation or activation, followed by
successful resubmission of the correct bytes.

`focused-green.tar.gz` contains eight payload tests and the PostgreSQL capacity
regression, all passing without skips. The capacity fixture uses synthetic payload
declarations solely to prove capacity refusal precedes copying/checksum work and
durable succession. It is not a provider or semantic-validity claim.

The final packaged mutation run passed (`final-green.log` and
`packaged-green.tar.gz`). It changes a caller-owned byte after
preflight but before reservation, then requires successful real versioned provider
publication through the managed service. Peak recovery memory includes the new
snapshot and the existing worker-owned upload reservation; no fallback bypasses
those bounds. These correctness tests are not performance measurements.
