# Selected historical wire decoding

`DocumentHistoricalResponseMaterialization` verifies request/response identity,
retained artifacts and the final Any boundary, then reuses that checked descriptor
for bounded dynamic decoding. It never consults a registry. The result owns
serialized response and selected-value reservations until close; parsed heap and
transport allocation remain separately bounded host obligations.

The actual-descriptor fixture checks the decoded field and value, result lifetime,
every cancellation and reservation-refusal point, invalid requests, nested unknown
selection fields, byte limits, malformed payloads, wire-value limits and corrupt
descriptors. Malformed and resource-limited decoding remain distinct failures.

`:protomolt-repo-admission:test --tests '*DocumentRetainedPathMaterializationTest'`
passes 28 cases. Existing identity, artifact, occurrence-path and cancellation
cases are included in that count.

This supplies verified decode material for a future remote adapter. It is not
evidence of a deployed remote client, transport authentication, current ACL checks
or a new validation verdict. An already returned Java view cannot be revoked.
