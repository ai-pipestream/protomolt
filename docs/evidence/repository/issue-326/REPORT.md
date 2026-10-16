# Structural refusal messages (#326)

The structural guard now distinguishes depth from node count and includes the path for depth and unsupported Any locations. DocumentSaves preserves the unknown-field wording only for that specific cause. Status and recording behavior are unchanged. errors.proto documents RESOURCE_EXHAUSTED for the existing host budget failure.

The new gRPC test failed against unchanged production at 40e48a627: a deep document returned the unknown-schema prefix and generic structural-limits message. Red log: /tmp/pr326-red.log.

The public-path test sends nested protobuf Values and 1,000,001 list elements in the existing custom_fields Struct field. This is intentional protobuf content, without JSON conversion. Both requests must return INVALID_ARGUMENT / MALFORMED_REQUEST, no outcome, no revision and no upload. Unknown-field wording is separately pinned exactly.

Acceptance limitation: the compiled Document has Any only at structured_data and parser_results.value.document.shape. An unsupported Any path cannot be sent as a known field through this compiled gRPC request. The guard is tested directly with an unsupported path, not presented as transport coverage. No new schema or endpoint was added to manufacture that case. The node/depth constants are unchanged; the node test exercises the real threshold.

Final command: ./gradlew :protomolt-repo-accession:test --tests '*AccessionGrpcIT' --tests '*DocumentSavesIT.unknownFieldsAreRefusedBeforeAuthorizationAndNotRecorded' :protomolt-repo-admission:test --tests '*DocumentStructuralRefusalTest' --max-workers=2
Log: /tmp/pr326-final.log. No full-container or RustFS qualification claimed.
