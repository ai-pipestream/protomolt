# Registry corruption and repair

The real Git registry adapter test now corrupts a stored descriptor at its selected
SHA-256 path before admission. Fingerprint validation refuses the bytes, no artifact
is cached, no load remains retained and admission reservations return to zero.
Restoring the exact bytes permits real admission through the same resolver, with a
second registry read and no cache hit. No production behavior changed.

The first test run expected a store exception; the existing fingerprint validator
reports IllegalArgumentException with an explicit mismatch message. The assertion
now checks that exact refusal. Filesystem outages retain their separate tests.

`./gradlew :protomolt-repo-schema-registry:test --tests '*RegistrySchemaResolverTest' --console=plain`
passed all six cases in a four-second build. The JUnit result is attached.
