# Host reader composition qualification

Code checkpoint: `a588919d7` on `agent/host-reader-composition`.

Command:

```sh
./gradlew :protomolt-repo-service:test --tests '*ReaderHostCompositionIT' --tests '*BoundedArchiveOptionsTest' --tests '*BoundedDocumentOptionsTest' --max-workers=2 --console=plain
```

Result: 12 tests, zero failures, errors or skips. Five composition cases use real
PostgreSQL, LocalStack and Redis containers (the identity validation case does not
perform provider I/O). Seven existing option tests cover profile validation.

The new cases check exact host/boot identity and shared reader binding, duplicate
startup leaving the live execution ACTIVE, fresh replacement startup, clean local
drain without a termination receipt, partial construction failure, bounded Redis
archive composition, and a held host-row lock causing bounded close failure with
successful retry after release. SQL state is read over independent JDBC connections.

Sol reviewed the API and implementation. Its unbounded shutdown-fence finding was
fixed and covered by the held-lock regression. Final review reported no further
blocker in this scope.

Hosted bounded-document publication is not qualified by this run. No remote
termination verifier, host discovery loop, deployment, performance or main-branch
merge is claimed. The full storage run in the separate reader-host-lifecycle
checkout tests its earlier `731af65ef` checkpoint, not these service changes.
