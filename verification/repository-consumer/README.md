# Published repository library consumer

Run from the repository root. These commands publish only into a filesystem
repository; they do not release artifacts remotely.

```sh
./gradlew -I gradle/repository-consumer.init.gradle \
  -PrepositoryConsumerRepository=/tmp/protomolt-repository-maven \
  -PpublishVersion=0.1.0-repository-boundary-proof stageRepositoryConsumer
./gradlew -p verification/repository-consumer \
  -PrepositoryConsumerRepository=/tmp/protomolt-repository-maven \
  -PcandidateVersion=0.1.0-repository-boundary-proof check
./gradlew -p verification/repository-consumer \
  -PrepositoryConsumerRepository=/tmp/protomolt-repository-maven \
  -PcandidateVersion=0.1.0-repository-boundary-proof -PpomOnly check --rerun-tasks
```

The independent build compiles migrated Java imports, checks typed assembly and
conditional-write alternatives, and rejects storage/database implementations in
the resolved production graph. The second resolution uses Maven POM metadata
without Gradle metadata redirection. Neither run substitutes project dependencies.
Use a unique candidate version and destination when comparing different revisions.

The separate `publicationClient` source set depends only on the published
`protomolt-repo-publication-grpc` artifact. It compiles client construction and the
shared publication method without dependencies from the other consumer source
sets. Its resolved runtime rejects repository server/engine code, SQL, Kafka and
AWS, Azure or Redis storage SDKs. Both metadata modes apply that check. This is a
packaging check; real transport behavior is tested in the packaged storage suite.
