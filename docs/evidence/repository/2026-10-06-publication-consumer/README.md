# Published publication-client consumer

The source at `5aa8a41bc` was staged into the unique filesystem Maven repository
`/tmp/protomolt-publication-maven-5aa8a41bc` with candidate version
`0.1.0-publication-5aa8a41bc`. The extended independent consumer then compiled
publication client construction and SPI invocation in two metadata modes.
All commands passed. No remote artifact release occurred.

```
./gradlew -I gradle/repository-consumer.init.gradle -PrepositoryConsumerRepository=/tmp/protomolt-publication-maven-5aa8a41bc -PpublishVersion=0.1.0-publication-5aa8a41bc stageRepositoryConsumer
./gradlew -p verification/repository-consumer -PrepositoryConsumerRepository=/tmp/protomolt-publication-maven-5aa8a41bc -PcandidateVersion=0.1.0-publication-5aa8a41bc check
./gradlew -p verification/repository-consumer -PrepositoryConsumerRepository=/tmp/protomolt-publication-maven-5aa8a41bc -PcandidateVersion=0.1.0-publication-5aa8a41bc -PpomOnly check --rerun-tasks
```

Staging took ten seconds. The Gradle-metadata consumer took five seconds; the
POM-only consumer took 879 ms. POM-only resolution disables Gradle metadata
redirection. Neither build substitutes projects for the published dependencies.

The publicationClient source set declares only the publication client artifact;
other consumer source sets do not supply its dependencies. Its resolved runtime
contains no checked repository server/container/engine, SQL or Kafka modules,
and no AWS, Azure or Redis provider groups. Full resolved component identities
are in both consumer logs. The predicate checks the named artifacts and groups;
it is not an exhaustive classifier for every possible database library.

Sol reviewed source-set isolation, staging and metadata-mode selection and found
no blocker. This closes the publication client's isolated packaging check. It
does not change the separate real-transport, provider or performance evidence.
