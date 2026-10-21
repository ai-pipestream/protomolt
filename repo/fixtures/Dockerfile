# The dev corpus seeder (CorpusSeederMain): a one-shot process the dev compose
# profile runs against the repository host's PostgreSQL and object store. The
# distribution is built by Gradle before the image
# (./gradlew :protomolt-repo-fixtures:installDist), keeping the image a thin
# JRE layer over the launcher. Configuration is environment driven: the same
# DOCUMENT_PLATFORM_* family as the repository host, plus PROTOMOLT_CORPUS_SEED=dev,
# without which the seeder exits before touching the store.
#
#   docker compose -f deploy/document-platform/compose.yml --profile dev up
FROM eclipse-temurin:25-jre

COPY build/install/protomolt-repo-fixtures /opt/protomolt-repo-fixtures

RUN useradd --system --uid 10001 --create-home --home-dir /home/protomolt protomolt \
    && chmod -R a+rX /opt/protomolt-repo-fixtures
USER 10001
ENV HOME=/home/protomolt
WORKDIR /home/protomolt

ENTRYPOINT ["/opt/protomolt-repo-fixtures/bin/protomolt-repo-fixtures"]
