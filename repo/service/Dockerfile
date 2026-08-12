# Runs the standalone repo-service (RepoServiceMain). The distribution is built
# by Gradle before the image (./gradlew :protomolt-repo-service:installDist),
# keeping the image a thin JRE layer over the launcher. All configuration is
# environment driven (DOCUMENT_PLATFORM_*); the service needs a PostgreSQL
# ledger and an S3-compatible object store.
#
#   docker run -p 9090:9090 ghcr.io/ai-pipestream/protomolt-repo-service
FROM eclipse-temurin:21-jre

COPY build/install/protomolt-repo-service /opt/protomolt-repo-service

# A compromise of the process should not own the container: run as a dedicated
# non-root user whose home is the only writable directory (mount volumes for
# anything persistent).
RUN useradd --system --uid 10001 --create-home --home-dir /home/protomolt protomolt \
    # Jars inherit the build host's cache permissions (some are 600); the app dir is
    # root-owned read-only for the app user, so normalize read bits explicitly.
    && chmod -R a+rX /opt/protomolt-repo-service
USER 10001
ENV HOME=/home/protomolt
WORKDIR /home/protomolt

# gRPC (DocumentService, DriveService, health, reflection); HTTP upload route.
EXPOSE 9090 8080

# There is no HTTP health route when the upload server is disabled, so probe
# the gRPC port with bash's /dev/tcp; the image needs no curl/wget layer.
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9090"]

ENTRYPOINT ["/opt/protomolt-repo-service/bin/protomolt-repo-service"]
