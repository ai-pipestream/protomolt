# Runs the one-container document platform (DocumentPlatformMain). The
# distribution is built by Gradle before the image
# (./gradlew :protomolt-document-platform:installDist), keeping the image a
# thin JRE layer over the launcher. All configuration is environment driven
# (DOCUMENT_PLATFORM_*); the platform needs a PostgreSQL ledger, a second
# PostgreSQL database for the jobs store, and an S3-compatible object store.
# The registry's git repository lives under /data (mount a volume).
#
#   docker compose -f deploy/document-platform/compose.yml up
#
# JRE 25: the search door links the Lucene 11 line, which ships Java 25
# bytecode; on an older JRE the first search would fail to load classes.
FROM eclipse-temurin:25-jre

COPY build/install/protomolt-document-platform /opt/protomolt-document-platform

# A compromise of the process should not own the container: run as a dedicated
# non-root user; /data holds the registry git repository and is the only
# writable mount.
RUN useradd --system --uid 10001 --create-home --home-dir /home/protomolt protomolt \
    # Jars inherit the build host's cache permissions (some are 600); the app dir is
    # root-owned read-only for the app user, so normalize read bits explicitly.
    && chmod -R a+rX /opt/protomolt-document-platform \
    && mkdir -p /data && chown 10001 /data
USER 10001
ENV HOME=/home/protomolt
WORKDIR /home/protomolt

# repo gRPC; intake gRPC; parse gRPC; registry HTTP; playground HTTP.
EXPOSE 9090 9092 9093 8081 8095 8096 9094 9095

# The registry HTTP port answers /health once the whole platform is up; probe
# it with bash's /dev/tcp so the image needs no curl/wget layer.
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 \
  CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8081"]

ENTRYPOINT ["/opt/protomolt-document-platform/bin/protomolt-document-platform"]
