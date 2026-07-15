# Runs the one-process ProtoMolt server. The distribution is built by Gradle
# before the image (./gradlew :protomolt-serve:installDist), keeping the image
# a thin JRE layer over the launcher.
#
#   docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
FROM eclipse-temurin:21-jre

COPY build/install/protomolt-serve /opt/protomolt-serve

# A compromise of the process should not own the container: run as a dedicated
# non-root user whose home is the only writable directory (demo registries and
# gather caches default under it; mount volumes for anything persistent).
RUN useradd --system --uid 10001 --create-home --home-dir /home/protomolt protomolt \
    # Jars inherit the build host's cache permissions (some are 600); the app dir is
    # root-owned read-only for the app user, so normalize read bits explicitly.
    && chmod -R a+rX /opt/protomolt-serve
USER 10001
ENV HOME=/home/protomolt
WORKDIR /home/protomolt

# REST/OpenAPI/Swagger/MCP; gRPC; optional Confluent-protocol registry.
EXPOSE 8080 9090 8081

# The REST host's /health endpoint stays open in every auth mode. The probe uses
# bash's /dev/tcp so the image needs no curl/wget layer.
HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
  CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8080 && printf 'GET /health HTTP/1.0\\r\\n\\r\\n' >&3 && head -1 <&3 | grep -q 200"]

ENTRYPOINT ["/opt/protomolt-serve/bin/protomolt-serve"]
