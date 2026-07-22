# Runs the ProtoMolt ACP agent. The Agent Client Protocol is spoken over stdio,
# so this image has no ports: an ACP-capable IDE (Zed, JetBrains AI chat) launches
# the container and drives it over the container's stdin/stdout.
#
# The distribution is built by Gradle before the image
# (./gradlew :protomolt-acp:installDist), keeping the image a thin JRE layer.
#
#   docker run -i --rm ghcr.io/ai-pipestream/protomolt-acp
FROM eclipse-temurin:21-jre

COPY build/install/protomolt-acp /opt/protomolt-acp

# A compromise of the agent should not own the container: run as a dedicated
# non-root user whose home is the only writable directory (gather caches default
# under it; mount a volume for anything persistent).
RUN useradd --system --uid 10001 --create-home --home-dir /home/protomolt protomolt \
    # Jars inherit the build host's cache permissions (some are 600); the app dir is
    # root-owned read-only for the app user, so normalize read bits explicitly.
    && chmod -R a+rX /opt/protomolt-acp
USER 10001
ENV HOME=/home/protomolt
WORKDIR /home/protomolt

# The protocol is on stdout; the launcher's logs go to stderr, so they never
# corrupt the stream. Nothing here writes to stdout but the agent.
ENTRYPOINT ["/opt/protomolt-acp/bin/protomolt-acp"]
