# Runs one persistent Codex or Kimi delegation participant (protomolt-agent-host).
# The distribution is built by Gradle before the image
# (./gradlew :protomolt-agent-host:installDist), keeping the image a thin JRE
# layer over the launcher.
#
# The host shells out to the provider CLIs. The Codex CLI is installed from
# npm here; the Kimi CLI is a standalone binary that ships inside the Kimi
# configuration directory, so it enters the container through the mounted
# provider config volume (see deploy/krick/compose.yml). Provider
# authentication is never baked into the image: mount ~/.codex and/or
# ~/.kimi-code from the host.
#
#   docker run --rm protomolt-agent-host:local --help
FROM node:22.20.0-bookworm-slim AS node

FROM eclipse-temurin:21-jre

# Node runtime for the Codex CLI (node, npm, corepack live in /usr/local).
COPY --from=node /usr/local /usr/local

# git: the agents inspect and commit inside mounted worktrees.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Pin the Codex CLI to the version the deployment docs name.
RUN npm install -g @openai/codex@0.147.0 \
    && npm cache clean --force

COPY build/install/protomolt-agent-host /opt/protomolt-agent-host

# A compromise of the process should not own the container: run as a dedicated
# non-root user whose home carries the mounted provider config directories.
RUN useradd --system --uid 10001 --create-home --home-dir /home/protomolt protomolt \
    # Jars inherit the build host's cache permissions (some are 600); the app dir is
    # root-owned read-only for the app user, so normalize read bits explicitly.
    && chmod -R a+rX /opt/protomolt-agent-host
USER 10001
ENV HOME=/home/protomolt
# The Kimi CLI binary ships in its config directory (mounted at runtime).
ENV PATH="/home/protomolt/.kimi-code/bin:/usr/local/bin:/opt/protomolt-agent-host/bin:${PATH}"
WORKDIR /home/protomolt

ENTRYPOINT ["/opt/protomolt-agent-host/bin/protomolt-agent-host"]
