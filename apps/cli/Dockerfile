# Runs the ProtoMolt CLI as a GraalVM native binary — no JRE in the image.
# The binary is built by Gradle before the image
# (JAVA_HOME=<graalvm-25> ./gradlew :protomolt-cli:nativeCompile), keeping the
# image a thin glibc layer over the executable.
#
#   docker run --rm ghcr.io/ai-pipestream/protomolt-cli list
#
# The binary links only glibc + libz, so any base whose glibc is at least as
# new as the build host's works (ubuntu-latest CI runners build against 2.39).
FROM debian:trixie-slim

COPY build/native/nativeCompile/protomolt-cli /usr/local/bin/protomolt

# A compromise of the process should not own the container: run as a dedicated
# non-root user. The CLI itself writes nothing, but gather-git's clone cache
# defaults under HOME (mount a volume to persist it).
RUN useradd --system --uid 10001 --create-home --home-dir /home/protomolt protomolt \
    && chmod a+rx /usr/local/bin/protomolt
USER 10001
ENV HOME=/home/protomolt
WORKDIR /home/protomolt

ENTRYPOINT ["/usr/local/bin/protomolt"]
CMD ["--help"]
