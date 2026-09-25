# Build first with ./gradlew :protomolt-correction:installDist.
FROM eclipse-temurin:25-jre

COPY build/install/protomolt-correction /opt/protomolt-correction
RUN useradd --system --uid 10001 --create-home --home-dir /home/protomolt protomolt \
    && chmod -R a+rX /opt/protomolt-correction \
    && mkdir -p /data/correction \
    && chown -R 10001:10001 /data/correction
USER 10001
ENV HOME=/home/protomolt
WORKDIR /home/protomolt
VOLUME ["/data/correction"]
EXPOSE 9090

# The gRPC health method is authenticated; this checks local listener liveness.
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9090"]

ENTRYPOINT ["/opt/protomolt-correction/bin/protomolt-correction"]
