# Runs the one-process ProtoMolt server. The distribution is built by Gradle
# before the image (./gradlew :protomolt-serve:installDist), keeping the image
# a thin JRE layer over the launcher.
#
#   docker run -p 8080:8080 -p 9090:9090 ghcr.io/ai-pipestream/protomolt-serve --demo
FROM eclipse-temurin:21-jre

COPY build/install/protomolt-serve /opt/protomolt-serve

# REST/OpenAPI/Swagger/MCP; gRPC; optional Confluent-protocol registry.
EXPOSE 8080 9090 8081

ENTRYPOINT ["/opt/protomolt-serve/bin/protomolt-serve"]
