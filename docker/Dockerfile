# One Aeron Cluster member per container, so a failover test can kill, pause and partition a node
# with docker's own primitives rather than signalling JVMs that share a host. A node is the unit:
# the tap is aeron:ipc and co-located apps attach to this member's embedded media driver, so
# anything co-located belongs in this container beside it.
#
# The uber jar is built on the host (./gradlew uberJar) and copied in, not built in a stage here —
# CI already caches Gradle on the runner, and a Gradle stage would miss that cache every run.
FROM eclipse-temurin:21-jre

COPY build/libs/phixeron-0.1.0-uber.jar /opt/phixeron/phixeron.jar
COPY src/main/scripts/ports.sh          /opt/phixeron/ports.sh
COPY docker/entrypoint.sh               /opt/phixeron/entrypoint.sh

ENV PHIXERON_JAR=/opt/phixeron/phixeron.jar

ENTRYPOINT ["/opt/phixeron/entrypoint.sh"]
