# One Aeron Cluster member per container, so a failover test can kill, pause and partition a node
# with docker's own primitives rather than signalling JVMs that share a host. A node is the unit:
# the tap is aeron:ipc and co-located apps attach to this member's embedded media driver, so
# anything co-located belongs in this container beside it.
#
# The uber jar is built on the host (./gradlew uberJar) and copied in, not built in a stage here —
# CI already caches Gradle on the runner, and a Gradle stage would miss that cache every run.
FROM eclipse-temurin:21-jre

# Globbed so the version is not written here; more than one uber jar in build/libs fails the COPY.
COPY build/libs/seqeron-*-uber.jar       /opt/seqeron/seqeron.jar
COPY src/main/scripts/ports.sh           /opt/seqeron/ports.sh
COPY src/main/scripts/seqeron-home.sh    /opt/seqeron/seqeron-home.sh
COPY docker/entrypoint.sh                /opt/seqeron/entrypoint.sh

# SEQERON_HOME set, so seqeron-home.sh does not guess a layout this image does not have.
ENV SEQERON_HOME=/opt/seqeron
ENV SEQERON_JAR=/opt/seqeron/seqeron.jar

ENTRYPOINT ["/opt/seqeron/entrypoint.sh"]
