# One Aeron Cluster member per container, so a failover test can kill, pause and partition a node
# with docker's own primitives rather than signalling JVMs that share a host. A node is the unit:
# the tap is aeron:ipc and co-located apps attach to this member's embedded media driver, so
# anything co-located belongs in this container beside it.
#
# The operator distribution is built on the host (./gradlew operatorDist) and copied in, not built in a
# stage here — CI already caches Gradle on the runner, and a Gradle stage would miss that cache every run.
# Tagged releases push this image to GHCR (.github/workflows/release.yml).
FROM eclipse-temurin:21-jre

# bin/ beside lib/ is the layout seqeron-home.sh recognises, so no SEQERON_HOME or SEQERON_JAR is set.
COPY build/install/seqeron     /opt/seqeron
COPY docker/node-env.sh        /opt/seqeron/docker/node-env.sh
COPY docker/entrypoint.sh      /opt/seqeron/docker/entrypoint.sh
COPY docker/clusterctl         /usr/local/bin/clusterctl

ENTRYPOINT ["/opt/seqeron/docker/entrypoint.sh"]
