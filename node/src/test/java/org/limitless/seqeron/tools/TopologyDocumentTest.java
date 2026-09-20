package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.xml.sax.SAXParseException;

/**
 * What {@code clusterctl load-topology} accepts and what it refuses. Every case here is one an operator only
 * ever meets as a refusal, which is exactly why they are worth pinning: a check that silently stopped
 * applying would look, from the outside, like a document that was fine — and the deployment would come up
 * with no primary gateway, or with an application whose traffic is dropped frame by frame at run time.
 *
 * <p>The two layers are asserted separately because they fail differently and an operator reads them
 * differently: {@link SAXParseException} carries the line and column of the offending row, while the
 * relational checks {@link TopologyDocument} makes itself name the rows in conflict.
 */
class TopologyDocumentTest {
    @TempDir
    private Path dir;

    /** A well-formed pair — two instances of one logical gateway, sharing a sourceId, one of them rank 0. */
    private static final String GATEWAY_PAIR = """
            <gateway name="GW-A" id="1" sourceId="9" rank="0"/>
            <gateway name="GW-B" id="2" sourceId="9" rank="1"/>""";

    // ── accepted ──────────────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("all three sections are read in document order")
    void readsAllThreeSections() throws Exception {
        final TopologyDocument topology = read(document(GATEWAY_PAIR, """
                <application name="probe" sourceId="8"/>
                <application name="basicdata" sourceId="5"/>""", """
                <protocol payloadId="2" version="1" name="order"/>
                <protocol payloadId="3" version="1" name="session"/>"""));

        assertEquals(2, topology.gateways().size());
        assertEquals("GW-A", topology.gateways().get(0).gatewayName(), "document order is publish order");
        assertEquals(9, topology.gateways().get(1).gatewaySourceId(), "a pair shares one sourceId");
        assertEquals(1, topology.gateways().get(1).preferenceRank());
        assertEquals("basicdata", topology.applications().get(1).applicationName());
        assertEquals(3, topology.protocols().get(1).payloadId());
    }

    @Test
    @DisplayName("a deployment with no applications and no protocols declares neither section")
    void applicationsAndProtocolsMayBeAbsent() throws Exception {
        final TopologyDocument topology = read(document(GATEWAY_PAIR, null, null));

        assertEquals(2, topology.gateways().size());
        assertTrue(topology.applications().isEmpty());
        assertTrue(topology.protocols().isEmpty());
    }

    @Test
    @DisplayName("the two optional sections may be present and empty")
    void applicationsAndProtocolsMayBeEmpty() throws Exception {
        final TopologyDocument topology = read(document(GATEWAY_PAIR, "", ""));

        assertTrue(topology.applications().isEmpty());
        assertTrue(topology.protocols().isEmpty());
    }

    // ── refused by topology.xsd ───────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("payloadId 1 is never registrable — the schema is C-1's only enforcement")
    void retiredPayloadIdIsRefused() {
        refusedBySchema(document(GATEWAY_PAIR, null, """
                <protocol payloadId="1" version="1" name="core"/>"""));
    }

    @Test
    @DisplayName("payloadId 0 is invalid on the wire and refused with it")
    void payloadIdZeroIsRefused() {
        refusedBySchema(document(GATEWAY_PAIR, null, """
                <protocol payloadId="0" version="1" name="nothing"/>"""));
    }

    @Test
    @DisplayName("an empty gateway list is refused — it elects nobody")
    void emptyGatewayListIsRefused() {
        refusedBySchema(document("", null, null));
    }

    @Test
    @DisplayName("a duplicate gatewayId is refused — it would silently drop an instance")
    void duplicateGatewayIdIsRefused() {
        refusedBySchema(document("""
                <gateway name="GW-A" id="1" sourceId="9" rank="0"/>
                <gateway name="GW-B" id="1" sourceId="9" rank="1"/>""", null, null));
    }

    @Test
    @DisplayName("a duplicate gateway name is refused — the name is the launch-time join key")
    void duplicateGatewayNameIsRefused() {
        refusedBySchema(document("""
                <gateway name="GW-A" id="1" sourceId="9" rank="0"/>
                <gateway name="GW-A" id="2" sourceId="9" rank="1"/>""", null, null));
    }

    @Test
    @DisplayName("a duplicate payloadId is an operator slip, not a supersede")
    void duplicatePayloadIdIsRefused() {
        refusedBySchema(document(GATEWAY_PAIR, null, """
                <protocol payloadId="2" version="1" name="order"/>
                <protocol payloadId="2" version="2" name="order-v2"/>"""));
    }

    @Test
    @DisplayName("two applications may not share a sourceId")
    void duplicateApplicationSourceIdIsRefused() {
        refusedBySchema(document(GATEWAY_PAIR, """
                <application name="probe" sourceId="8"/>
                <application name="other" sourceId="8"/>""", null));
    }

    @Test
    @DisplayName("a name longer than the char[32] it encodes into is refused")
    void oversizedNameIsRefused() {
        refusedBySchema(document("""
                <gateway name="%s" id="1" sourceId="9" rank="0"/>""".formatted("G".repeat(33)), null, null));
    }

    @Test
    @DisplayName("a name outside printable US-ASCII is refused")
    void nonAsciiNameIsRefused() {
        refusedBySchema(document("""
                <gateway name="GW-é" id="1" sourceId="9" rank="0"/>""", null, null));
    }

    @Test
    @DisplayName("a negative sourceId is refused — -1 is the cluster's own")
    void negativeSourceIdIsRefused() {
        refusedBySchema(document("""
                <gateway name="GW-A" id="1" sourceId="-1" rank="0"/>""", null, null));
    }

    @Test
    @DisplayName("the sections must appear in publish order")
    void sectionsOutOfOrderAreRefused() {
        refusedBySchema("""
                <?xml version="1.0" encoding="UTF-8"?>
                <topology xmlns="http://limitless.org/seqeron/topology/1">
                  <protocols><protocol payloadId="2" version="1" name="order"/></protocols>
                  <gateways>%s</gateways>
                </topology>
                """.formatted(GATEWAY_PAIR));
    }

    @Test
    @DisplayName("a document in another namespace is not a topology document")
    void wrongNamespaceIsRefused() {
        refusedBySchema("""
                <?xml version="1.0" encoding="UTF-8"?>
                <topology xmlns="http://example.org/topology">
                  <gateways>%s</gateways>
                </topology>
                """.formatted(GATEWAY_PAIR));
    }

    @Test
    @DisplayName("a DOCTYPE is refused outright rather than resolved")
    void doctypeIsRefused() {
        refusedBySchema("""
                <?xml version="1.0" encoding="UTF-8"?>
                <!DOCTYPE topology [<!ENTITY x "y">]>
                <topology xmlns="http://limitless.org/seqeron/topology/1">
                  <gateways>%s</gateways>
                </topology>
                """.formatted(GATEWAY_PAIR));
    }

    @Test
    @DisplayName("a schemaLocation the document names is ignored — the packaged schema is the one that binds")
    void documentSuppliedSchemaLocationIsIgnored() {
        // A file that may name its own schema may name a lax one, which would make C-1 advisory.
        refusedBySchema("""
                <?xml version="1.0" encoding="UTF-8"?>
                <topology xmlns="http://limitless.org/seqeron/topology/1"
                          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                          xsi:schemaLocation="http://limitless.org/seqeron/topology/1 lax.xsd">
                  <gateways>%s</gateways>
                  <protocols><protocol payloadId="1" version="1" name="core"/></protocols>
                </topology>
                """.formatted(GATEWAY_PAIR));
    }

    // ── refused by the loader: the checks a row cannot make about itself ──────────────────────────────

    @Test
    @DisplayName("a logical gateway with no rank-0 row has no primary")
    void noPrimaryIsRefused() {
        assertRefused(document("""
                <gateway name="GW-A" id="1" sourceId="9" rank="1"/>
                <gateway name="GW-B" id="2" sourceId="9" rank="2"/>""", null, null),
                      "has 0 rank-0 row(s)");
    }

    @Test
    @DisplayName("a logical gateway with two rank-0 rows has an arbitrary primary")
    void twoPrimariesAreRefused() {
        assertRefused(document("""
                <gateway name="GW-A" id="1" sourceId="9" rank="0"/>
                <gateway name="GW-B" id="2" sourceId="9" rank="0"/>""", null, null),
                      "has 2 rank-0 row(s)");
    }

    @Test
    @DisplayName("a gateway may not take clusterctl's own sourceId")
    void reservedSourceIdIsRefusedForAGateway() {
        assertRefused(document("""
                <gateway name="GW-A" id="1" sourceId="2" rank="0"/>""", null, null),
                      "is reserved for clusterctl's own markers");
    }

    @Test
    @DisplayName("an application may not take clusterctl's own sourceId either")
    void reservedSourceIdIsRefusedForAnApplication() {
        assertRefused(document(GATEWAY_PAIR, """
                <application name="probe" sourceId="2"/>""", null),
                      "is reserved for clusterctl's own markers");
    }

    @Test
    @DisplayName("an application sharing a listed gateway's sourceId would be dropped frame by frame")
    void applicationSourceIdCollidingWithAGatewayIsRefused() {
        assertRefused(document(GATEWAY_PAIR, """
                <application name="probe" sourceId="9"/>""", null),
                      "is gateway GW-A's");
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────────────

    /** A document with the given section bodies; a null section is omitted entirely, as the schema allows. */
    private static String document(final String gateways, final String applications, final String protocols) {
        final StringBuilder xml = new StringBuilder("""
                <?xml version="1.0" encoding="UTF-8"?>
                <topology xmlns="http://limitless.org/seqeron/topology/1">
                """);
        xml.append("  <gateways>").append(gateways).append("</gateways>\n");
        if (applications != null) {
            xml.append("  <applications>").append(applications).append("</applications>\n");
        }
        if (protocols != null) {
            xml.append("  <protocols>").append(protocols).append("</protocols>\n");
        }
        return xml.append("</topology>\n").toString();
    }

    private TopologyDocument read(final String xml) throws Exception {
        final Path file = Files.createTempFile(dir, "topology", ".xml");
        Files.writeString(file, xml);
        return TopologyDocument.read(file.toFile());
    }

    /** The schema layer refuses, with the line and column an operator needs to find the row. */
    private void refusedBySchema(final String xml) {
        final SAXParseException ex = assertThrows(SAXParseException.class, () -> read(xml));
        assertTrue(ex.getLineNumber() > 0, "an operator has to be able to find the offending row");
    }

    /** The loader's own relational checks refuse, naming the rows in conflict. */
    private void assertRefused(final String xml, final String expected) {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> read(xml));
        assertTrue(ex.getMessage().contains(expected),
                   "expected a message naming " + expected + ", got: " + ex.getMessage());
    }
}
